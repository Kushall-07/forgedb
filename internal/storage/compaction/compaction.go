// Package compaction merges a set of a database's existing, immutable
// SSTables into a single new SSTable, eliminating the obsolete duplicate
// versions and safely-removable tombstones that accumulate once the same
// key has been written to more than one SSTable over time (see
// docs/storage/phase4-compaction.md for the full design and worked
// examples).
//
// This package knows how to read sstable.Readers via the minimal
// iterator capability added alongside it, and how to publish its result
// through manifest.VersionSet's existing atomic multi-table update
// methods; it does not implement its own SSTable serialization, and it
// does not know about MemTable, the WAL, Raft, or networking. As in
// Phase 3, compaction is invoked explicitly by a caller (demonstrated in
// compaction_test.go) -- there is no background trigger, size policy, or
// automatic scheduling.
package compaction

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/Kushall-07/forgedb/internal/storage/manifest"
	"github.com/Kushall-07/forgedb/internal/storage/sstable"
)

// ErrNoInputTables is returned by Compact when called with no input table
// IDs: there is nothing to merge, and running it would be a no-op at
// best or, if allowed to proceed, a compaction that spuriously claims to
// have "removed" zero tables.
var ErrNoInputTables = errors.New("compaction: no input tables specified")

// Result describes the outcome of a successful Compact call.
type Result struct {
	// OutputID is the Manifest ID assigned to the new SSTable. It is zero
	// if the compaction produced no output table -- every input entry
	// was a tombstone that could be safely dropped, so there was no
	// remaining state to write.
	OutputID uint64
	// OutputFileName is the new SSTable's file name, relative to the
	// directory Compact was given. It is empty exactly when OutputID is
	// zero.
	OutputFileName string
	// TablesRemoved lists the input table IDs retired by this
	// compaction, sorted ascending.
	TablesRemoved []uint64
}

// Compact merges the SSTables named by inputIDs -- which must all be
// part of vs's current active Version -- into at most one new SSTable,
// and atomically publishes the result: the new table (if any) is added
// and every input table is removed from the active Version in a single
// Manifest update (manifest.VersionSet.ReplaceTables, or RemoveTables if
// the merge produced no surviving entries). dir is the directory
// containing the Manifest and every SSTable file it names; the output
// table, if any, is written there too.
//
// Compact fails without changing vs or writing anything durable if
// inputIDs is empty, names a table that is not part of the current
// active Version, or contains a duplicate ID. It also fails, again
// without having published anything, if an input table cannot be opened
// or read (including because it is corrupt), if the output table cannot
// be built, or if the built output fails Compact's own validation pass
// against the merged data it was built from.
//
// If tracker is non-nil, Compact calls tracker.Retire for each input
// table's file once the Manifest update that retires it has durably
// succeeded, so a caller using Tracker to bracket its reads with
// Acquire/release (see Tracker) never has an input file deleted out from
// under an in-flight read -- see "Reader safety" in
// docs/storage/phase4-compaction.md. A nil tracker leaves every input
// file exactly where it was; Compact never deletes an input file itself
// in that case.
func Compact(vs *manifest.VersionSet, dir string, inputIDs []uint64, tracker *Tracker) (Result, error) {
	if len(inputIDs) == 0 {
		return Result{}, ErrNoInputTables
	}

	sortedInputs := append([]uint64(nil), inputIDs...)
	sort.Slice(sortedInputs, func(i, j int) bool { return sortedInputs[i] < sortedInputs[j] })
	for i := 1; i < len(sortedInputs); i++ {
		if sortedInputs[i] == sortedInputs[i-1] {
			return Result{}, fmt.Errorf("compaction: duplicate input table id %d", sortedInputs[i])
		}
	}

	version := vs.Current()
	active := make(map[uint64]string, len(version.Tables))
	for _, t := range version.Tables {
		active[t.ID] = t.FileName
	}

	inputPaths := make(map[uint64]string, len(sortedInputs))
	for _, id := range sortedInputs {
		fileName, ok := active[id]
		if !ok {
			return Result{}, fmt.Errorf("compaction: table %d is not part of the active version", id)
		}
		inputPaths[id] = filepath.Join(dir, fileName)
	}

	// A winning tombstone can only be dropped from the output if no
	// active table outside this compaction's input set is old enough to
	// still hold a value that tombstone would otherwise be shadowing.
	// "Old enough" is exactly "has a smaller ID than every input table":
	// a table with a larger ID, whether or not it is part of this
	// compaction, is always newer than every input and so can never be
	// the thing a dropped tombstone would resurrect. This is
	// deliberately conservative -- it does not attempt to reason about
	// per-table key ranges -- matching the phase's correctness-first
	// requirement: when in doubt, keep the tombstone.
	minInputID := sortedInputs[0]
	canDropTombstones := true
	for id := range active {
		if _, isInput := inputPaths[id]; isInput {
			continue
		}
		if id < minInputID {
			canDropTombstones = false
			break
		}
	}

	readers := make(map[uint64]*sstable.Reader, len(sortedInputs))
	closeReaders := func() {
		for id, r := range readers {
			r.Close()
			delete(readers, id)
		}
	}
	defer closeReaders()
	for _, id := range sortedInputs {
		r, err := sstable.Open(inputPaths[id])
		if err != nil {
			return Result{}, fmt.Errorf("compaction: open input table %d (%s): %w", id, inputPaths[id], err)
		}
		readers[id] = r
	}

	merged, err := mergeTables(sortedInputs, readers, canDropTombstones)
	// The merge is fully materialized (mergeTables returns a slice, not
	// a live iterator) once it returns, so the input files are no longer
	// needed; close them now rather than leaving them open (and, on
	// platforms where an open handle blocks deletion, undeletable) for
	// the rest of Compact.
	closeReaders()
	if err != nil {
		return Result{}, fmt.Errorf("compaction: merge: %w", err)
	}

	var result Result
	if len(merged) == 0 {
		// Every input entry was a tombstone dropped as safe: there is no
		// surviving state to write, so retire the inputs with no
		// replacement rather than asking sstable.Writer to build a
		// table with zero entries (which it rejects outright).
		if err := vs.RemoveTables(sortedInputs); err != nil {
			return Result{}, fmt.Errorf("compaction: publish removal: %w", err)
		}
		result = Result{TablesRemoved: sortedInputs}
	} else {
		outputName := outputFileName(sortedInputs)
		outputPath := filepath.Join(dir, outputName)

		w := sstable.NewWriter()
		for _, e := range merged {
			if err := w.Add(e.Key, e.Value, e.Tombstone); err != nil {
				return Result{}, fmt.Errorf("compaction: build output entry: %w", err)
			}
		}
		if err := w.Build(outputPath); err != nil {
			return Result{}, fmt.Errorf("compaction: build output table: %w", err)
		}
		if err := validateOutput(outputPath, merged); err != nil {
			os.Remove(outputPath) // best-effort: never publish, so no reader can be affected either way.
			return Result{}, fmt.Errorf("compaction: validate output table: %w", err)
		}

		newID, err := vs.ReplaceTables(sortedInputs, outputName)
		if err != nil {
			os.Remove(outputPath) // best-effort: the output was never published, so it is safe to discard.
			return Result{}, fmt.Errorf("compaction: publish output table: %w", err)
		}
		result = Result{OutputID: newID, OutputFileName: outputName, TablesRemoved: sortedInputs}
	}

	if tracker != nil {
		for _, id := range sortedInputs {
			tracker.Retire(inputPaths[id])
		}
	}
	return result, nil
}

// outputFileName derives a deterministic name for a compaction's output
// table from its sorted input IDs. This keeps naming simple (Phase 3
// left SSTable naming entirely to the caller, with no rotation or
// assignment policy of its own) while guaranteeing that compacting the
// same input set always produces the same output name -- there is
// nothing here for two different runs to disagree on.
func outputFileName(sortedInputIDs []uint64) string {
	parts := make([]string, len(sortedInputIDs))
	for i, id := range sortedInputIDs {
		parts[i] = strconv.FormatUint(id, 10)
	}
	return "compacted-" + strings.Join(parts, "-") + ".sst"
}

// validateOutput re-opens the just-built table at path and walks it end
// to end, checking that it reports exactly the entries it was built
// from, in the same order -- catching a Writer/Reader mismatch or a
// silently truncated write before Compact ever publishes the table into
// the Manifest.
func validateOutput(path string, want []sstable.Entry) error {
	r, err := sstable.Open(path)
	if err != nil {
		return fmt.Errorf("reopen: %w", err)
	}
	defer r.Close()

	if r.Count() != uint64(len(want)) {
		return fmt.Errorf("entry count: got %d, want %d", r.Count(), len(want))
	}

	it := r.NewIterator()
	i := 0
	for it.Next() {
		if i >= len(want) {
			return fmt.Errorf("more entries than expected (Count said %d)", len(want))
		}
		got, w := it.Entry(), want[i]
		if !bytes.Equal(got.Key, w.Key) {
			return fmt.Errorf("entry %d key: got %q, want %q", i, got.Key, w.Key)
		}
		if got.Tombstone != w.Tombstone {
			return fmt.Errorf("entry %d (%q) tombstone: got %v, want %v", i, got.Key, got.Tombstone, w.Tombstone)
		}
		if !got.Tombstone && !bytes.Equal(got.Value, w.Value) {
			return fmt.Errorf("entry %d (%q) value: got %q, want %q", i, got.Key, got.Value, w.Value)
		}
		i++
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("iterate: %w", err)
	}
	if i != len(want) {
		return fmt.Errorf("fewer entries than expected: got %d, want %d", i, len(want))
	}
	return nil
}
