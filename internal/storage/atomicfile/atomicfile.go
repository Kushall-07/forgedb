// Package atomicfile implements the write-temp / fsync / atomic-rename /
// fsync-directory protocol that Phase 3 uses for both SSTable creation and
// Manifest updates: a file at a given path is either fully replaced with
// new content, or left exactly as it was -- there is no state in which a
// reader can observe a partially written file.
package atomicfile

import (
	"fmt"
	"os"
	"path/filepath"
)

// Write atomically replaces the file at path with data.
//
// The steps are:
//
//  1. Write data to a temporary file (path + ".tmp") in the same
//     directory as path, so the eventual rename is same-volume and atomic.
//  2. Fsync the temporary file, so its content is durable before it is
//     ever linked to path.
//  3. Rename the temporary file over path. A rename either fully
//     completes or fully fails; there is no in-between state where path
//     is a truncated or half-written file. If path already exists, the
//     rename replaces it in one step -- Write never truncates an existing
//     file in place.
//  4. Fsync the directory containing path, so the rename itself (the
//     directory-entry update) is durable.
//
// If any step before the rename fails, path is left completely untouched:
// whatever was there before Write was called -- a previous valid file, or
// nothing -- is still there, and the temporary file is removed. This is
// what makes the protocol safe to use for a Manifest: a failed update
// never leaves callers with a corrupt or missing Manifest, only the
// unchanged previous one.
//
// Step 4 is best-effort: on platforms where the standard library cannot
// fsync a directory (notably Windows -- see the package-level docs in
// docs/storage/phase3-sstables-manifest.md), its failure does not fail
// Write, since the rename in step 3 has already durably completed the
// file replacement from path's point of view.
func Write(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("atomicfile: create directory %s: %w", dir, err)
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("atomicfile: create temp file %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("atomicfile: write temp file %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("atomicfile: sync temp file %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("atomicfile: close temp file %s: %w", tmp, err)
	}

	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("atomicfile: rename %s to %s: %w", tmp, path, err)
	}

	// Best-effort: see the Write doc comment and
	// docs/storage/phase3-sstables-manifest.md for why a failure here is
	// not treated as fatal.
	_ = syncDir(dir)

	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
