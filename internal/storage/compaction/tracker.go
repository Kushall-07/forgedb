package compaction

import (
	"os"
	"sync"
)

// Tracker manages the physical-deletion lifecycle of SSTable files that
// compaction has retired from the active Manifest but that a reader may
// still have open. Compact itself never deletes an input SSTable's file
// as part of publishing its result -- removing a table from the active
// Version (a Manifest update) and reclaiming its file on disk are
// deliberately separate steps, because a caller that obtained a Version
// before compaction ran may still be reading one of the input files (see
// docs/storage/phase4-compaction.md, "Reader safety").
//
// A caller that wants retired files actually reclaimed uses a Tracker:
// every read of an SSTable file is bracketed by Acquire and the release
// function it returns, and Retire (called once a table is no longer in
// the active Version, as Compact does for each of its inputs when given
// a non-nil Tracker) deletes the file immediately if nothing currently
// holds it, or defers deletion until the last outstanding Acquire on it
// is released.
//
// A Tracker is safe for concurrent use. The zero value is not usable;
// use NewTracker.
type Tracker struct {
	mu      sync.Mutex
	refs    map[string]int
	retired map[string]bool
}

// NewTracker returns an empty Tracker.
func NewTracker() *Tracker {
	return &Tracker{refs: map[string]int{}, retired: map[string]bool{}}
}

// Acquire records that path is about to be read, and returns a release
// function the caller must call exactly once when it is done reading --
// typically via defer, immediately after Acquire. Acquire does not open
// or validate the file itself; it only participates in Retire's
// deferred-deletion bookkeeping, so it is safe to call for a path that
// does not (yet, or ever) exist.
func (t *Tracker) Acquire(path string) (release func()) {
	t.mu.Lock()
	t.refs[path]++
	t.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() { t.release(path) })
	}
}

func (t *Tracker) release(path string) {
	t.mu.Lock()
	t.refs[path]--
	shouldDelete := false
	if t.refs[path] <= 0 {
		delete(t.refs, path)
		if t.retired[path] {
			delete(t.retired, path)
			shouldDelete = true
		}
	}
	t.mu.Unlock()

	if shouldDelete {
		// Best-effort: a failure to physically reclaim a file that is
		// already logically retired (absent from the active Manifest)
		// affects disk usage, not correctness -- nothing will ever read
		// it again via the Manifest -- so it is not treated as fatal.
		_ = os.Remove(path)
	}
}

// Retire marks path as no longer part of the active Manifest. If no
// Acquire on path is currently outstanding, the file is deleted
// immediately; otherwise deletion is deferred until the last outstanding
// Acquire's release function runs.
func (t *Tracker) Retire(path string) {
	t.mu.Lock()
	held := t.refs[path] > 0
	if held {
		t.retired[path] = true
	}
	t.mu.Unlock()

	if !held {
		_ = os.Remove(path) // best-effort; see the release doc comment above.
	}
}

// Held reports whether path currently has an outstanding Acquire. It
// exists to make Tracker's behavior observable in tests; correct use of
// Tracker does not require calling it.
func (t *Tracker) Held(path string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.refs[path] > 0
}
