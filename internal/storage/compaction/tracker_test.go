package compaction

import (
	"os"
	"path/filepath"
	"testing"
)

func mustWriteFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func TestTrackerDeletesImmediatelyWhenNotHeld(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")
	mustWriteFile(t, path)

	tr := NewTracker()
	tr.Retire(path)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("Retire with no outstanding Acquire: file still exists (stat err=%v), want deleted", err)
	}
}

func TestTrackerDefersDeletionWhileHeld(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")
	mustWriteFile(t, path)

	tr := NewTracker()
	release := tr.Acquire(path)

	tr.Retire(path)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Retire while held: file was deleted (stat err=%v), want still present", err)
	}
	if !tr.Held(path) {
		t.Fatalf("Held: got false, want true while release has not been called")
	}

	release()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("after release of a retired file: file still exists (stat err=%v), want deleted", err)
	}
	if tr.Held(path) {
		t.Fatalf("Held after release: got true, want false")
	}
}

func TestTrackerDeletesOnlyAfterLastRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")
	mustWriteFile(t, path)

	tr := NewTracker()
	release1 := tr.Acquire(path)
	release2 := tr.Acquire(path)

	tr.Retire(path)
	release1()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("after first of two releases: file was deleted (stat err=%v), want still present", err)
	}

	release2()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("after second (last) release: file still exists (stat err=%v), want deleted", err)
	}
}

func TestTrackerReleaseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")
	mustWriteFile(t, path)

	tr := NewTracker()
	release := tr.Acquire(path)
	tr.Retire(path)

	release()
	release() // must not double-decrement or panic

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("after idempotent double release: file still exists (stat err=%v), want deleted", err)
	}
}

func TestTrackerRetireWithoutAcquireOnMissingFileDoesNotPanic(t *testing.T) {
	tr := NewTracker()
	tr.Retire(filepath.Join(t.TempDir(), "does-not-exist.sst")) // must not panic or error out loudly
}

func TestTrackerAcquireAfterRetireIsIndependent(t *testing.T) {
	// A path retired (and deleted, since unheld) before any Acquire is
	// unrelated to a later, distinct Acquire/Retire cycle on the same
	// path string -- Tracker has no notion of a path being "permanently"
	// retired, only of currently-outstanding holds.
	path := filepath.Join(t.TempDir(), "table.sst")
	mustWriteFile(t, path)

	tr := NewTracker()
	tr.Retire(path)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("first Retire: file still exists, want deleted")
	}

	mustWriteFile(t, path) // a new file happens to reuse the same path
	release := tr.Acquire(path)
	tr.Retire(path)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("second Retire while held: file was deleted, want still present")
	}
	release()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("after release: file still exists, want deleted")
	}
}
