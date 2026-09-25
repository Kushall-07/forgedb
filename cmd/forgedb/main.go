package main

import (
	"os"

	"github.com/Kushall-07/forgedb/internal/metrics"
	"github.com/Kushall-07/forgedb/internal/storage"
)

func main() {
	metrics.Info("ForgeDB starting...")

	// Minimal in-process demonstration that the Phase 2 WAL-backed
	// storage engine works end to end, including recovery across a
	// close/reopen cycle. This is not a server: no network listeners are
	// started here.
	if err := run("data"); err != nil {
		metrics.Error("storage demo failed: " + err.Error())
		os.Exit(1)
	}
}

func run(dataDir string) error {
	store, err := storage.NewMemStore(dataDir)
	if err != nil {
		return err
	}
	if err := store.Put([]byte("hello"), []byte("world")); err != nil {
		store.Close()
		return err
	}
	value, err := store.Get([]byte("hello"))
	if err != nil {
		store.Close()
		return err
	}
	metrics.Info("storage engine ready: hello=" + string(value))
	if err := store.Close(); err != nil {
		return err
	}

	// Reopen against the same data directory to demonstrate that the WAL
	// reconstructs the MemTable across a restart.
	reopened, err := storage.NewMemStore(dataDir)
	if err != nil {
		return err
	}
	defer reopened.Close()

	recovered, err := reopened.Get([]byte("hello"))
	if err != nil {
		return err
	}
	metrics.Info("recovered after reopen: hello=" + string(recovered))
	return nil
}
