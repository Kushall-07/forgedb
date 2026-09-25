package main

import (
	"github.com/Kushall-07/forgedb/internal/metrics"
	"github.com/Kushall-07/forgedb/internal/storage"
)

func main() {
	metrics.Info("ForgeDB starting...")

	// Minimal in-process demonstration that the Phase 1 storage engine
	// works end to end. This is not a server: no network listeners are
	// started here.
	store := storage.NewMemStore()
	defer store.Close()

	if err := store.Put([]byte("hello"), []byte("world")); err != nil {
		metrics.Error("storage demo failed: " + err.Error())
		return
	}
	value, err := store.Get([]byte("hello"))
	if err != nil {
		metrics.Error("storage demo failed: " + err.Error())
		return
	}
	metrics.Info("storage engine ready: hello=" + string(value))
}
