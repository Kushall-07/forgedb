package statemachine

// Result is the deterministic outcome of applying one Command. Every
// replica that applies the same Command against the same prior state
// produces an identical Result (see StateMachine's determinism
// requirement), which is what makes it safe to cache and replay for
// request deduplication.
type Result struct {
	// Applied is true only when this call caused a real mutation against
	// storage. It is false for a replayed duplicate (see Replayed), a
	// request-ID conflict, a stale request, or any other outcome that
	// never reached storage.
	Applied bool

	// Replayed is true when this Result was not just computed but copied
	// from a prior application of the identical (ClientID, RequestID,
	// Op, Key, Value) -- i.e. a genuine client retry -- rather than a
	// fresh execution. Storage is never touched a second time to produce
	// a replayed Result.
	Replayed bool

	// Key and Value echo the command's own key/value on success (Value is
	// always empty for a Delete). They are meaningless when Err is set.
	Key   []byte
	Value []byte

	// Err reports a deterministic, resolved failure: ErrRequestIDConflict,
	// ErrStaleRequest, or a validation error surfaced by storage (e.g.
	// storage.ErrEmptyKey). Err is distinct from the error StateMachine.Apply
	// itself returns -- see Apply's doc comment -- a non-nil Err here still
	// means the command's log entry was fully and deterministically
	// resolved and must count as applied.
	Err error
}
