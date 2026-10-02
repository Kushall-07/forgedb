package correctness

// model is Check's reference sequential KV specification (§10 of the
// Phase 11 brief): a plain map, matching ForgeDB's actual documented KV
// semantics (internal/storage.Store.Get/Put/Delete and
// statemachine.KVStateMachine) exactly:
//
//	PUT(k, v):    state[k] = v                     -- always succeeds
//	DELETE(k):    delete(state, k)                 -- always succeeds
//	GET(k):       found, v := state[k]             -- always succeeds;
//	              "not found" is itself a valid, successful answer, never
//	              an error (see storage.ErrKeyNotFound, which ForgeDB's
//	              real Get/ConsistentGet paths return exactly when the key
//	              has no live value -- deleted or never written)
//
// The initial state is always the empty map -- no key has ever been
// written (§53). A test that seeds a live cluster with pre-existing data
// before recording a History must account for that data as ordinary
// recorded Put operations preceding everything else, never as an
// un-recorded assumption baked into the model's starting state.
//
// model only ever represents *reference* state used by the search in
// checker.go; it has no connection to any live ForgeDB node's actual
// storage.
type model struct {
	state map[string]string
}

func newModel() *model {
	return &model{state: make(map[string]string)}
}

// clone returns an independent copy of m, so the search in checker.go can
// try a candidate operation against a tentative state without disturbing
// the state a sibling branch (a different candidate order) needs to start
// from.
func (m *model) clone() *model {
	cp := make(map[string]string, len(m.state))
	for k, v := range m.state {
		cp[k] = v
	}
	return &model{state: cp}
}

// apply executes op against m's current state, mutating it, and returns
// the (found, value) a GET would observe -- meaningful only when op.Kind
// == OpGet. PUT and DELETE always succeed against this model by
// construction (there is no way to construct a storage-level validation
// failure here): Check only ever calls apply with operations already
// filtered to History.Completed(), i.e. operations ForgeDB itself already
// resolved as successful, so there is nothing left for the reference
// model to reject.
func (m *model) apply(op Operation) (found bool, value string) {
	switch op.Kind {
	case OpPut:
		m.state[op.Key] = op.Value
		return false, ""
	case OpDelete:
		delete(m.state, op.Key)
		return false, ""
	case OpGet:
		v, ok := m.state[op.Key]
		return ok, v
	default:
		return false, ""
	}
}

// matches reports whether applying op against m (without mutating m --
// see apply) would reproduce exactly the result op's own Outcome/Found/
// Result fields already recorded as observed. This is Check's Rule 2
// (§11): a candidate linearization is only valid at a given point if
// every operation placed there actually observes what it is recorded to
// have observed.
//
// matches performs the real mutation via apply; callers that need to
// reject a candidate without committing it must call this against a
// clone (see checker.go).
func (m *model) matches(op Operation) bool {
	found, value := m.apply(op)
	if op.Kind != OpGet {
		return true // Put/Delete have no observable return value to mismatch
	}
	if found != op.Found {
		return false
	}
	return !found || value == op.Result
}
