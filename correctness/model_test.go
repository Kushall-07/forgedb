package correctness

import "testing"

// These tests exercise the reference model (model.go) directly,
// independent of the search in checker.go -- pinning down the exact
// sequential KV specification Check replays candidate orderings against.

func TestModel_InitialStateIsEmpty(t *testing.T) {
	m := newModel()
	found, _ := m.apply(Operation{Kind: OpGet, Key: "x"})
	if found {
		t.Fatalf("new model: Get(x) found a value, want not-found")
	}
}

func TestModel_PutThenGet(t *testing.T) {
	m := newModel()
	m.apply(Operation{Kind: OpPut, Key: "x", Value: "1"})
	found, value := m.apply(Operation{Kind: OpGet, Key: "x"})
	if !found || value != "1" {
		t.Fatalf("Get(x) = (%v, %q), want (true, \"1\")", found, value)
	}
}

func TestModel_Overwrite(t *testing.T) {
	m := newModel()
	m.apply(Operation{Kind: OpPut, Key: "x", Value: "1"})
	m.apply(Operation{Kind: OpPut, Key: "x", Value: "2"})
	found, value := m.apply(Operation{Kind: OpGet, Key: "x"})
	if !found || value != "2" {
		t.Fatalf("Get(x) = (%v, %q), want (true, \"2\")", found, value)
	}
}

func TestModel_DeleteThenGet(t *testing.T) {
	m := newModel()
	m.apply(Operation{Kind: OpPut, Key: "x", Value: "1"})
	m.apply(Operation{Kind: OpDelete, Key: "x"})
	found, _ := m.apply(Operation{Kind: OpGet, Key: "x"})
	if found {
		t.Fatalf("Get(x) after Delete: found a value, want not-found")
	}
}

func TestModel_DeleteThenRecreate(t *testing.T) {
	// §56: tombstone semantics at the client-visible level.
	m := newModel()
	m.apply(Operation{Kind: OpPut, Key: "x", Value: "1"})
	m.apply(Operation{Kind: OpDelete, Key: "x"})
	m.apply(Operation{Kind: OpPut, Key: "x", Value: "2"})
	found, value := m.apply(Operation{Kind: OpGet, Key: "x"})
	if !found || value != "2" {
		t.Fatalf("Get(x) after delete+recreate = (%v, %q), want (true, \"2\")", found, value)
	}
}

func TestModel_IndependentKeysCommute(t *testing.T) {
	// §55: independent-key operations must commute -- either application
	// order produces the same final state.
	order1 := newModel()
	order1.apply(Operation{Kind: OpPut, Key: "x", Value: "1"})
	order1.apply(Operation{Kind: OpPut, Key: "y", Value: "2"})

	order2 := newModel()
	order2.apply(Operation{Kind: OpPut, Key: "y", Value: "2"})
	order2.apply(Operation{Kind: OpPut, Key: "x", Value: "1"})

	for _, k := range []string{"x", "y"} {
		f1, v1 := order1.apply(Operation{Kind: OpGet, Key: k})
		f2, v2 := order2.apply(Operation{Kind: OpGet, Key: k})
		if f1 != f2 || v1 != v2 {
			t.Fatalf("key %q: order1=(%v,%q) order2=(%v,%q), want equal", k, f1, v1, f2, v2)
		}
	}
}

func TestModel_CloneIsIndependent(t *testing.T) {
	m := newModel()
	m.apply(Operation{Kind: OpPut, Key: "x", Value: "1"})
	clone := m.clone()
	clone.apply(Operation{Kind: OpPut, Key: "x", Value: "2"})

	_, v := m.apply(Operation{Kind: OpGet, Key: "x"})
	if v != "1" {
		t.Fatalf("mutating a clone changed the original: Get(x) = %q, want \"1\"", v)
	}
}

func TestModel_MatchesDistinguishesFoundFromValue(t *testing.T) {
	m := newModel()
	m.apply(Operation{Kind: OpPut, Key: "x", Value: "1"})

	if !m.matches(Operation{Kind: OpGet, Key: "x", Found: true, Result: "1"}) {
		t.Fatalf("matches: correct (found, value) rejected")
	}
	if m.matches(Operation{Kind: OpGet, Key: "x", Found: true, Result: "2"}) {
		t.Fatalf("matches: wrong value accepted")
	}
	if m.matches(Operation{Kind: OpGet, Key: "x", Found: false}) {
		t.Fatalf("matches: wrong found=false accepted for a present key")
	}
}

func TestModel_MatchesAlwaysTrueForWrites(t *testing.T) {
	m := newModel()
	if !m.matches(Operation{Kind: OpPut, Key: "x", Value: "1"}) {
		t.Fatalf("matches: Put must always match (nothing to mismatch)")
	}
	if !m.matches(Operation{Kind: OpDelete, Key: "x"}) {
		t.Fatalf("matches: Delete must always match (nothing to mismatch)")
	}
}
