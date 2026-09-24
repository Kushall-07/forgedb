package config

import "testing"

func TestNode_FieldsPreserved(t *testing.T) {
	n := Node{
		ID:   "node-1",
		HTTP: ":8080",
		GRPC: ":9000",
	}

	if n.ID != "node-1" {
		t.Errorf("ID = %q, want %q", n.ID, "node-1")
	}
	if n.HTTP != ":8080" {
		t.Errorf("HTTP = %q, want %q", n.HTTP, ":8080")
	}
	if n.GRPC != ":9000" {
		t.Errorf("GRPC = %q, want %q", n.GRPC, ":9000")
	}
}

func TestNode_DistinctIdentitiesRemainIndependent(t *testing.T) {
	n1 := Node{ID: "node-1", HTTP: ":8080", GRPC: ":9000"}
	n2 := Node{ID: "node-2", HTTP: ":8081", GRPC: ":9001"}

	if n1.ID == n2.ID {
		t.Fatalf("expected distinct node IDs, both were %q", n1.ID)
	}
	if n1.HTTP == n2.HTTP {
		t.Fatalf("expected distinct HTTP addresses, both were %q", n1.HTTP)
	}
}
