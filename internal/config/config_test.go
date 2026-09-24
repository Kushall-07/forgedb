package config

import "testing"

func TestConfig_FieldsPreserved(t *testing.T) {
	cfg := Config{
		NodeID:  "node-1",
		HTTP:    ":8080",
		GRPC:    ":9000",
		DataDir: "./data/node-1",
	}

	if cfg.NodeID != "node-1" {
		t.Errorf("NodeID = %q, want %q", cfg.NodeID, "node-1")
	}
	if cfg.HTTP != ":8080" {
		t.Errorf("HTTP = %q, want %q", cfg.HTTP, ":8080")
	}
	if cfg.GRPC != ":9000" {
		t.Errorf("GRPC = %q, want %q", cfg.GRPC, ":9000")
	}
	if cfg.DataDir != "./data/node-1" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "./data/node-1")
	}
}
