package config

import (
	"reflect"
	"testing"
	"time"
)

func TestParsePeers_Empty(t *testing.T) {
	peers, err := ParsePeers("")
	if err != nil {
		t.Fatalf("ParsePeers(\"\") error = %v, want nil", err)
	}
	if peers != nil {
		t.Fatalf("ParsePeers(\"\") = %v, want nil", peers)
	}
}

func TestParsePeers_GRPCOnly(t *testing.T) {
	peers, err := ParsePeers("node-1=forgedb-1:9091,node-2=forgedb-2:9092")
	if err != nil {
		t.Fatalf("ParsePeers error = %v", err)
	}
	want := []Node{
		{ID: "node-1", GRPC: "forgedb-1:9091"},
		{ID: "node-2", GRPC: "forgedb-2:9092"},
	}
	if !reflect.DeepEqual(peers, want) {
		t.Fatalf("ParsePeers = %+v, want %+v", peers, want)
	}
}

func TestParsePeers_WithHTTP(t *testing.T) {
	peers, err := ParsePeers("node-1=forgedb-1:9091|forgedb-1:8081")
	if err != nil {
		t.Fatalf("ParsePeers error = %v", err)
	}
	want := []Node{{ID: "node-1", GRPC: "forgedb-1:9091", HTTP: "forgedb-1:8081"}}
	if !reflect.DeepEqual(peers, want) {
		t.Fatalf("ParsePeers = %+v, want %+v", peers, want)
	}
}

func TestParsePeers_InvalidEntry(t *testing.T) {
	cases := []string{
		"node-1",             // no '='
		"=forgedb-1:9091",    // empty id
		"node-1=",            // empty addr
		"node-1=|forgedb:80", // empty grpc half
	}
	for _, c := range cases {
		if _, err := ParsePeers(c); err == nil {
			t.Errorf("ParsePeers(%q) error = nil, want error", c)
		}
	}
}

func validConfig() Config {
	return Config{
		NodeID:        "node-1",
		HTTP:          ":8081",
		GRPC:          ":9091",
		DataDir:       "data",
		APIToken:      "test-token",
		MaxValueBytes: DefaultMaxValueBytes,
		Peers: []Node{
			{ID: "node-1", GRPC: ":9091", HTTP: ":8081"},
			{ID: "node-2", GRPC: "forgedb-2:9092", HTTP: "forgedb-2:8082"},
			{ID: "node-3", GRPC: "forgedb-3:9093", HTTP: "forgedb-3:8083"},
		},
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestConfig_Validate_NoPeers(t *testing.T) {
	cfg := validConfig()
	cfg.Peers = nil
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil (zero peers is a valid single-node config)", err)
	}
}

func TestConfig_Validate_MissingNodeID(t *testing.T) {
	cfg := validConfig()
	cfg.NodeID = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error for empty NodeID")
	}
}

func TestConfig_Validate_InvalidHTTPAddr(t *testing.T) {
	cfg := validConfig()
	cfg.HTTP = "not-a-valid-addr"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error for invalid HTTP address")
	}
}

func TestConfig_Validate_InvalidGRPCAddr(t *testing.T) {
	cfg := validConfig()
	cfg.GRPC = "not-a-valid-addr"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error for invalid GRPC address")
	}
}

func TestConfig_Validate_EmptyDataDir(t *testing.T) {
	cfg := validConfig()
	cfg.DataDir = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error for empty DataDir")
	}
}

func TestConfig_Validate_MissingAPIToken(t *testing.T) {
	cfg := validConfig()
	cfg.APIToken = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error when APIToken is empty -- the API must never start unauthenticated")
	}
}

func TestConfig_Validate_DuplicatePeerID(t *testing.T) {
	cfg := validConfig()
	cfg.Peers = append(cfg.Peers, Node{ID: "node-2", GRPC: "other:9999"})
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error for duplicate peer id")
	}
}

func TestConfig_Validate_DuplicatePeerGRPCAddr(t *testing.T) {
	cfg := validConfig()
	cfg.Peers = append(cfg.Peers, Node{ID: "node-4", GRPC: "forgedb-2:9092"})
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error for two peer ids sharing a gRPC address")
	}
}

func TestConfig_Validate_SelfAddressMismatch(t *testing.T) {
	cfg := validConfig()
	for i, p := range cfg.Peers {
		if p.ID == cfg.NodeID {
			cfg.Peers[i].GRPC = "wrong-address:1234"
		}
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error for self peer entry mismatching configured GRPC address")
	}
}

// TestConfig_Validate_WildcardBindWithHostnameAdvertise is a direct
// regression test for a real defect found running the actual 3-node
// Docker Compose cluster: Validate used to require the self peer entry's
// gRPC address to be byte-for-byte identical to the configured GRPC bind
// address, which rejected the completely standard and correct Docker
// pattern every service in docker-compose.yml actually uses -- binding
// 0.0.0.0:9090 while being dialed by peers as "forgedb-1:9090" (the
// Compose service name). All three containers crash-looped on this
// check before the fix.
func TestConfig_Validate_WildcardBindWithHostnameAdvertise(t *testing.T) {
	cfg := Config{
		NodeID:        "node-1",
		HTTP:          "0.0.0.0:8080",
		GRPC:          "0.0.0.0:9090",
		DataDir:       "/data",
		APIToken:      "test-token",
		MaxValueBytes: DefaultMaxValueBytes,
		Peers: []Node{
			{ID: "node-1", GRPC: "forgedb-1:9090", HTTP: "forgedb-1:8080"},
			{ID: "node-2", GRPC: "forgedb-2:9090", HTTP: "forgedb-2:8080"},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil for a wildcard bind advertised under a hostname", err)
	}
}

func TestConfig_Validate_WildcardBindWrongAdvertisedPort(t *testing.T) {
	cfg := Config{
		NodeID:        "node-1",
		HTTP:          "0.0.0.0:8080",
		GRPC:          "0.0.0.0:9090",
		DataDir:       "/data",
		APIToken:      "test-token",
		MaxValueBytes: DefaultMaxValueBytes,
		Peers: []Node{
			{ID: "node-1", GRPC: "forgedb-1:9999", HTTP: "forgedb-1:8080"}, // wrong port
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error when the self peer entry advertises the wrong port even under a wildcard bind")
	}
}

func TestConfig_Validate_PeerInvalidGRPCAddr(t *testing.T) {
	cfg := validConfig()
	cfg.Peers = append(cfg.Peers, Node{ID: "node-4", GRPC: "garbage"})
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error for a peer with an invalid gRPC address")
	}
}

func TestConfig_PeerIDs_ExcludesSelf(t *testing.T) {
	cfg := validConfig()
	got := cfg.PeerIDs()
	want := []string{"node-2", "node-3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PeerIDs() = %v, want %v", got, want)
	}
}

func TestConfig_PeerGRPCAddrs_ExcludesSelf(t *testing.T) {
	cfg := validConfig()
	got := cfg.PeerGRPCAddrs()
	want := map[string]string{"node-2": "forgedb-2:9092", "node-3": "forgedb-3:9093"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PeerGRPCAddrs() = %v, want %v", got, want)
	}
}

func TestConfig_PeerHTTPAddrs_IncludesSelf(t *testing.T) {
	cfg := validConfig()
	got := cfg.PeerHTTPAddrs()
	want := map[string]string{"node-1": ":8081", "node-2": "forgedb-2:8082", "node-3": "forgedb-3:8083"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PeerHTTPAddrs() = %v, want %v", got, want)
	}
}

func TestLoad_RequiresNodeID(t *testing.T) {
	t.Setenv(EnvNodeID, "")
	t.Setenv(EnvPeers, "")
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error when NODE_ID is unset")
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv(EnvNodeID, "node-1")
	t.Setenv(EnvHTTPAddr, "")
	t.Setenv(EnvGRPCAddr, "")
	t.Setenv(EnvDataDir, "")
	t.Setenv(EnvPeers, "")
	t.Setenv(EnvLogLevel, "")
	t.Setenv(EnvMetricsEnabled, "")
	t.Setenv(EnvRPCTimeout, "")
	t.Setenv(EnvTickInterval, "")
	t.Setenv(EnvAPIToken, "test-token")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTP != ":8080" || cfg.GRPC != ":9090" || cfg.DataDir != "data" || cfg.LogLevel != "info" {
		t.Fatalf("Load() defaults = %+v, want HTTP=:8080 GRPC=:9090 DataDir=data LogLevel=info", cfg)
	}
	if !cfg.MetricsEnabled {
		t.Fatal("Load() MetricsEnabled = false, want true by default")
	}
	if cfg.RPCTimeout != 0 || cfg.TickInterval != 0 {
		t.Fatalf("Load() RPCTimeout/TickInterval = %v/%v, want zero (package defaults apply downstream)", cfg.RPCTimeout, cfg.TickInterval)
	}
}

func TestLoad_ParsesPeersAndDurations(t *testing.T) {
	t.Setenv(EnvNodeID, "node-1")
	t.Setenv(EnvHTTPAddr, ":8081")
	t.Setenv(EnvGRPCAddr, ":9091")
	t.Setenv(EnvPeers, "node-1=:9091|:8081,node-2=forgedb-2:9092|forgedb-2:8082")
	t.Setenv(EnvRPCTimeout, "3s")
	t.Setenv(EnvTickInterval, "150ms")
	t.Setenv(EnvAPIToken, "test-token")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.Peers) != 2 {
		t.Fatalf("Load() Peers = %+v, want 2 entries", cfg.Peers)
	}
	if cfg.RPCTimeout != 3*time.Second {
		t.Errorf("RPCTimeout = %v, want 3s", cfg.RPCTimeout)
	}
	if cfg.TickInterval != 150*time.Millisecond {
		t.Errorf("TickInterval = %v, want 150ms", cfg.TickInterval)
	}
}

func TestLoad_InvalidPeersFailsFast(t *testing.T) {
	t.Setenv(EnvNodeID, "node-1")
	t.Setenv(EnvPeers, "garbage-no-equals-sign")
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for malformed PEERS")
	}
}

func TestLoad_RequiresAPIToken(t *testing.T) {
	t.Setenv(EnvNodeID, "node-1")
	t.Setenv(EnvPeers, "")
	t.Setenv(EnvAPIToken, "")
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error when FORGEDB_API_TOKEN is unset")
	}
}

func TestLoad_InvalidMetricsEnabledFailsFast(t *testing.T) {
	t.Setenv(EnvNodeID, "node-1")
	t.Setenv(EnvPeers, "")
	t.Setenv(EnvMetricsEnabled, "not-a-bool")
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for invalid METRICS_ENABLED")
	}
}

func TestLoad_MaxValueBytes_DefaultsWhenUnset(t *testing.T) {
	t.Setenv(EnvNodeID, "node-1")
	t.Setenv(EnvPeers, "")
	t.Setenv(EnvAPIToken, "test-token")
	t.Setenv(EnvMaxValueBytes, "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.MaxValueBytes != DefaultMaxValueBytes {
		t.Fatalf("MaxValueBytes = %d, want default %d", cfg.MaxValueBytes, DefaultMaxValueBytes)
	}
}

func TestLoad_MaxValueBytes_CustomValue(t *testing.T) {
	t.Setenv(EnvNodeID, "node-1")
	t.Setenv(EnvPeers, "")
	t.Setenv(EnvAPIToken, "test-token")
	t.Setenv(EnvMaxValueBytes, "2097152") // 2 MiB

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.MaxValueBytes != 2097152 {
		t.Fatalf("MaxValueBytes = %d, want 2097152", cfg.MaxValueBytes)
	}
}

func TestLoad_MaxValueBytes_NonNumericFailsFast(t *testing.T) {
	t.Setenv(EnvNodeID, "node-1")
	t.Setenv(EnvPeers, "")
	t.Setenv(EnvAPIToken, "test-token")
	t.Setenv(EnvMaxValueBytes, "not-a-number")
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for non-numeric MAX_VALUE_BYTES")
	}
}

func TestLoad_MaxValueBytes_NonPositiveFailsFast(t *testing.T) {
	for _, v := range []string{"0", "-1"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv(EnvNodeID, "node-1")
			t.Setenv(EnvPeers, "")
			t.Setenv(EnvAPIToken, "test-token")
			t.Setenv(EnvMaxValueBytes, v)
			if _, err := Load(); err == nil {
				t.Fatalf("Load() error = nil, want error for MAX_VALUE_BYTES=%s (must never silently reduce to a non-positive limit)", v)
			}
		})
	}
}

func TestLoad_CORSOrigins_DefaultsToNil(t *testing.T) {
	t.Setenv(EnvNodeID, "node-1")
	t.Setenv(EnvPeers, "")
	t.Setenv(EnvAPIToken, "test-token")
	t.Setenv(EnvCORSOrigins, "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CORSOrigins != nil {
		t.Fatalf("CORSOrigins = %v, want nil (CORS disabled) when FORGEDB_CORS_ORIGINS is unset", cfg.CORSOrigins)
	}
}

func TestLoad_CORSOrigins_ParsesCommaSeparatedList(t *testing.T) {
	t.Setenv(EnvNodeID, "node-1")
	t.Setenv(EnvPeers, "")
	t.Setenv(EnvAPIToken, "test-token")
	t.Setenv(EnvCORSOrigins, "https://dashboard.example.vercel.app, http://localhost:5173 ,")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []string{"https://dashboard.example.vercel.app", "http://localhost:5173"}
	if !reflect.DeepEqual(cfg.CORSOrigins, want) {
		t.Fatalf("CORSOrigins = %v, want %v", cfg.CORSOrigins, want)
	}
}
