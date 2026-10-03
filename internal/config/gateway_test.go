package config

import (
	"os"
	"reflect"
	"testing"
)

func withEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		old, had := os.LookupEnv(k)
		if v == "" {
			os.Unsetenv(k)
		} else {
			os.Setenv(k, v)
		}
		t.Cleanup(func(k string, had bool, old string) func() {
			return func() {
				if had {
					os.Setenv(k, old)
				} else {
					os.Unsetenv(k)
				}
			}
		}(k, had, old))
	}
}

func TestLoadGateway_DefaultsAndPeers(t *testing.T) {
	withEnv(t, map[string]string{
		EnvGatewayAddr: "",
		EnvPeers:       "node-1=forgedb-1:9090|forgedb-1:8080,node-2=forgedb-2:9090|forgedb-2:8080",
	})

	cfg, err := LoadGateway()
	if err != nil {
		t.Fatalf("LoadGateway() error = %v", err)
	}
	if cfg.Addr != DefaultGatewayAddr {
		t.Errorf("Addr = %q, want default %q", cfg.Addr, DefaultGatewayAddr)
	}
	want := []string{"http://forgedb-1:8080", "http://forgedb-2:8080"}
	if !reflect.DeepEqual(cfg.Backends, want) {
		t.Errorf("Backends = %v, want %v", cfg.Backends, want)
	}
}

func TestLoadGateway_CustomAddr(t *testing.T) {
	withEnv(t, map[string]string{
		EnvGatewayAddr: ":9999",
		EnvPeers:       "node-1=forgedb-1:9090|forgedb-1:8080",
	})

	cfg, err := LoadGateway()
	if err != nil {
		t.Fatalf("LoadGateway() error = %v", err)
	}
	if cfg.Addr != ":9999" {
		t.Errorf("Addr = %q, want :9999", cfg.Addr)
	}
}

func TestLoadGateway_NoHTTPAddrsFails(t *testing.T) {
	withEnv(t, map[string]string{
		EnvGatewayAddr: "",
		// Peers with no HTTP half at all.
		EnvPeers: "node-1=forgedb-1:9090,node-2=forgedb-2:9090",
	})

	_, err := LoadGateway()
	if err == nil {
		t.Fatal("LoadGateway() error = nil, want failure when no peer has an HTTP address")
	}
}

func TestLoadGateway_EmptyPeersFails(t *testing.T) {
	withEnv(t, map[string]string{
		EnvGatewayAddr: "",
		EnvPeers:       "",
	})

	_, err := LoadGateway()
	if err == nil {
		t.Fatal("LoadGateway() error = nil, want failure when PEERS is empty")
	}
}

func TestLoadGateway_InvalidPeersFails(t *testing.T) {
	withEnv(t, map[string]string{
		EnvGatewayAddr: "",
		EnvPeers:       "not-a-valid-entry",
	})

	_, err := LoadGateway()
	if err == nil {
		t.Fatal("LoadGateway() error = nil, want failure on malformed PEERS")
	}
}

func TestLoadGateway_MaxBodyBytes_DefaultsWhenUnset(t *testing.T) {
	withEnv(t, map[string]string{
		EnvGatewayAddr:         "",
		EnvPeers:               "node-1=forgedb-1:9090|forgedb-1:8080",
		EnvGatewayMaxBodyBytes: "",
	})

	cfg, err := LoadGateway()
	if err != nil {
		t.Fatalf("LoadGateway() error = %v", err)
	}
	if cfg.MaxBodyBytes != DefaultGatewayMaxBodyBytes {
		t.Errorf("MaxBodyBytes = %d, want default %d", cfg.MaxBodyBytes, DefaultGatewayMaxBodyBytes)
	}
}

func TestLoadGateway_MaxBodyBytes_CustomValue(t *testing.T) {
	withEnv(t, map[string]string{
		EnvGatewayAddr:         "",
		EnvPeers:               "node-1=forgedb-1:9090|forgedb-1:8080",
		EnvGatewayMaxBodyBytes: "8388608", // 8 MiB
	})

	cfg, err := LoadGateway()
	if err != nil {
		t.Fatalf("LoadGateway() error = %v", err)
	}
	if cfg.MaxBodyBytes != 8388608 {
		t.Errorf("MaxBodyBytes = %d, want 8388608", cfg.MaxBodyBytes)
	}
}

func TestLoadGateway_MaxBodyBytes_NonPositiveFailsFast(t *testing.T) {
	for _, v := range []string{"0", "-1", "not-a-number"} {
		t.Run(v, func(t *testing.T) {
			withEnv(t, map[string]string{
				EnvGatewayAddr:         "",
				EnvPeers:               "node-1=forgedb-1:9090|forgedb-1:8080",
				EnvGatewayMaxBodyBytes: v,
			})
			if _, err := LoadGateway(); err == nil {
				t.Fatalf("LoadGateway() error = nil, want error for GATEWAY_MAX_BODY_BYTES=%s", v)
			}
		})
	}
}

func TestLoadGateway_SchemeAlreadyPresentIsKept(t *testing.T) {
	withEnv(t, map[string]string{
		EnvGatewayAddr: "",
		EnvPeers:       "node-1=forgedb-1:9090|https://forgedb-1:8080",
	})

	cfg, err := LoadGateway()
	if err != nil {
		t.Fatalf("LoadGateway() error = %v", err)
	}
	want := []string{"https://forgedb-1:8080"}
	if !reflect.DeepEqual(cfg.Backends, want) {
		t.Errorf("Backends = %v, want %v", cfg.Backends, want)
	}
}
