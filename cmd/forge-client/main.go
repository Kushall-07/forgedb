// Command forge-client is a minimal CLI client for a deployed ForgeDB
// cluster's HTTP API (internal/api's /kv/{key} surface -- Phase 14's
// addition, see that package's doc comment). It speaks only HTTP to a
// host-exposed address such as http://localhost:8081; it has no
// knowledge of Raft terms, gRPC, or peer addresses, exactly as
// docs/deployment/phase14-docker-deployment.md's client-compatibility
// requirement describes.
//
// Usage:
//
//	forge-client -addrs http://localhost:8081,http://localhost:8082,http://localhost:8083 -token <token> put <key> <value>
//	forge-client -addrs http://localhost:8081,http://localhost:8082,http://localhost:8083 -token <token> get <key>
//	forge-client -addrs http://localhost:8081,http://localhost:8082,http://localhost:8083 -token <token> delete <key>
//
// -token (or FORGE_CLIENT_TOKEN) must match the server's FORGEDB_API_TOKEN.
//
// -addrs lists every node's client-facing HTTP address the operator
// knows of (in no particular order); forge-client does not need to be
// told which one is the leader. Since Phase E, /kv is authenticated
// (see internal/api/auth.go): forge-client requires a bearer token via
// -token or FORGE_CLIENT_TOKEN and sends it as "Authorization: Bearer
// <token>" on every request; it is never logged or printed. A request sent
// to a follower gets back a
// 421 Misdirected Request with a JSON {"error":"not_leader","leader_id":
// ...,"leader_http":...} body (see internal/api/kv.go); when leader_http
// is present, forge-client retries there directly, and otherwise falls
// back to simply trying the next configured address, bounded by a fixed
// number of attempts -- ForgeDB does not invent a more elaborate client
// routing protocol than that hint (see
// docs/deployment/phase14-docker-deployment.md's "leader redirection"
// section).
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultTimeout = 5 * time.Second
	maxAttempts    = 6 // generous enough to survive one failover hop plus a couple of address retries
)

type notLeaderBody struct {
	Error      string `json:"error"`
	LeaderID   string `json:"leader_id"`
	LeaderHTTP string `json:"leader_http"`
}

func main() {
	addrsFlag := ""
	tokenFlag := ""
	args := os.Args[1:]
	args = extractFlag(args, "-addrs", &addrsFlag)
	args = extractFlag(args, "-token", &tokenFlag)

	if addrsFlag == "" {
		addrsFlag = os.Getenv("FORGE_CLIENT_ADDRS")
	}
	if addrsFlag == "" {
		fmt.Fprintln(os.Stderr, "forge-client: -addrs (or FORGE_CLIENT_ADDRS) is required, e.g. -addrs http://localhost:8081,http://localhost:8082,http://localhost:8083")
		os.Exit(2)
	}
	if tokenFlag == "" {
		tokenFlag = os.Getenv("FORGE_CLIENT_TOKEN")
	}
	if tokenFlag == "" {
		fmt.Fprintln(os.Stderr, "forge-client: -token (or FORGE_CLIENT_TOKEN) is required -- /kv requires the same bearer token the server was started with (FORGEDB_API_TOKEN)")
		os.Exit(2)
	}
	addrs := splitNonEmpty(addrsFlag, ",")
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "forge-client: -addrs must list at least one address")
		os.Exit(2)
	}

	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: forge-client -addrs <addr1,addr2,...> <put|get|delete> <key> [value]")
		os.Exit(2)
	}

	cmd, key := args[0], args[1]
	client := &http.Client{Timeout: defaultTimeout}

	var (
		result []byte
		status int
		err    error
	)
	switch cmd {
	case "put":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: forge-client ... put <key> <value>")
			os.Exit(2)
		}
		status, result, err = doRequest(client, addrs, tokenFlag, http.MethodPut, key, []byte(args[2]))
	case "get":
		status, result, err = doRequest(client, addrs, tokenFlag, http.MethodGet, key, nil)
	case "delete":
		status, result, err = doRequest(client, addrs, tokenFlag, http.MethodDelete, key, nil)
	default:
		fmt.Fprintf(os.Stderr, "forge-client: unknown command %q (want put|get|delete)\n", cmd)
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "forge-client: %v\n", err)
		os.Exit(1)
	}
	if status == http.StatusNotFound {
		fmt.Fprintln(os.Stderr, "forge-client: key not found")
		os.Exit(1)
	}
	if status >= 400 {
		fmt.Fprintf(os.Stderr, "forge-client: request failed: status %d: %s\n", status, result)
		os.Exit(1)
	}
	os.Stdout.Write(result)
	if cmd != "get" {
		fmt.Println()
	}
}

// doRequest sends method/key[/body] to the first address in addrs,
// following a not_leader response's leader_http hint (or, absent a
// hint, simply trying the next configured address) up to maxAttempts
// times total. It never requires the caller to know which node is
// currently leader.
func doRequest(client *http.Client, addrs []string, token, method, key string, body []byte) (int, []byte, error) {
	if len(addrs) == 0 {
		return 0, nil, errors.New("no addresses to try")
	}

	next := addrs[0]
	tried := make(map[string]bool)
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if tried[next] {
			// Already tried this address and it didn't resolve the
			// request (e.g. it pointed back at itself, or at a node
			// we've already seen) -- fall back to the next untried
			// configured address instead of looping forever.
			advanced := false
			for _, a := range addrs {
				if !tried[a] {
					next = a
					advanced = true
					break
				}
			}
			if !advanced {
				break
			}
		}
		tried[next] = true

		url := strings.TrimRight(next, "/") + "/kv/" + key
		req, err := http.NewRequest(method, url, bytesReader(body))
		if err != nil {
			return 0, nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", next, err)
			// Try the next configured address; this node may simply be
			// down or unreachable.
			for _, a := range addrs {
				if !tried[a] {
					next = a
					break
				}
			}
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return 0, nil, fmt.Errorf("%s: read response: %w", next, readErr)
		}

		if resp.StatusCode == http.StatusMisdirectedRequest {
			var nl notLeaderBody
			if err := json.Unmarshal(data, &nl); err == nil && nl.LeaderHTTP != "" {
				next = nl.LeaderHTTP
				if !strings.Contains(next, "://") {
					next = "http://" + next
				}
				continue
			}
			// No usable hint: fall back to the next configured address.
			for _, a := range addrs {
				if !tried[a] {
					next = a
					break
				}
			}
			continue
		}

		return resp.StatusCode, data, nil
	}

	if lastErr != nil {
		return 0, nil, fmt.Errorf("exhausted %d attempts, last error: %w", maxAttempts, lastErr)
	}
	return 0, nil, fmt.Errorf("exhausted %d attempts without finding a leader", maxAttempts)
}

func bytesReader(b []byte) io.Reader {
	if b == nil {
		return nil
	}
	return strings.NewReader(string(b))
}

// extractFlag pulls "-name value" (or "-name=value") out of args,
// writing the value into dst and returning args with it removed. This
// avoids pulling in the flag package's subcommand-unfriendly parsing for
// forge-client's tiny "flag then positional verb/key/value" grammar.
func extractFlag(args []string, name string, dst *string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == name && i+1 < len(args):
			*dst = args[i+1]
			i++
		case strings.HasPrefix(a, name+"="):
			*dst = strings.TrimPrefix(a, name+"=")
		default:
			out = append(out, a)
		}
	}
	return out
}

func splitNonEmpty(s, sep string) []string {
	var out []string
	for _, p := range strings.Split(s, sep) {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
