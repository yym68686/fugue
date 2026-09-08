package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/pflag"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestCanonicalReplacementsPreserveR1Baseline(t *testing.T) {
	raw, err := os.ReadFile("../../docs/cli-refactor-audit-2026-09-08/migration-parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var baseline struct {
		Cases []struct {
			Old         string            `json:"old"`
			Replacement string            `json:"replacement"`
			Flags       []commandFlagInfo `json:"flags"`
		}
	}
	if err = json.Unmarshal(raw, &baseline); err != nil {
		t.Fatal(err)
	}
	root := newCLI(&bytes.Buffer{}, &bytes.Buffer{}).newRootCommand()
	for _, old := range baseline.Cases {
		t.Run(old.Old, func(t *testing.T) {
			cmd, rest, err := root.Find(strings.Fields(strings.TrimPrefix(old.Replacement, "fugue ")))
			if err != nil || len(rest) > 0 {
				t.Fatalf("missing replacement: %v %v", err, rest)
			}
			flags := pflag.NewFlagSet("parity", pflag.ContinueOnError)
			flags.AddFlagSet(cmd.InheritedFlags())
			flags.AddFlagSet(cmd.Flags())
			for _, flag := range old.Flags {
				if flag.Name == "type" && old.Old == "fugue service create" {
					continue
				}
				if strings.HasPrefix(old.Replacement, "fugue app failover policy ") && isRolloutOnlyFlag(flag.Name) {
					continue
				}
				now := flags.Lookup(flag.Name)
				if now == nil {
					t.Errorf("missing flag %s", flag.Name)
					continue
				}
				if now.Value.Type() != flag.Type || now.DefValue != flag.Default {
					t.Errorf("flag %s changed from %s=%s to %s=%s", flag.Name, flag.Type, flag.Default, now.Value.Type(), now.DefValue)
				}
			}
		})
	}
}

func TestTrafficShowIsReadOnlyAndReleaseListUsesVersions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("unexpected mutation: %s", r.Method)
		}
		switch r.URL.Path {
		case "/v1/apps":
			fmt.Fprint(w, `{"apps":[{"id":"app_demo","name":"demo"}]}`)
		case "/v1/apps/app_demo/traffic":
			fmt.Fprint(w, `{"traffic":{"stable_weight":100,"candidate_weight":0}}`)
		case "/v1/apps/app_demo/releases":
			fmt.Fprint(w, `{"releases":[{"id":"release_demo","role":"stable","status":"serving"}]}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	for _, args := range [][]string{{"app", "traffic", "show", "demo"}, {"app", "release", "versions", "demo"}} {
		var out, stderr bytes.Buffer
		err := runWithStreams(append([]string{"--base-url", srv.URL, "--token", "test", "--json"}, args...), &out, &stderr)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal(out.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		if args[1] == "traffic" && payload["observed_state"] != "unknown" {
			t.Fatal(payload)
		}
		if args[1] == "release" && payload["releases"] == nil {
			t.Fatal(payload)
		}
	}
}
func TestCanarySecondStepFailurePreservesCreatedRelease(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/apps":
			fmt.Fprint(w, `{"apps":[{"id":"app_demo","name":"demo"}]}`)
		case r.URL.Path == "/v1/apps/app_demo/releases":
			fmt.Fprint(w, `{"release":{"id":"release_demo","role":"candidate"}}`)
		case r.Method == "GET":
			fmt.Fprint(w, `{"traffic":{"stable_weight":100}}`)
		case r.Method == "PATCH":
			w.WriteHeader(409)
			fmt.Fprint(w, `{"error":"generation changed"}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()
	var out, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", srv.URL, "--token", "test", "--json", "app", "release", "canary", "start", "demo"}, &out, &stderr)
	if err == nil {
		t.Fatal("wanted failure")
	}
	var payload map[string]any
	if err := json.Unmarshal(out.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["outcome"] != "partial" || payload["release"].(map[string]any)["id"] != "release_demo" {
		t.Fatal(payload)
	}
}
