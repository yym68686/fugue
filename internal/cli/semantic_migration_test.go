package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestCanonicalReplacementsPreserveFlagsAndDefaults(t *testing.T) {
	pairs := [][2]string{
		{"env ls", "app env ls"}, {"files get", "app config get"}, {"workspace put", "app fs put"}, {"app workspace ls", "app fs ls"}, {"curl", "api request"},
		{"template inspect", "deploy inspect"}, {"deploy plan", "deploy inspect"}, {"app route set", "app domain primary set"}, {"app route check", "app domain primary check"}, {"app route show", "app domain primary verify"},
		{"app binding attach", "app service attach"}, {"app redeploy", "app deploy"}, {"app rebuild", "app build"}, {"app release deploy", "app deploy"}, {"app release rollback", "app rollback"},
		{"project show", "project overview"}, {"project storage", "project images usage"}, {"project usage", "project images usage"}, {"runtime attach", "runtime enroll create"}, {"runtime access grant", "admin runtime access grant"},
		{"runtime pool set", "admin runtime pool set"}, {"runtime offer set", "admin runtime offer set"}, {"runtime delete", "admin runtime delete"}, {"app failover configure", "app failover policy set"}, {"app failover disable", "app failover policy clear"},
		{"app sync resume", "app source sync resume"}, {"app sync run", "app source sync run"}, {"app continuity show", "app rollout policy show"},
		{"app release ls", "app image ls"}, {"app release policy set", "app image retention set"}, {"app release tracking set", "app image tracking set"}, {"app release prune", "app image prune"}, {"app release traffic", "app traffic set"},
	}
	for _, pair := range pairs {
		t.Run(pair[0], func(t *testing.T) {
			root := newCLI(&bytes.Buffer{}, &bytes.Buffer{}).newRootCommand()
			resolve := func(path string) *cobra.Command {
				cmd, rest, err := root.Find(strings.Fields(path))
				if err != nil || len(rest) > 0 {
					t.Fatalf("%s: %v %v", path, err, rest)
				}
				return cmd
			}
			flags := func(cmd *cobra.Command) map[string]string {
				out := map[string]string{}
				cmd.Flags().VisitAll(func(f *pflag.Flag) { out[f.Name] = f.Value.Type() + "=" + f.DefValue })
				return out
			}
			if a, b := flags(resolve(pair[0])), flags(resolve(pair[1])); !reflect.DeepEqual(a, b) {
				t.Fatalf("old flags=%v new=%v", a, b)
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
