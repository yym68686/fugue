package model

import (
	"strings"
	"testing"
)

func TestParseEdgeRequestBodyPoliciesNormalizesAndMatchesExactRoutes(t *testing.T) {
	t.Parallel()

	policies, err := ParseEdgeRequestBodyPolicies(`[
		{
			"name":"source-imports",
			"methods":["post", "POST"],
			"paths":["/api/fugue/projects/create-and-import-upload", "/api/fugue/apps/import-upload"],
			"max_bytes":167772160,
			"timeout_seconds":300,
			"max_concurrent":2
		}
	]`)
	if err != nil {
		t.Fatalf("parse request body policies: %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("expected one policy, got %#v", policies)
	}
	policy := policies[0]
	if got := strings.Join(policy.Methods, ","); got != "POST" {
		t.Fatalf("expected normalized POST method, got %q", got)
	}
	if policy.RetryAfterSeconds != 5 {
		t.Fatalf("expected default retry-after 5, got %d", policy.RetryAfterSeconds)
	}
	if matched, ok := MatchEdgeRequestBodyPolicy(policies, "POST", "/api/fugue/apps/import-upload"); !ok || matched.Name != policy.Name {
		t.Fatalf("expected exact route match, got policy=%#v ok=%t", matched, ok)
	}
	for _, requestPath := range []string{
		"/api/fugue/apps/import-upload/",
		"/api/fugue/apps/import-upload/child",
		"/api/upload",
	} {
		if _, ok := MatchEdgeRequestBodyPolicy(policies, "POST", requestPath); ok {
			t.Fatalf("unexpected prefix match for %q", requestPath)
		}
	}
	if _, ok := MatchEdgeRequestBodyPolicy(policies, "GET", "/api/fugue/apps/import-upload"); ok {
		t.Fatal("unexpected method match")
	}
}

func TestParseEdgeRequestBodyPoliciesRejectsAmbiguousOrUnsafeMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "unknown field", raw: `[{
			"name":"upload","methods":["POST"],"paths":["/upload"],
			"max_bytes":10,"timeout_seconds":10,"max_concurrent":1,"unexpected":true
		}]`, want: "unknown field"},
		{name: "non normalized path", raw: `[{
			"name":"upload","methods":["POST"],"paths":["/api/../upload"],
			"max_bytes":10,"timeout_seconds":10,"max_concurrent":1
		}]`, want: "normalized exact path"},
		{name: "overlap", raw: `[
			{"name":"one","methods":["POST"],"paths":["/upload"],"max_bytes":10,"timeout_seconds":10,"max_concurrent":1},
			{"name":"two","methods":["POST"],"paths":["/upload"],"max_bytes":20,"timeout_seconds":20,"max_concurrent":2}
		]`, want: "overlap"},
		{name: "unbounded", raw: `[{
			"name":"upload","methods":["POST"],"paths":["/upload"],
			"max_bytes":0,"timeout_seconds":0,"max_concurrent":0
		}]`, want: "max_bytes"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseEdgeRequestBodyPolicies(test.raw)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected error containing %q, got %v", test.want, err)
			}
		})
	}
}
