package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestArtifactSecretScanAcceptsOnlyTypedBusinessDNSAnchor(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		name    string
		anchor  string
		allowed bool
	}{
		{"positive sequence", `{"digest":"` + digest + `","fencing_token":3}`, true},
		{"explicit baseline", `{"digest":"` + digest + `","fencing_token":3,"baseline_edge_id":"edge-a"}`, true},
		{"credential string", `{"digest":"` + digest + `","fencing_token":"credential-value"}`, false},
		{"numeric string", `{"digest":"` + digest + `","fencing_token":"3"}`, false},
		{"zero", `{"digest":"` + digest + `","fencing_token":0}`, false},
		{"negative", `{"digest":"` + digest + `","fencing_token":-1}`, false},
		{"fractional", `{"digest":"` + digest + `","fencing_token":1.5}`, false},
		{"overflow", `{"digest":"` + digest + `","fencing_token":9223372036854775808}`, false},
		{"lossy large integer", `{"digest":"` + digest + `","fencing_token":9007199254740993}`, false},
		{"null", `{"digest":"` + digest + `","fencing_token":null}`, false},
		{"missing digest", `{"fencing_token":3}`, false},
		{"malformed digest", `{"digest":"sha256:invalid","fencing_token":3}`, false},
		{"unknown field", `{"digest":"` + digest + `","fencing_token":3,"extra":true}`, false},
		{"secret sibling", `{"digest":"` + digest + `","fencing_token":3,"api_key":"credential-value"}`, false},
		{"empty baseline", `{"digest":"` + digest + `","fencing_token":3,"baseline_edge_id":""}`, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content := json.RawMessage(`{"dns_query_policy":{"physical_routes":[{"business_exploration":{"initial_dns_publications":{"dns-a":` + test.anchor + `}}}]}}`)
			found := firstSecretLikeContentPath(content, "")
			if (found == "") != test.allowed {
				t.Fatalf("allowed=%v path=%q", test.allowed, found)
			}
		})
	}
	for _, content := range []string{
		`{"not_initial_dns_publications":{"dns-a":{"digest":"` + digest + `","fencing_token":3}}}`,
		`{"unrelated":{"initial_dns_publications":{"dns-a":{"digest":"` + digest + `","fencing_token":3}}}}`,
		`{"fencing_token":3}`,
		`{"unrelated":{"digest":"` + digest + `","fencing_token":3}}`,
		`{"initial_dns_publications":{"dns-a":{"nested":{"digest":"` + digest + `","fencing_token":3}}}}`,
		`{"initial_dns_publications":{"dns-a":{"digest":"` + digest + `","access_token":3}}}`,
	} {
		if firstSecretLikeContentPath(json.RawMessage(content), "") == "" {
			t.Fatal("untyped secret-like field passed")
		}
	}
	if firstSecretLikeContentPath(json.RawMessage(`{"business_exploration":{"initial_dns_publications":{"dns.a":{"digest":"`+digest+`","fencing_token":3}}}}`), "") != "" {
		t.Fatal("valid DNS node identity rejected")
	}
	if firstSecretLikeContentPath(json.RawMessage(`{"business_allocation":{"policy":{"version":"business-edge-exploration-v1","initial_dns_publications":{"dns-a":{"digest":"`+digest+`","fencing_token":3}}}}}`), "") != "" {
		t.Fatal("signed allocation policy rejected")
	}
}
