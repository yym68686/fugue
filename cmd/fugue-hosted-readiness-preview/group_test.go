package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestGroupPreviewCannotTurnRedactedPolicyOrMissingIngressIntoProof(t *testing.T) {
	for _, input := range []string{`{"Host":"service.example.test","Before":{"policies":[{"id":"cached","bypass_on_authorization":"redacted"}]}}`,
		`{"Host":"service.example.test","Physical":[]}`,
		`{"Host":"service.example.test","Physical":[{"EdgeID":"edge-a","EdgeGroupID":"group","Address":"192.0.2.1"}],"Eligible":[{"EdgeID":"edge-b","EdgeGroupID":"group","Address":"192.0.2.2"}]}`,
		`{} {}`,
	} {
		var out bytes.Buffer
		if err := groupPreview(strings.NewReader(input), &out); err == nil || out.Len() != 0 {
			t.Fatal("published a result with incomplete diagnostic data")
		}
	}
}
