package releasedomain

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

const (
	testEvidenceBaseCommit   = "1111111111111111111111111111111111111111"
	testEvidenceTargetCommit = "2222222222222222222222222222222222222222"
)

func TestDecodeAndVerifyChangedFileEvidence(t *testing.T) {
	encoded := makeChangedFileEvidence(t, testEvidenceBaseCommit, testEvidenceTargetCommit, []ChangedFile{{
		Status:           ChangeModified,
		Path:             "synthetic/node-local.go",
		ValuePointers:    []string{"/nodeLocal/enabled"},
		ConsumerDomains:  []Domain{DomainNodeLocal},
		SemanticDomains:  []Domain{DomainNodeLocal},
		OutsideConsumers: []string{"cmd/operator"},
	}})

	evidence, err := DecodeAndVerifyChangedFileEvidence(
		bytes.NewReader(encoded),
		testEvidenceBaseCommit,
		testEvidenceTargetCommit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.BaseCommit() != testEvidenceBaseCommit || evidence.TargetCommit() != testEvidenceTargetCommit {
		t.Fatalf("evidence refs = %s..%s", evidence.BaseCommit(), evidence.TargetCommit())
	}
	if err := validateCanonicalSHA256Digest(evidence.Digest(), "verified digest"); err != nil {
		t.Fatal(err)
	}

	first := evidence.Changes()
	first[0].Path = "mutated"
	first[0].ValuePointers[0] = "/mutated"
	first[0].ConsumerDomains[0] = DomainBackup
	first[0].SemanticDomains[0] = DomainBackup
	first[0].OutsideConsumers[0] = "mutated"
	second := evidence.Changes()
	if second[0].Path != "synthetic/node-local.go" ||
		second[0].ValuePointers[0] != "/nodeLocal/enabled" ||
		second[0].ConsumerDomains[0] != DomainNodeLocal ||
		second[0].SemanticDomains[0] != DomainNodeLocal ||
		second[0].OutsideConsumers[0] != "cmd/operator" {
		t.Fatalf("verified evidence was mutable: %#v", second)
	}
}

func TestDecodeAndVerifyChangedFileEvidenceRejectsUntrustedRefs(t *testing.T) {
	encoded := makeChangedFileEvidence(t, testEvidenceBaseCommit, testEvidenceTargetCommit, []ChangedFile{})
	for _, test := range []struct {
		name   string
		base   string
		target string
	}{
		{name: "abbreviated base", base: strings.Repeat("1", 39), target: testEvidenceTargetCommit},
		{name: "uppercase base", base: strings.Repeat("A", 40), target: testEvidenceTargetCommit},
		{name: "revision expression", base: "HEAD~1", target: testEvidenceTargetCommit},
		{name: "abbreviated target", base: testEvidenceBaseCommit, target: strings.Repeat("2", 39)},
		{name: "wrong base", base: strings.Repeat("3", 40), target: testEvidenceTargetCommit},
		{name: "wrong target", base: testEvidenceBaseCommit, target: strings.Repeat("4", 40)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeAndVerifyChangedFileEvidence(bytes.NewReader(encoded), test.base, test.target); err == nil {
				t.Fatal("untrusted ref unexpectedly succeeded")
			}
		})
	}
}

func TestDecodeAndVerifyChangedFileEvidenceRejectsMalformedDocuments(t *testing.T) {
	valid := makeChangedFileEvidence(t, testEvidenceBaseCommit, testEvidenceTargetCommit, []ChangedFile{{
		Status: ChangeModified,
		Path:   "synthetic/node-local.go",
	}})
	validString := string(valid)
	oversized := bytes.Repeat([]byte{' '}, maxChangedFileEvidenceBytes+1)

	tests := []struct {
		name string
		data []byte
	}{
		{name: "null root", data: []byte("null")},
		{name: "unknown root field", data: []byte(strings.Replace(validString, "{", `{"unknown":true,`, 1))},
		{name: "duplicate root field", data: []byte(strings.Replace(validString, "{", `{"apiVersion":"release-domain.fugue.dev/v1",`, 1))},
		{name: "duplicate nested field", data: []byte(strings.Replace(validString, `"status":"M"`, `"status":"M","status":"M"`, 1))},
		{name: "unknown nested field", data: []byte(strings.Replace(validString, `"status":"M"`, `"unknown":true,"status":"M"`, 1))},
		{name: "null changes", data: []byte(strings.Replace(validString, `"changes":[{"status":"M","path":"synthetic/node-local.go"}]`, `"changes":null`, 1))},
		{name: "null change", data: []byte(strings.Replace(validString, `"changes":[{"status":"M","path":"synthetic/node-local.go"}]`, `"changes":[null]`, 1))},
		{name: "null optional array", data: []byte(strings.Replace(validString, `"path":"synthetic/node-local.go"`, `"path":"synthetic/node-local.go","consumerDomains":null`, 1))},
		{name: "missing required field", data: []byte(strings.Replace(validString, `"kind":"ChangedFileEvidence",`, "", 1))},
		{name: "trailing value", data: append(append([]byte(nil), valid...), []byte("{}")...)},
		{name: "invalid raw UTF-8", data: append(append([]byte(nil), valid[:len(valid)-1]...), []byte{0xff, '}'}...)},
		{name: "isolated surrogate", data: []byte(strings.Replace(validString, "synthetic/node-local.go", `synthetic/\ud800.go`, 1))},
		{name: "oversized", data: oversized},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeAndVerifyChangedFileEvidence(
				bytes.NewReader(test.data),
				testEvidenceBaseCommit,
				testEvidenceTargetCommit,
			); err == nil {
				t.Fatal("malformed evidence unexpectedly succeeded")
			}
		})
	}
}

func TestDecodeAndVerifyChangedFileEvidenceRejectsIdentityStatusAndDigestDrift(t *testing.T) {
	baseDocument := changedFileEvidenceDocument{
		APIVersion:   ChangedFileEvidenceAPIVersion,
		Kind:         ChangedFileEvidenceKind,
		Policy:       ChangedFileEvidencePolicy,
		BaseCommit:   testEvidenceBaseCommit,
		TargetCommit: testEvidenceTargetCommit,
		Changes:      []ChangedFile{{Status: ChangeModified, Path: "synthetic/node-local.go"}},
	}

	tests := []struct {
		name   string
		mutate func(*changedFileEvidenceDocument)
	}{
		{name: "api version", mutate: func(document *changedFileEvidenceDocument) { document.APIVersion = "release-domain.fugue.dev/v2" }},
		{name: "kind", mutate: func(document *changedFileEvidenceDocument) { document.Kind = "OtherEvidence" }},
		{name: "policy", mutate: func(document *changedFileEvidenceDocument) { document.Policy = "working-tree-v1" }},
		{name: "embedded base format", mutate: func(document *changedFileEvidenceDocument) { document.BaseCommit = strings.Repeat("A", 40) }},
		{name: "embedded target format", mutate: func(document *changedFileEvidenceDocument) { document.TargetCommit = strings.Repeat("2", 64) }},
		{name: "status", mutate: func(document *changedFileEvidenceDocument) { document.Changes[0].Status = "R" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := baseDocument
			document.Changes = cloneChangedFiles(baseDocument.Changes)
			test.mutate(&document)
			encoded := marshalChangedFileEvidenceDocument(t, document, true)
			if _, err := DecodeAndVerifyChangedFileEvidence(bytes.NewReader(encoded), testEvidenceBaseCommit, testEvidenceTargetCommit); err == nil {
				t.Fatal("drifted evidence unexpectedly succeeded")
			}
		})
	}

	t.Run("mismatched digest", func(t *testing.T) {
		document := baseDocument
		document.Digest = "sha256:" + strings.Repeat("0", sha256.Size*2)
		encoded, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeAndVerifyChangedFileEvidence(bytes.NewReader(encoded), testEvidenceBaseCommit, testEvidenceTargetCommit); err == nil {
			t.Fatal("digest mismatch unexpectedly succeeded")
		}
	})

	t.Run("noncanonical digest spelling", func(t *testing.T) {
		document := baseDocument
		encoded := marshalChangedFileEvidenceDocument(t, document, true)
		encoded = bytes.Replace(encoded, []byte(`"digest":"sha256:`), []byte(`"digest":"SHA256:`), 1)
		if _, err := DecodeAndVerifyChangedFileEvidence(bytes.NewReader(encoded), testEvidenceBaseCommit, testEvidenceTargetCommit); err == nil {
			t.Fatal("uppercase digest unexpectedly succeeded")
		}
	})
}

func makeChangedFileEvidence(t *testing.T, baseCommit, targetCommit string, changes []ChangedFile) []byte {
	t.Helper()
	return marshalChangedFileEvidenceDocument(t, changedFileEvidenceDocument{
		APIVersion:   ChangedFileEvidenceAPIVersion,
		Kind:         ChangedFileEvidenceKind,
		Policy:       ChangedFileEvidencePolicy,
		BaseCommit:   baseCommit,
		TargetCommit: targetCommit,
		Changes:      changes,
	}, true)
}

func marshalChangedFileEvidenceDocument(t *testing.T, document changedFileEvidenceDocument, recomputeDigest bool) []byte {
	t.Helper()
	if recomputeDigest {
		payload := changedFileEvidencePayload{
			APIVersion:   document.APIVersion,
			Kind:         document.Kind,
			Policy:       document.Policy,
			BaseCommit:   document.BaseCommit,
			TargetCommit: document.TargetCommit,
			Changes:      document.Changes,
		}
		encodedPayload, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(encodedPayload)
		document.Digest = fmt.Sprintf("sha256:%x", digest[:])
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
