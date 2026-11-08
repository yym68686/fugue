package main

import (
	"strings"
	"testing"

	"fugue/internal/store"
)

func TestSourceVerificationContractRejectsUnrelatedAuthority(t *testing.T) {
	valid := `{"schema":"` + store.CompilerInputSourceVerificationSchema + `","artifact_id":"neutral-parent","expected_content_hash":"sha256:` + strings.Repeat("a", 64) + `"}`
	if _, err := decodeSourceVerificationContract(strings.NewReader(valid)); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{valid + ` {}`, strings.TrimSuffix(valid, "}") + `,"key":"caller-key"}`, strings.TrimSuffix(valid, "}") + `,"apply":true}`, strings.Repeat(" ", 16<<10) + valid, strings.Replace(valid, "neutral-parent", " ", 1)} {
		if _, err := decodeSourceVerificationContract(strings.NewReader(value)); err == nil {
			t.Fatal("unrelated authority or malformed contract accepted")
		}
	}
}

func TestSourceVerificationFlagsRejectMutationAndUnrelatedModes(t *testing.T) {
	if !validSourceVerificationFlags(map[string]bool{"mode": true, "source-verification-contract": true}) {
		t.Fatal("exact read-only selection rejected")
	}
	for _, name := range []string{"apply", "rehearse", "digest", "task-input-contract", "local-pin-key", "dependency-budget"} {
		if validSourceVerificationFlags(map[string]bool{"mode": true, "source-verification-contract": true, name: true}) {
			t.Fatal("unrelated flag accepted", name)
		}
	}
	if validSourceVerificationFlags(map[string]bool{"mode": true}) || validSourceVerificationFlags(map[string]bool{"source-verification-contract": true}) {
		t.Fatal("incomplete selection accepted")
	}
}
