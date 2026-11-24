package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/edgegroupfront"
)

func TestNodeWorkerSealOutputsExactlyTheClaimWireFormat(t *testing.T) {
	_, _, journal := nodeWorkerRuntimeFixture(t)
	want := journal.Digest
	journal.Digest = ""
	unsigned, err := json.MarshalIndent(journal, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runNodeWorkerSeal(nil, bytes.NewReader(unsigned), &output); err != nil {
		t.Fatal(err)
	}
	parsed, err := edgegroupfront.ReadNodeWorkerJournal(output.Bytes())
	if err != nil || parsed.Digest != want {
		t.Fatalf("release job cannot retain the canonical sealed claim: %v %s", err, parsed.Digest)
	}
	for _, malformed := range []string{
		output.String(),
		string(unsigned) + "{}",
		strings.Replace(string(unsigned), "\"node_uid\"", "\"unexpected\"", 1),
		strings.Repeat(" ", (16<<10)+1) + string(unsigned),
	} {
		output.Reset()
		if err := runNodeWorkerSeal(nil, strings.NewReader(malformed), &output); err == nil || output.Len() != 0 {
			t.Fatal("invalid journal produced claim bytes")
		}
	}
}
