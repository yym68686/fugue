package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"fugue/internal/livediagnostics"
)

func TestReportTokenWrappingPreservesLargeStructuredEvidence(t *testing.T) {
	rows := make([]map[string]any, 24000)
	for i := range rows {
		rows[i] = map[string]any{"value": json.Number("9007199254740993"), "escaped": "quoted\" slash\\ braces{},[]", "index": i}
	}
	data, _ := json.Marshal(rows)
	r := livediagnostics.ProbeReport{Evidence: []livediagnostics.Evidence{{Name: "sample", Data: data}}}
	var result bytes.Buffer
	if err := writeReport(&result, r); err != nil {
		t.Fatal(err)
	}
	if result.Len() <= 1<<20 {
		t.Fatal("fixture does not exercise long reports")
	}
	for _, line := range bytes.Split(result.Bytes(), []byte{'\n'}) {
		if len(line) > 33<<10 {
			t.Fatalf("report line exceeds bound: %d", len(line))
		}
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, result.Bytes()); err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(r)
	if !bytes.Equal(compact.Bytes(), want) {
		t.Fatal("line wrapping changed report values")
	}
}
