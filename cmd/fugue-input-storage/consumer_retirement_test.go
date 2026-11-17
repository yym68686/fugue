package main

import (
	"strings"
	"testing"
)

func TestConsumerRetirementContractDecoderIsStrictAndBounded(t *testing.T) {
	good := `{"version":1,"actor":"neutral-operator","reason":"explicit retirement","permanently_abandon_recovery":true,"targets":[{"consumer_id":"edge-worker:neutral","artifact_kind":"edge_route_bundle","scope_key":"global"}]}`
	if _, err := decodeConsumerRetirementContract(strings.NewReader(good)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{good + `{}`, strings.Replace(good, `"version":1`, `"version":1,"force":true`, 1), strings.Replace(good, `:true`, `:false`, 1), strings.Repeat(" ", 65537) + good} {
		if _, err := decodeConsumerRetirementContract(strings.NewReader(raw)); err == nil {
			t.Fatal("invalid retirement contract accepted")
		}
	}
}
