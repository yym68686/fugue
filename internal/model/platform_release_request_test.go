package model

import (
	"encoding/json"
	"testing"
)

func TestReleaseRequestCannotSilentlyOmitRequestedPrecondition(t *testing.T) {
	for _, raw := range []string{`{"release_channel":"shadow","producer_reconfiguration":null}`, `{"release_channel":"shadow","producer_reconfiguration":{"unknown":true}}`, `{"release_channel":"shadow","unknown":true}`} {
		var request PlatformArtifactReleaseRequest
		if json.Unmarshal([]byte(raw), &request) == nil {
			t.Fatal("invalid precondition accepted", raw)
		}
	}
	var request PlatformArtifactReleaseRequest
	if err := json.Unmarshal([]byte(`{"release_channel":"shadow","idempotency_key":"legacy"}`), &request); err != nil || request.ProducerReconfiguration != nil {
		t.Fatal("legacy request changed", err)
	}
}
