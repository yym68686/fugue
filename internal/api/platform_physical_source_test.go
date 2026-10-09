package api

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPhysicalSelectionChangeTriggersProducerWithoutEvidenceOnlyChurn(t *testing.T) {
	projection, policy, selections, now := physicalQueryProjectionFixture(t)
	if err := applyPhysicalDNSSelections(&projection, policy, selections, now); err != nil {
		t.Fatal(err)
	}
	before, err := platformProducerSourceDigest(projection, "authority-a")
	if err != nil {
		t.Fatal(err)
	}
	clone := func() platformIntentProjectionResponse {
		raw, _ := json.Marshal(projection)
		var copied platformIntentProjectionResponse
		if err := json.Unmarshal(raw, &copied); err != nil {
			t.Fatal(err)
		}
		return copied
	}
	changed := clone()
	selection := changed.RuntimeSnapshot.DNSSelections[0].PhysicalSelection
	selection.PrimaryEdgeID, selection.OrderedEdgeIDs = "edge-a", []string{"edge-a", "edge-b"}
	after, err := platformProducerSourceDigest(changed, "authority-a")
	if err != nil || before == after {
		t.Fatal("new physical primary does not trigger a producer publication", err)
	}
	refreshed := clone()
	selection = refreshed.RuntimeSnapshot.DNSSelections[0].PhysicalSelection
	selection.CapturedAt = now.Add(time.Minute)
	selection.DNSReceiptID = "new-real-answer"
	refreshed.RuntimeSnapshot.DNSSelections[0].PhysicalEvidence = json.RawMessage(`{"new":"network-observations"}`)
	after, err = platformProducerSourceDigest(refreshed, "authority-a")
	if err != nil || before != after {
		t.Fatal("fresh evidence with unchanged order causes producer churn", err)
	}
}
