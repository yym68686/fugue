package edgequality

import (
	"encoding/json"
	"testing"
)

func consensusFixture(t *testing.T, secondWinner bool) Snapshot {
	t.Helper()
	receipts := []Receipt{}
	for _, hostname := range []string{"first.example.test", "second.example.test"} {
		snapshot := networkFixture()
		snapshot.Policy.Version = DeliveryNetworkPolicyVersion
		snapshot.Hostname, snapshot.DNSHostname = hostname, "alias.example.test"
		snapshot.ActualDNSReceipt = json.RawMessage(`{"z":1,"a":{"z":2,"a":3}}`)
		for index := range snapshot.Observations {
			observation := &snapshot.Observations[index]
			observation.Hostname = hostname
			observation.ClientNetworkMS, observation.ServiceNetworkMS = number(180), number(10)
			observation.DownloadBPS = number(20000)
			if observation.EdgeID == "edge-b" && (secondWinner || hostname == "first.example.test") {
				observation.DownloadBPS = number(1 << 20)
			}
		}
		receipt, err := Capture(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, receipt)
	}
	root, err := ServiceConsensusSnapshot("alias.example.test", receipts)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestSharedServiceAliasRequiresUnanimousSustainedWinner(t *testing.T) {
	for _, unanimous := range []bool{false, true} {
		snapshot := consensusFixture(t, unanimous)
		receipt, err := Capture(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if unanimous && (receipt.Result.Hypothesis != "switch" || receipt.Result.ProposedEdgeID != "edge-b") || !unanimous && receipt.Result.Hypothesis != "hold" {
			t.Fatal("shared alias changed before all owners approved", receipt.Result)
		}
		if _, err := Replay(receipt); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(receipt)
		var reordered any
		_ = json.Unmarshal(raw, &reordered)
		raw, _ = json.Marshal(reordered)
		var exported Receipt
		_ = json.Unmarshal(raw, &exported)
		if _, err := Replay(exported); err != nil {
			t.Fatal("canonical nested export broke offline replay", err)
		}
	}
}

func TestSharedServiceAliasRejectsRebindingAndNestedOrOmittedEvidence(t *testing.T) {
	snapshot := consensusFixture(t, true)
	for _, edit := range []func(*Snapshot){
		func(value *Snapshot) { value.ServiceReceipts = value.ServiceReceipts[:1] },
		func(value *Snapshot) { value.ServiceReceipts[1] = value.ServiceReceipts[0] },
		func(value *Snapshot) { value.CurrentEdgeID = "edge-b" },
		func(value *Snapshot) { value.Hostname = "foreign.example.test" },
		func(value *Snapshot) { value.Candidates[0].RouteGeneration = "invented" },
		func(value *Snapshot) {
			value.ServiceReceipts[0].Snapshot.ServiceReceipts = []Receipt{value.ServiceReceipts[1]}
		},
		func(value *Snapshot) {
			value.ServiceReceipts[0].Snapshot.ActualDNSReceipt = json.RawMessage(`{"different":true}`)
		},
	} {
		raw, _ := json.Marshal(snapshot)
		var changed Snapshot
		_ = json.Unmarshal(raw, &changed)
		edit(&changed)
		if _, err := Capture(changed); err == nil {
			t.Fatal("shared-service binding was bypassed")
		}
	}
}

func TestSharedServiceConsensusKeepsDistinctPathsAndTrafficClasses(t *testing.T) {
	prior := consensusFixture(t, true)
	receipts := []Receipt{}
	for index, child := range prior.ServiceReceipts {
		snapshot := child.Snapshot
		snapshot.Hostname, snapshot.DNSHostname = "app.example.test", ""
		snapshot.PathPrefix, snapshot.TrafficClass = "/", "dynamic_api"
		if index == 1 {
			snapshot.PathPrefix, snapshot.TrafficClass = "/stream", "streaming"
		}
		for position := range snapshot.Observations {
			snapshot.Observations[position].Hostname, snapshot.Observations[position].TrafficClass = snapshot.Hostname, snapshot.TrafficClass
		}
		receipt, err := Capture(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, receipt)
	}
	root, err := ServiceConsensusSnapshot("app.example.test", receipts)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := Capture(root)
	if err != nil || receipt.Result.Hypothesis != "switch" || receipt.Result.ProposedEdgeID != "edge-b" {
		t.Fatal("complete route consensus did not preserve independent classes", receipt.Result, err)
	}
	if _, err := Replay(receipt); err != nil {
		t.Fatal(err)
	}
	if len(receipt.Result.Candidates[0].Metrics) != len(receipts[0].Result.Candidates[0].Metrics)+len(receipts[1].Result.Candidates[0].Metrics) {
		t.Fatal("same-host path metrics overwrote each other")
	}
}
