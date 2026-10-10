package edgequality

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDeliveryV4ChoosesSustainedFasterDownlinkInSameClientCohort(t *testing.T) {
	snapshot := networkFixture()
	snapshot.Policy.Version = DeliveryNetworkPolicyVersion
	for index := range snapshot.Observations {
		observation := &snapshot.Observations[index]
		observation.ClientNetworkMS, observation.ServiceNetworkMS = number(180), number(10)
		observation.DownloadBPS = number(20000)
		if observation.EdgeID == "edge-b" {
			observation.DownloadBPS = number(1 << 20)
		}
	}
	receipt, err := Capture(snapshot)
	if err != nil || receipt.Result.Hypothesis != "switch" || receipt.Result.ProposedEdgeID != "edge-b" {
		t.Fatal(receipt.Result, err)
	}
	if _, err := Replay(receipt); err != nil {
		t.Fatal(err)
	}
	for index := range snapshot.Observations {
		if snapshot.Observations[index].EdgeID == "edge-b" {
			snapshot.Observations[index].ClientCohort = "tcp_peer:198.51.100.0/24"
		}
	}
	if result := evaluateTest(t, snapshot); result.Hypothesis != "hold" {
		t.Fatal("compared downloads from unrelated networks", result)
	}
}

func TestDeliveryV4RejectsInventedRatesAndPreservesHistoricalSemantics(t *testing.T) {
	now := time.Now().UTC()
	rtt := 180.0
	client := &model.EdgeClientNetworkSample{ConnectionID: "connection-a", Slot: "a", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now.Add(-time.Minute), ObservedAt: now, TCPInfoAvailable: true, RTTMS: &rtt,
		DeliveryBaseline: &model.EdgeClientDeliveryCounters{ObservedAt: now.Add(-time.Second), BytesAcked: 1000, BusyMicroseconds: 100000, DataSegmentsOut: 10},
		Delivery:         &model.EdgeClientDeliveryCounters{ObservedAt: now, BytesAcked: 101000, BusyMicroseconds: 600000, DataSegmentsOut: 110, RetransmittedSegments: 5, DeliveryRateBytesPerSecond: 250000}}
	sample := model.EdgeNetworkSample{Source: "public_front_tcp_info_v1", ClientNetwork: client}
	observation := Observation{ClientSource: "public_tcp_info", ClientNetworkMS: &rtt, ClientCohort: client.Scope, Scope: "global"}
	observation.DownloadBPS, observation.ClientRetransmissionRate = model.EdgeClientDeliveryMetrics(client)
	if !networkWitnessMeasurementMatches(observation, sample, DeliveryNetworkPolicyVersion) {
		t.Fatal("replay lost verified delivery window")
	}
	if networkWitnessMeasurementMatches(observation, sample, BoundedNetworkPolicyVersion) {
		t.Fatal("changed historical V3 measurement authority")
	}
	observation.DownloadBPS = number(999999)
	if networkWitnessMeasurementMatches(observation, sample, DeliveryNetworkPolicyVersion) {
		t.Fatal("accepted download rate without matching raw counters")
	}
	observation.DownloadBPS, observation.ClientRetransmissionRate = nil, nil
	if !networkWitnessMeasurementMatches(observation, sample, BoundedNetworkPolicyVersion) {
		t.Fatal("old policy no longer interprets optional counters as unknown")
	}
	client.Delivery.ApplicationLimited = true
	observation.DownloadBPS, observation.ClientRetransmissionRate = model.EdgeClientDeliveryMetrics(client)
	if observation.DownloadBPS != nil || observation.ClientFailureRate != nil || !networkWitnessMeasurementMatches(observation, sample, DeliveryNetworkPolicyVersion) {
		t.Fatal("application wait became bandwidth or retransmissions became connection failures")
	}
}
