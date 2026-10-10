package frontnetwork

import (
	"encoding/json"
	"testing"
)

func TestDeliveryCountersRequireCompleteNewFrontEvidence(t *testing.T) {
	query, connections, now := liveFixture()
	for key, value := range map[string]string{"tcp_bytes_acked": "100000", "tcp_busy_time_us": "1000000", "tcp_data_segs_out": "100", "tcp_delivery_rate_bps": "90000", "tcp_delivery_rate_app_limited": "false", "tcp_last_data_sent_ms": "5"} {
		connections.Active[0].TCPInfo[key] = json.RawMessage(value)
	}
	sample, err := SampleFromLiveConnections(query, connections, now)
	if err != nil || sample.Delivery == nil || sample.Delivery.BytesAcked != 100000 || sample.Delivery.ApplicationLimited || sample.Delivery.DeliveryRateBytesPerSecond != 90000 {
		t.Fatal(sample, err)
	}
	connections.Active[0].TCPInfo["tcp_busy_time_us"] = json.RawMessage(`"invalid"`)
	if _, err := SampleFromLiveConnections(query, connections, now); err == nil {
		t.Fatal("accepted malformed kernel evidence")
	}
	delete(connections.Active[0].TCPInfo, "tcp_busy_time_us")
	sample, err = SampleFromLiveConnections(query, connections, now)
	if err != nil || sample.Delivery != nil || sample.RTTMS == nil {
		t.Fatal("old Front compatibility lost or missing counter invented", sample, err)
	}
}
