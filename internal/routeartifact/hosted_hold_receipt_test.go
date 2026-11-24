package routeartifact

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
)

func TestScopedDNSReceiptRejectsAWithheldAddressInTheSignedRR(t *testing.T) {
	const name = "target.example.test"
	rr := model.EdgeDNSRecord{Name: name, Type: "A", TTL: 60, Values: []string{"8.8.8.8", "9.9.9.9"}}
	u := &pc.HostedDNSContent{Views: []pc.DNSQueryView{{Records: []model.EdgeDNSRecord{rr}}}, Withheld: []pc.HostedAddressHold{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", Address: "8.8.8.8"}}}
	p := HostedSelectedPacket{Delivery: HostedDelivery{Schema: "fugue.hosted-recipient-input/v1", Host: "client.example.org", Member: "dns-a", MembershipSet: "set", MembershipRelease: "release", MembershipRevision: 1, MembershipFence: 1, Parts: []HostedDeliveryPart{{Recipient: HostedRecipient{Kind: model.PlatformArtifactKindDNSAnswerBundle}, Content: HostedRecipientInput{DNS: u}}}}}
	req, e := hostedExpectedSamples(p)
	if e != nil || len(req) != 2 {
		t.Fatal("missing UDP/TCP requirements", e)
	}
	at := time.Now()
	for _, r := range req {
		s := HostedSample{Name: name, Family: "A", ObservedAt: at, ValidUntil: at.Add(time.Second), RRSet: []string{name + ". 60 IN A 9.9.9.9"}}
		if !r.matches(s, "") {
			t.Fatal("eligible answer rejected")
		}
		s.RRSet = append(s.RRSet, name+". 60 IN A 8.8.8.8")
		if r.matches(s, "") {
			t.Fatal("held address accepted as successful execution")
		}
	}
	if !reflect.DeepEqual(u.Views[0].Records[0].Values, []string{"8.8.8.8", "9.9.9.9"}) {
		t.Fatal("receipt validation rewrote declared RR")
	}
}
