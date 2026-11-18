package dnsserver

import (
	"encoding/json"

	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

// DecisionECSCoverage describes a replay-verifiable receipt, not a serving
// authorization. Receipts redact the client IP, so an ASN hint is only an
// upper bound on the number of prefixes that can safely be classified.
func DecisionECSCoverage(receipt DNSDecisionReceipt) (ecs, asnHint, valid bool) {
	if !receipt.WriteSucceeded || receipt.RCode != dns.RcodeSuccess || len(receipt.RRSet) == 0 ||
		(receipt.QType != dns.TypeA && receipt.QType != dns.TypeAAAA) || receipt.AnswerPublication == nil {
		return false, false, false
	}
	if _, err := ReplayDNSDecision(receipt); err != nil {
		return false, false, false
	}
	var input dnsDecisionReplay
	var query dns.Msg
	if json.Unmarshal(receipt.ReplayInput, &input) != nil || query.Unpack(input.Query) != nil {
		return false, false, false
	}
	if opt := query.IsEdns0(); opt != nil {
		count := 0
		for _, option := range opt.Option {
			if subnet, ok := option.(*dns.EDNS0_SUBNET); ok && subnet != nil {
				count++
				ecs = subnet.SourceScope == 0 && subnet.SourceNetmask > 0 &&
					(subnet.Family == 1 && subnet.SourceNetmask <= 32 || subnet.Family == 2 && subnet.SourceNetmask <= 128)
			}
		}
		ecs = ecs && count == 1
	}
	for _, stage := range input.Stages {
		if stage != nil && stage.Publication == *receipt.AnswerPublication {
			hint := stage.Hint
			return ecs, ecs && hint.Source == "ecs" && platformconfig.ClientASN(hint.ASN) != 0, true
		}
	}
	return false, false, false
}
