package dnsserver

import (
	"errors"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func validateServingDNSSelectors(views []platformconfig.DNSQueryView) error {
	for _, view := range views {
		for _, record := range view.Records {
			if len(record.Candidates) == 0 {
				continue
			}
			switch record.AnswerPolicy.PolicyKind {
			case model.DNSAnswerPolicyKindPhysicalQuality, model.DNSAnswerPolicyKindPhysicalOrder:
			default:
				return errors.New("legacy DNS selector retired; signed physical order required")
			}
		}
	}
	return nil
}
