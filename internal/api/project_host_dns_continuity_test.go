package api

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestProjectHostDNSContinuityRequiresAllLiveConsumers(t *testing.T) {
	for _, name := range []string{"both_ready", "old_binary", "stale", "no_receipt", "unexpected", "missing"} {
		t.Run(name, func(t *testing.T) {
			fact := &model.PlatformConsumerInstance{CompatibilityCapabilities: []string{platformconfig.DNSManagedHostContinuityCapabilityV1}}
			status := model.PlatformConsumerConvergenceStatus{Pass: true, RequiredExpected: 2, RequiredPassing: 2, Assessments: []model.PlatformConsumerEvidenceAssessment{
				{Required: true, State: model.InvariantEvidenceStatePass, Observed: fact},
				{Required: true, State: model.InvariantEvidenceStatePass, Observed: fact},
			}}
			switch name {
			case "old_binary":
				status.Assessments[1].Observed = &model.PlatformConsumerInstance{}
			case "stale":
				status.Assessments[1].State = model.InvariantEvidenceStateUnknown
			case "no_receipt":
				status.Assessments[1].Observed = nil
			case "unexpected":
				status.UnexpectedConsumers = []string{"obsolete-dns"}
			case "missing":
				status.Assessments = status.Assessments[:1]
			}
			if ready := projectHostDNSConsumersReady(status); ready != (name == "both_ready") {
				t.Fatalf("ready=%t for %s", ready, name)
			}
		})
	}
}
