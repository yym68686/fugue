package api

import (
	"errors"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
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
			if ready := projectHostConsumersReady(status, platformconfig.DNSManagedHostContinuityCapabilityV1); ready != (name == "both_ready") {
				t.Fatalf("ready=%t for %s", ready, name)
			}
			if projectHostConsumersReady(status, platformcontrol.SharedCustomHostTLSCapabilityV1) {
				t.Fatal("DNS continuity cannot substitute for the Worker TLS capability")
			}
			fact.CompatibilityCapabilities = append(fact.CompatibilityCapabilities, platformcontrol.SharedCustomHostTLSCapabilityV1)
			if ready := projectHostConsumersReady(status, platformcontrol.SharedCustomHostTLSCapabilityV1); ready != (name == "both_ready") {
				t.Fatalf("worker ready=%t for %s", ready, name)
			}
		})
	}
}

func TestManagedProjectHostPreflightBlocksUnreadyWorkersOnlyForSharedDestination(t *testing.T) {
	single := map[string]string{"/": "api"}
	shared := map[string]string{"/": "web", "/api": "api"}
	for _, test := range []struct {
		name          string
		before, after map[string]string
		dns, worker   error
		want          error
	}{
		{"enable with old Worker", single, shared, nil, errProjectHostWorkerTLS, errProjectHostWorkerTLS},
		{"keep with old Worker", shared, shared, nil, errProjectHostWorkerTLS, errProjectHostWorkerTLS},
		{"revert without Worker", shared, single, nil, errProjectHostWorkerTLS, nil},
		{"revert without DNS continuity", shared, single, errProjectHostDNSContinuity, nil, errProjectHostDNSContinuity},
		{"retain one application", single, single, errProjectHostDNSContinuity, errProjectHostWorkerTLS, nil},
		{"enable with both ready", single, shared, nil, nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := model.ProjectHostRoutePlan{Before: test.before, After: test.after, HostnameOwnerAppID: "api"}
			if err := projectHostManagedPreflight(plan, test.dns, test.worker); !errors.Is(err, test.want) {
				t.Fatalf("preflight returned %v, want %v", err, test.want)
			}
		})
	}
}
