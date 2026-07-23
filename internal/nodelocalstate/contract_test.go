package nodelocalstate

import (
	"encoding/json"
	"strings"
	"testing"
)

const testWorkloadContractDigest = "sha256:5d5696c423b25f1931a4eb1bc1274ae066c80b2979a20d4ff47ac13acf835d0c"

func TestBootstrapNoopContractCanonicalizesAndDigests(t *testing.T) {
	var schema map[string]any
	if err := json.Unmarshal([]byte(SchemaJSON), &schema); err != nil || schema["$schema"] == nil {
		t.Fatalf("embedded JSON schema is invalid: %v", err)
	}
	document := testDocument("nodelocal-0001", nil)
	if err := Validate(document); err != nil {
		t.Fatalf("validate bootstrap: %v", err)
	}
	if !IsBootstrapNoop(document) {
		t.Fatal("expected bootstrap no-op")
	}
	left, err := Digest(document)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	raw, err := CanonicalJSON(document)
	if err != nil {
		t.Fatalf("canonical JSON: %v", err)
	}
	var content map[string]any
	if err := json.Unmarshal(raw, &content); err != nil {
		t.Fatalf("decode canonical JSON: %v", err)
	}
	decoded, err := DecodeForArtifact(content, document.Generation, ScopeKey, "v1")
	if err != nil {
		t.Fatalf("decode artifact: %v", err)
	}
	right, err := Digest(decoded)
	if err != nil {
		t.Fatalf("redigest: %v", err)
	}
	if left != right || !strings.HasPrefix(left, "sha256:") || len(left) != 71 {
		t.Fatalf("digest drift: %q / %q", left, right)
	}
}

func TestContractRejectsDuplicateOverlapAndInvalidModes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Document)
	}{
		{name: "duplicate active", mutate: func(document *Document) {
			document.Desired.ActiveShadow = append(document.Desired.ActiveShadow, document.Desired.ActiveShadow[0])
		}},
		{name: "overlap", mutate: func(document *Document) {
			document.Desired.PreservedIptables = []NodeIdentity{document.Desired.ActiveShadow[0]}
		}},
		{name: "fugue id overlap", mutate: func(document *Document) {
			document.Desired.PreservedIptables[0].FugueNodeID = document.Desired.ActiveShadow[0].FugueNodeID
		}},
		{name: "kubernetes uid overlap", mutate: func(document *Document) {
			document.Desired.PreservedIptables[0].ExpectedKubernetesUID = document.Desired.ActiveShadow[0].ExpectedKubernetesUID
		}},
		{name: "missing active snapshot", mutate: func(document *Document) { document.Desired.ActiveShadow = nil }},
		{name: "missing preserved snapshot", mutate: func(document *Document) { document.Desired.PreservedIptables = nil }},
		{name: "active iptables", mutate: func(document *Document) { document.Desired.ActiveMode = "iptables" }},
		{name: "preserved shadow", mutate: func(document *Document) { document.Desired.PreservedMode = "shadow" }},
		{name: "unknown enforcement", mutate: func(document *Document) { document.Enforcement.Mode = "automatic" }},
		{name: "unknown transition", mutate: func(document *Document) { document.Transition.Type = "add_many" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := testDocument("nodelocal-0001", nil)
			test.mutate(&document)
			if err := Validate(document); err == nil {
				t.Fatal("invalid contract unexpectedly passed")
			}
		})
	}
}

func TestContractRejectsWhitespaceGeneration(t *testing.T) {
	document := testDocument(" nodelocal-0001 ", nil)
	if err := Validate(document); err == nil {
		t.Fatal("generation with surrounding whitespace unexpectedly passed")
	}
}

func TestTransitionAllowsOnlyNoopOrOneShadowAddition(t *testing.T) {
	base := testDocument("nodelocal-0001", nil)
	target := testDocument("nodelocal-0002", &base)
	added := testNode("runtime-c", "node-c", "33333333-3333-3333-3333-333333333333")
	target.Desired.ActiveShadow = append(target.Desired.ActiveShadow, added)
	target.Transition = Transition{Type: TransitionAddOneShadow, Node: &added}
	decision := ClassifyTransition(base, target)
	if !decision.Allowed || decision.Type != TransitionAddOneShadow || len(decision.Added) != 1 || decision.Added[0] != added {
		t.Fatalf("one-add decision = %+v", decision)
	}
	if err := ValidateTransition(&base, target, base.Generation); err != nil {
		t.Fatalf("validate one-add: %v", err)
	}
	if err := ValidateBaseTargetLive(base, target, base.Generation, base.Generation); err != nil {
		t.Fatalf("validate base/target/live: %v", err)
	}

	replaced := target
	replaced.Generation = "nodelocal-0003"
	replaced.BaseGeneration = target.Generation
	replaced.PreviousGeneration = target.Generation
	replaced.RollbackGeneration = base.Generation
	replaced.Desired.ActiveShadow[0].ExpectedKubernetesUID = "44444444-4444-4444-4444-444444444444"
	replaced.Transition.Node = &replaced.Desired.ActiveShadow[0]
	if decision := ClassifyTransition(target, replaced); decision.Allowed {
		t.Fatalf("identity replacement unexpectedly allowed: %+v", decision)
	}
}

func TestTransitionSeparatesMembershipFromEnforcement(t *testing.T) {
	base := testDocument("nodelocal-0001", nil)
	target := testDocument("nodelocal-0002", &base)
	added := testNode("runtime-c", "node-c", "33333333-3333-3333-3333-333333333333")
	target.Desired.ActiveShadow = append(target.Desired.ActiveShadow, added)
	target.Transition = Transition{Type: TransitionAddOneShadow, Node: &added}
	target.Enforcement.Mode = EnforcementEnabled
	if decision := ClassifyTransition(base, target); decision.Allowed {
		t.Fatalf("membership plus enforcement unexpectedly allowed: %+v", decision)
	}

	target = testDocument("nodelocal-0002", &base)
	target.Enforcement.Mode = EnforcementEnabled
	target.Transition.Type = TransitionEnforcementUpdate
	if decision := ClassifyTransition(base, target); !decision.Allowed || decision.Type != TransitionEnforcementUpdate {
		t.Fatalf("separate enforcement update rejected: %+v", decision)
	}
	target.Enforcement.Mode = EnforcementReportOnly
	base.Enforcement.Mode = EnforcementEnabled
	if decision := ClassifyTransition(base, target); decision.Allowed {
		t.Fatalf("forward enforcement regression unexpectedly allowed: %+v", decision)
	}
}

func TestTransitionRejectsStaleBaseAndRollback(t *testing.T) {
	base := testDocument("nodelocal-0001", nil)
	target := testDocument("nodelocal-0002", &base)
	target.BaseGeneration = "nodelocal-0000"
	if decision := ClassifyTransition(base, target); decision.Allowed {
		t.Fatalf("stale base unexpectedly allowed: %+v", decision)
	}
	target.BaseGeneration = base.Generation
	target.RollbackGeneration = "nodelocal-0000"
	if err := ValidateTransition(&base, target, base.Generation); err == nil {
		t.Fatal("stale rollback unexpectedly allowed")
	}
	if err := ValidateBaseTargetLive(base, target, "nodelocal-0099", base.Generation); err == nil {
		t.Fatal("live generation mismatch unexpectedly allowed")
	}
}

func testDocument(generation string, base *Document) Document {
	document := Document{
		ContractKind:       ContractKindMembership,
		ContractVersion:    ContractVersionV1,
		Generation:         generation,
		BaseGeneration:     generation,
		PreviousGeneration: generation,
		RollbackGeneration: generation,
		Compatibility: Compatibility{
			ControllerFloor:        ControllerCapabilityV1,
			WorkloadContractDigest: testWorkloadContractDigest,
		},
		Transition: Transition{Type: TransitionNoop},
		Desired: DesiredState{
			ActiveMode:        ActiveModeShadow,
			PreservedMode:     PreservedModeIptables,
			ActiveShadow:      []NodeIdentity{testNode("runtime-a", "node-a", "11111111-1111-1111-1111-111111111111"), testNode("runtime-b", "node-b", "22222222-2222-2222-2222-222222222222")},
			PreservedIptables: []NodeIdentity{testNode("runtime-d", "node-d", "dddddddd-dddd-dddd-dddd-dddddddddddd")},
		},
		Enforcement: Enforcement{Mode: EnforcementReportOnly},
		Audit:       AuditIntent{ActorType: "user", ActorID: "admin-1", Reason: "bootstrap current NodeLocal membership", ChangeID: "change-0001"},
	}
	if base != nil {
		document.BaseGeneration = base.Generation
		document.PreviousGeneration = base.Generation
		document.RollbackGeneration = base.Generation
		document.Desired = base.Desired
		document.Compatibility = base.Compatibility
	}
	return document
}

func testNode(id, name, uid string) NodeIdentity {
	return NodeIdentity{FugueNodeID: id, KubernetesName: name, ExpectedKubernetesUID: uid}
}
