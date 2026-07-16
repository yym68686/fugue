package releasedomain

import (
	"bytes"
	"strings"
	"testing"
)

const testPlanArtifactOwnership = `apiVersion: release-domain.fugue.dev/v1
kind: ReleaseDomainOwnership
domains:
  - node-local
  - authoritative-dns
  - control-plane
  - image-cache
  - backup
requiredBindings:
  - releaseName
  - releaseNamespace
fileRules:
  - id: synthetic-node-local
    exact: synthetic/node-local.go
    domains: [node-local]
valueRules: []
objectRules:
  - id: synthetic-node-local-config
    domain: node-local
    apiGroup: ""
    version: v1
    kind: ConfigMap
    scope: Namespaced
    namespace: '${releaseNamespace}'
    name: node-local
`

func TestBuildPlanFromArtifactsBuildsRevisionBoundSingleDomainPlan(t *testing.T) {
	input, evidenceDigest := validPlanArtifactInput(t)
	plan, err := BuildPlanFromArtifacts(input)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Result != OutcomeSingle || plan.SelectedDomain != DomainNodeLocal {
		t.Fatalf("plan = %#v", plan)
	}
	if err := VerifyPlanDigest(plan); err != nil {
		t.Fatal(err)
	}
	if plan.Digests.ChangedFiles != evidenceDigest {
		t.Fatalf("changed-file digest = %q, want canonical payload digest %q", plan.Digests.ChangedFiles, evidenceDigest)
	}
	if plan.Digests.Ownership != digestBytesSHA256(input.Ownership) {
		t.Fatalf("ownership digest = %q", plan.Digests.Ownership)
	}
	if plan.Digests.BaseManifest != digestBytesSHA256(input.BaseCanonicalManifest) ||
		plan.Digests.TargetManifest != digestBytesSHA256(input.TargetCanonicalManifest) ||
		plan.Digests.RepeatedTargetManifest != digestBytesSHA256(input.RepeatedCanonicalManifest) {
		t.Fatalf("manifest digests = %#v", plan.Digests)
	}
	if plan.Digests.ClassificationContext.DefaultNamespace != "fugue-system" ||
		plan.Digests.ClassificationContext.BindingMap()["releaseName"] != "fugue" ||
		plan.Digests.ClassificationContext.BindingMap()["releaseNamespace"] != "fugue-system" {
		t.Fatalf("classification context = %#v", plan.Digests.ClassificationContext)
	}
}

func TestBuildPlanFromArtifactsLetsBuildPlanFailClosedOnRepeatedRenderDrift(t *testing.T) {
	input, _ := validPlanArtifactInput(t)
	spec, err := LoadOwnership(bytes.NewReader(input.Ownership))
	if err != nil {
		t.Fatal(err)
	}
	input.RepeatedCanonicalManifest = canonicalPlanArtifactManifest(t, spec, "repeat-drift")

	plan, err := BuildPlanFromArtifacts(input)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Result != OutcomeUnknown || plan.SingleDomainDispatchAllowed() {
		t.Fatalf("render drift plan = %#v", plan)
	}
	found := false
	for _, item := range plan.Unknown {
		if item.Source == "planner" && item.Subject == "target-render" && strings.Contains(item.Reason, "repeated target render digest differs") {
			found = true
		}
	}
	if !found {
		t.Fatalf("render drift was not recorded by BuildPlan: %#v", plan.Unknown)
	}
}

func TestBuildPlanFromArtifactsRejectsInvalidArtifactBindings(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*PlanArtifactInput)
	}{
		{name: "untrusted evidence target", mutate: func(input *PlanArtifactInput) { input.TrustedTargetCommit = strings.Repeat("3", 40) }},
		{name: "abbreviated trusted base", mutate: func(input *PlanArtifactInput) { input.TrustedBaseCommit = strings.Repeat("1", 12) }},
		{name: "namespace mismatch", mutate: func(input *PlanArtifactInput) { input.Bindings["releaseNamespace"] = "other" }},
		{name: "noncanonical base manifest", mutate: func(input *PlanArtifactInput) {
			input.BaseCanonicalManifest = bytes.Replace(input.BaseCanonicalManifest, []byte("apiVersion: v1\n"), []byte("# not canonical\napiVersion: v1\n"), 1)
		}},
		{name: "invalid ownership", mutate: func(input *PlanArtifactInput) { input.Ownership = []byte("null\n") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			input, _ := validPlanArtifactInput(t)
			test.mutate(&input)
			if _, err := BuildPlanFromArtifacts(input); err == nil {
				t.Fatal("invalid artifact binding unexpectedly succeeded")
			}
		})
	}
}

func TestBuildPlanFromArtifactsFailsClosedWithoutReleaseNameBinding(t *testing.T) {
	input, _ := validPlanArtifactInput(t)
	delete(input.Bindings, "releaseName")
	plan, err := BuildPlanFromArtifacts(input)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Result != OutcomeUnknown || plan.SingleDomainDispatchAllowed() {
		t.Fatalf("missing releaseName plan = %#v", plan)
	}
	found := false
	for _, item := range plan.Unknown {
		if item.Source == "rendered-object" && item.Subject == "ownership-bindings" && strings.Contains(item.Reason, "releaseName") {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing releaseName was not recorded: %#v", plan.Unknown)
	}
}

func TestBuildPlanFromArtifactsDoesNotRetainCallerBuffers(t *testing.T) {
	input, _ := validPlanArtifactInput(t)
	wantOwnershipDigest := digestBytesSHA256(input.Ownership)
	wantTargetDigest := digestBytesSHA256(input.TargetCanonicalManifest)
	plan, err := BuildPlanFromArtifacts(input)
	if err != nil {
		t.Fatal(err)
	}
	for index := range input.Ownership {
		input.Ownership[index] = 'x'
	}
	for index := range input.TargetCanonicalManifest {
		input.TargetCanonicalManifest[index] = 'x'
	}
	input.Bindings["releaseNamespace"] = "mutated"
	if plan.Digests.Ownership != wantOwnershipDigest || plan.Digests.TargetManifest != wantTargetDigest {
		t.Fatalf("plan retained caller buffers: %#v", plan.Digests)
	}
	if got := plan.Digests.ClassificationContext.BindingMap()["releaseNamespace"]; got != "fugue-system" {
		t.Fatalf("plan retained caller bindings: %q", got)
	}
}

func validPlanArtifactInput(t *testing.T) (PlanArtifactInput, string) {
	t.Helper()
	ownership := []byte(testPlanArtifactOwnership)
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatal(err)
	}
	base := canonicalPlanArtifactManifest(t, spec, "base")
	target := canonicalPlanArtifactManifest(t, spec, "target")
	evidenceData := makeChangedFileEvidence(t, testEvidenceBaseCommit, testEvidenceTargetCommit, []ChangedFile{{
		Status: ChangeModified,
		Path:   "synthetic/node-local.go",
	}})
	verified, err := DecodeAndVerifyChangedFileEvidence(
		bytes.NewReader(evidenceData),
		testEvidenceBaseCommit,
		testEvidenceTargetCommit,
	)
	if err != nil {
		t.Fatal(err)
	}
	return PlanArtifactInput{
		Ownership:                 ownership,
		ChangedFileEvidence:       evidenceData,
		TrustedBaseCommit:         testEvidenceBaseCommit,
		TrustedTargetCommit:       testEvidenceTargetCommit,
		BaseCanonicalManifest:     base,
		TargetCanonicalManifest:   target,
		RepeatedCanonicalManifest: append([]byte(nil), target...),
		SSoTBaseDigest:            "ssot-base",
		SSoTTargetDigest:          "ssot-target",
		SSoTLiveDigest:            "ssot-base",
		DefaultNamespace:          "fugue-system",
		Bindings:                  map[string]string{"releaseName": "fugue", "releaseNamespace": "fugue-system"},
	}, verified.Digest()
}

func canonicalPlanArtifactManifest(t *testing.T, spec *OwnershipSpec, value string) []byte {
	t.Helper()
	raw := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: node-local\n  namespace: fugue-system\ndata:\n  value: " + value + "\n")
	canonical, err := CanonicalizeRenderedManifest(raw, spec, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
