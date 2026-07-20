package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/releasedomain"
)

func TestOperationalReportEmitsDigestBoundNonAuthorizingEvidence(t *testing.T) {
	fixture := newOperationalCommandFixture(t)
	var stderr bytes.Buffer
	exitCode := runOperationalReport(fixture.args(), ioDiscard{}, &stderr)
	if exitCode != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stderr=%q", exitCode, stderr.String())
	}
	encoded, err := os.ReadFile(fixture.output)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Digest string `json:"digest"`
	}
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	report, err := releasedomain.DecodeAndVerifyOperationalDomainEvidence(bytes.NewReader(encoded), envelope.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if report.Observation != releasedomain.OutcomeSingle || report.CandidateDomain != releasedomain.DomainControlPlane {
		t.Fatalf("unexpected report: %#v", report)
	}
	if report.AuthorizationEligible {
		t.Fatal("report-only command emitted authorization-eligible evidence")
	}
}

func TestOperationalReportRejectsDuplicateFlagsAndTrustedDrift(t *testing.T) {
	fixture := newOperationalCommandFixture(t)
	duplicate := append(fixture.args(), "--output", filepath.Join(t.TempDir(), "second.json"))
	if exit := runOperationalReport(duplicate, ioDiscard{}, &bytes.Buffer{}); exit == 0 {
		t.Fatal("duplicate --output unexpectedly accepted")
	}
	singleDash := fixture.args()
	singleDash[0] = "-changed-evidence"
	if exit := runOperationalReport(singleDash, ioDiscard{}, &bytes.Buffer{}); exit == 0 {
		t.Fatal("non-canonical single-dash flag unexpectedly accepted")
	}

	drifted := fixture.args()
	for index := range drifted {
		if drifted[index] == "--trusted-base" {
			drifted[index+1] = strings.Repeat("d", 40)
		}
	}
	if exit := runOperationalReport(drifted, ioDiscard{}, &bytes.Buffer{}); exit == 0 {
		t.Fatal("trusted base drift unexpectedly accepted")
	}
}

func TestOperationalReportRefusesInputOutputAlias(t *testing.T) {
	fixture := newOperationalCommandFixture(t)
	args := fixture.args()
	for index := range args {
		if args[index] == "--output" {
			args[index+1] = fixture.plan
		}
	}
	if exit := runOperationalReport(args, ioDiscard{}, &bytes.Buffer{}); exit == 0 {
		t.Fatal("input/output alias unexpectedly accepted")
	}
}

func TestOperationalReportSourceHasNoAuthorizationConstructor(t *testing.T) {
	data, err := os.ReadFile("operational.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"ExecutionAuthorization",
		"NewTransactionEnvelope",
		"VerifyRollbackOwnership",
		"releaseadapter",
		"control_plane_release_dispatch",
	} {
		if bytes.Contains(data, []byte(forbidden)) {
			t.Fatalf("operational report source references authorization path %q", forbidden)
		}
	}
	mainSource, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(mainSource, []byte(`args[0] == "operational-report"`)) {
		t.Fatal("main command does not expose the frozen dormant subcommand")
	}
}

type operationalCommandFixture struct {
	base            string
	target          string
	changedEvidence string
	imagePlan       string
	plan            string
	planDigest      string
	output          string
}

func newOperationalCommandFixture(t *testing.T) operationalCommandFixture {
	t.Helper()
	directory := t.TempDir()
	base := strings.Repeat("a", 40)
	target := strings.Repeat("b", 40)
	result := evidenceResult{
		baseCommit:   base,
		targetCommit: target,
		changes: []releasedomain.ChangedFile{{
			Status:          releasedomain.ChangeModified,
			Path:            "internal/controller/controller.go",
			ConsumerDomains: []releasedomain.Domain{releasedomain.DomainControlPlane},
		}},
	}
	changedDocument, err := newEvidenceDocument(result)
	if err != nil {
		t.Fatal(err)
	}
	changedBytes, err := json.MarshalIndent(changedDocument, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	changedBytes = append(changedBytes, '\n')
	changedPath := filepath.Join(directory, "changed.json")
	writeOperationalFixture(t, changedPath, changedBytes)

	imagePlan, err := releasedomain.NewOperationalImageRolloutPlan(
		base,
		target,
		changedDocument.Digest,
		[]releasedomain.OperationalImageRolloutTarget{{
			Name:             "controller",
			SourceBaseCommit: strings.Repeat("d", 40),
			ArtifactDigest:   "sha256:" + strings.Repeat("e", 64),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	imagePlanBytes, err := releasedomain.MarshalOperationalImageRolloutPlan(imagePlan)
	if err != nil {
		t.Fatal(err)
	}
	imagePlanPath := filepath.Join(directory, "image-plan.json")
	writeOperationalFixture(t, imagePlanPath, imagePlanBytes)

	context, err := releasedomain.NewClassificationContextEvidence(
		"fugue-system",
		map[string]string{"releaseNamespace": "fugue-system"},
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan := releasedomain.BuildPlan(releasedomain.PlanInput{
		Files: releasedomain.FileClassification{
			Domains: []releasedomain.Domain{releasedomain.DomainControlPlane},
			Evidence: []releasedomain.Evidence{{
				Source: "changed-file", Subject: "internal/controller/controller.go", Domains: []releasedomain.Domain{releasedomain.DomainControlPlane},
			}},
		},
		Rendered: releasedomain.RenderedClassification{
			Domains: []releasedomain.Domain{releasedomain.DomainControlPlane},
			Evidence: []releasedomain.Evidence{{
				Source: "rendered-object", Subject: "apps/v1 Deployment fugue-system/fugue-controller", Domains: []releasedomain.Domain{releasedomain.DomainControlPlane},
			}},
		},
		Digests: releasedomain.DigestEvidence{
			Base:                   "base-render",
			Target:                 "target-render",
			Live:                   "base-render",
			BaseManifest:           "sha256:" + strings.Repeat("1", 64),
			TargetManifest:         "sha256:" + strings.Repeat("2", 64),
			RepeatedTargetManifest: "sha256:" + strings.Repeat("2", 64),
			Ownership:              "sha256:" + strings.Repeat("3", 64),
			ChangedFiles:           changedDocument.Digest,
			ClassificationContext:  context,
		},
	})
	if plan.Result != releasedomain.OutcomeSingle {
		t.Fatalf("fixture plan = %#v", plan)
	}
	planBytes, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	planBytes = append(planBytes, '\n')
	planPath := filepath.Join(directory, "plan.json")
	writeOperationalFixture(t, planPath, planBytes)

	return operationalCommandFixture{
		base:            base,
		target:          target,
		changedEvidence: changedPath,
		imagePlan:       imagePlanPath,
		plan:            planPath,
		planDigest:      plan.PlanDigest,
		output:          filepath.Join(directory, "report.json"),
	}
}

func (fixture operationalCommandFixture) args() []string {
	return []string{
		"--changed-evidence", fixture.changedEvidence,
		"--image-plan", fixture.imagePlan,
		"--plan", fixture.plan,
		"--plan-digest", fixture.planDigest,
		"--trusted-base", fixture.base,
		"--trusted-target", fixture.target,
		"--output", fixture.output,
	}
}

func writeOperationalFixture(t *testing.T, filename string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filename, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(data []byte) (int, error) { return len(data), nil }
