package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/releaseadapter"
	"fugue/internal/releasedomain"
	"fugue/internal/releaseevidence"
)

func TestWritePublicEvidenceSingleTraceMatrix(t *testing.T) {
	for _, test := range []struct {
		name              string
		events            []traceStep
		write             bool
		rollbackAttempted bool
		rollbackCompleted bool
		rollbackFailed    bool
	}{
		{
			name: "success",
			events: []traceStep{
				{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhasePrepare, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhasePrepare, releaseadapter.TraceStateSucceeded},
				{releaseadapter.TracePhaseApply, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhaseApply, releaseadapter.TraceStateSucceeded},
				{releaseadapter.TracePhaseVerify, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhaseVerify, releaseadapter.TraceStateSucceeded},
				{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateSucceeded},
			},
			write: true,
		},
		{
			name: "pre-apply failure",
			events: []traceStep{
				{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhasePrepare, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhasePrepare, releaseadapter.TraceStateFailed},
				{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateFailed},
			},
		},
		{
			name: "rollback completed",
			events: []traceStep{
				{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhasePrepare, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhasePrepare, releaseadapter.TraceStateSucceeded},
				{releaseadapter.TracePhaseApply, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhaseApply, releaseadapter.TraceStateFailed},
				{releaseadapter.TracePhaseRollback, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhaseRollback, releaseadapter.TraceStateSucceeded},
				{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateFailed},
			},
			write:             true,
			rollbackAttempted: true,
			rollbackCompleted: true,
		},
		{
			name: "rollback failed",
			events: []traceStep{
				{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhasePrepare, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhasePrepare, releaseadapter.TraceStateSucceeded},
				{releaseadapter.TracePhaseApply, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhaseApply, releaseadapter.TraceStateFailed},
				{releaseadapter.TracePhaseRollback, releaseadapter.TraceStateStarted},
				{releaseadapter.TracePhaseRollback, releaseadapter.TraceStateFailed},
				{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateFailed},
			},
			write:             true,
			rollbackAttempted: true,
			rollbackFailed:    true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := authorizeSingleFixture(t)
			decision := readBundleDecision(t, fixture.bundle)
			traceFile := writeCanonicalTrace(t, fixture.root, decision, test.events)
			traceBytes, err := readSecureTraceSource(traceFile, maxPrivateTraceBytes)
			if err != nil {
				t.Fatalf("secure trace fixture: %v", err)
			}
			if _, err := decodePrivateTrace(traceBytes, decision); err != nil {
				t.Fatalf("decode trace fixture: %v", err)
			}
			output := newPublicOutputPath(t)
			args := publicEvidenceArgs(fixture, traceFile, output, test.write, test.rollbackAttempted, test.rollbackCompleted, test.rollbackFailed)
			var stdout, stderr bytes.Buffer
			if got := run(args, &stdout, &stderr); got != 0 {
				t.Fatalf("write public evidence exit = %d, stderr = %q", got, stderr.String())
			}
			if stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("public evidence command output = %q / %q", stdout.String(), stderr.String())
			}
			artifact := decodePublicArtifact(t, output)
			if artifact.Outcome != releaseevidence.OutcomeSingle || artifact.Domain != releasedomain.DomainNodeLocal {
				t.Fatalf("public artifact outcome/domain = %q/%q", artifact.Outcome, artifact.Domain)
			}
			if artifact.HeadSHA != fixture.targetCommit || artifact.HelmBaseRevision != 1 || artifact.HelmTargetRevision != 2 {
				t.Fatalf("public artifact revision binding = %#v", artifact)
			}
			if !artifact.ReverseAuthorized || artifact.ReverseDomain != artifact.Domain || artifact.ReverseObjectCount != 1 {
				t.Fatalf("public reverse evidence = %#v", artifact)
			}
			if artifact.WriteBoundaryCrossed != test.write || artifact.RollbackAttempted != test.rollbackAttempted || artifact.RollbackCompleted != test.rollbackCompleted || artifact.RollbackFailed != test.rollbackFailed {
				t.Fatalf("public write/rollback flags = %#v", artifact)
			}
			if len(artifact.Trace) != len(test.events) {
				t.Fatalf("public trace length = %d, want %d", len(artifact.Trace), len(test.events))
			}
			assertPublicFileIsPrivateAndRedacted(t, output)
		})
	}
}

func TestPrivateTraceWireSchemaMatchesDormantAdapter(t *testing.T) {
	if privateTraceAPIVersion != releaseadapter.TraceAPIVersion || privateTraceKind != releaseadapter.TraceKind {
		t.Fatal("private trace wire identity drifted from dormant adapter")
	}
	for _, pair := range []struct{ local, adapter string }{
		{string(privateTracePhaseTransaction), string(releaseadapter.TracePhaseTransaction)},
		{string(privateTracePhasePrepare), string(releaseadapter.TracePhasePrepare)},
		{string(privateTracePhaseApply), string(releaseadapter.TracePhaseApply)},
		{string(privateTracePhaseVerify), string(releaseadapter.TracePhaseVerify)},
		{string(privateTracePhaseRollback), string(releaseadapter.TracePhaseRollback)},
		{string(privateTraceStateStarted), string(releaseadapter.TraceStateStarted)},
		{string(privateTraceStateSucceeded), string(releaseadapter.TraceStateSucceeded)},
		{string(privateTraceStateFailed), string(releaseadapter.TraceStateFailed)},
	} {
		if pair.local != pair.adapter {
			t.Fatalf("private trace wire enum drift = %q/%q", pair.local, pair.adapter)
		}
	}
}

func TestWritePublicEvidenceNoWriteOutcomes(t *testing.T) {
	for _, test := range []struct {
		name    string
		outcome releasedomain.Outcome
		domains []releasedomain.Domain
		want    releaseevidence.Outcome
	}{
		{name: "zero", outcome: releasedomain.OutcomeZero, want: releaseevidence.OutcomeZero},
		{name: "multiple", outcome: releasedomain.OutcomeMultiple, domains: []releasedomain.Domain{releasedomain.DomainNodeLocal, releasedomain.DomainBackup}, want: releaseevidence.OutcomeMultiple},
		{name: "unknown", outcome: releasedomain.OutcomeUnknown, want: releaseevidence.OutcomeUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := authorizeOutcomeFixture(t, test.domains, test.outcome)
			traceFile := writeCanonicalTrace(t, fixture.root, dispatchDecision{}, nil)
			output := newPublicOutputPath(t)
			args := publicEvidenceArgs(fixture, traceFile, output, false, false, false, false)
			var stdout, stderr bytes.Buffer
			if got := run(args, &stdout, &stderr); got != 0 {
				t.Fatalf("write no-write evidence exit = %d, stderr = %q", got, stderr.String())
			}
			artifact := decodePublicArtifact(t, output)
			if artifact.Outcome != test.want || artifact.Domain != "" || artifact.ReverseAuthorized || artifact.ReverseObjectCount != 0 || len(artifact.Trace) != 0 {
				t.Fatalf("no-write artifact = %#v", artifact)
			}
			if artifact.HelmBaseRevision != 1 || artifact.HelmTargetRevision != 1 {
				t.Fatalf("no-write revisions = %d/%d", artifact.HelmBaseRevision, artifact.HelmTargetRevision)
			}
			assertPublicFileIsPrivateAndRedacted(t, output)
		})
	}
}

func TestWritePublicEvidenceRejectsTraceAndFlagTamperWithoutLeaks(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, commandFixture, string, []string)
	}{
		{name: "trace unknown free text", mutate: func(t *testing.T, _ commandFixture, trace string, _ []string) {
			overwritePrivateFile(t, trace, []byte("{\"private\":\""+testSecretSentinel+"\"}\n"))
		}},
		{name: "trace duplicate field", mutate: func(t *testing.T, _ commandFixture, trace string, _ []string) {
			data := mustReadFile(t, trace)
			data = bytes.Replace(data, []byte("{\"apiVersion\""), []byte("{\"apiVersion\":\"release-transaction-trace.fugue.dev/v1\",\"apiVersion\""), 1)
			overwritePrivateFile(t, trace, data)
		}},
		{name: "trace noncanonical whitespace", mutate: func(t *testing.T, _ commandFixture, trace string, _ []string) {
			data := append([]byte{' '}, mustReadFile(t, trace)...)
			overwritePrivateFile(t, trace, data)
		}},
		{name: "trace trailing JSON", mutate: func(t *testing.T, _ commandFixture, trace string, _ []string) {
			appendPrivateFile(t, trace, []byte("{}\n"))
		}},
		{name: "trace wrong domain", mutate: func(t *testing.T, _ commandFixture, trace string, _ []string) {
			data := bytes.Replace(mustReadFile(t, trace), []byte("node-local"), []byte("image-cache"), 1)
			overwritePrivateFile(t, trace, data)
		}},
		{name: "trace mode", mutate: func(t *testing.T, _ commandFixture, trace string, _ []string) {
			if err := os.Chmod(trace, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "trace parent mode", mutate: func(t *testing.T, _ commandFixture, trace string, _ []string) {
			if err := os.Chmod(filepath.Dir(trace), 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "trace hardlink", mutate: func(t *testing.T, fixture commandFixture, trace string, _ []string) {
			if err := os.Link(trace, filepath.Join(fixture.root, "trace-second-link")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "trace symlink", mutate: func(t *testing.T, _ commandFixture, trace string, _ []string) {
			real := trace + "-real"
			if err := os.Rename(trace, real); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, trace); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "head mismatch", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			setFlagArgument(args, "--head-sha", strings.Repeat("3", 40))
		}},
		{name: "write flag mismatch", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			setFlagArgument(args, "--write-boundary-crossed", "false")
		}},
		{name: "missing bool", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			for index, argument := range args {
				if argument == "--rollback-failed" {
					args[index] = "--removed-fixed-flag"
				}
			}
		}},
		{name: "duplicate flag", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			args[len(args)-1] = "--run-id"
		}},
		{name: "output aliases trace", mutate: func(_ *testing.T, _ commandFixture, trace string, args []string) {
			setFlagArgument(args, "--output", trace)
		}},
		{name: "output inside bundle", mutate: func(_ *testing.T, fixture commandFixture, _ string, args []string) {
			setFlagArgument(args, "--output", filepath.Join(fixture.bundle, "public.json"))
		}},
		{name: "sentinel output path error", mutate: func(_ *testing.T, fixture commandFixture, _ string, args []string) {
			setFlagArgument(args, "--output", filepath.Join(fixture.root, testSecretSentinel, "public.json"))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := authorizeSingleFixture(t)
			decision := readBundleDecision(t, fixture.bundle)
			traceFile := writeCanonicalTrace(t, fixture.root, decision, successTraceSteps())
			output := newPublicOutputPath(t)
			args := publicEvidenceArgs(fixture, traceFile, output, true, false, false, false)
			test.mutate(t, fixture, traceFile, args)
			var stdout, stderr bytes.Buffer
			if got := run(args, &stdout, &stderr); got != 1 {
				t.Fatalf("tampered public evidence exit = %d, stdout = %q, stderr = %q", got, stdout.String(), stderr.String())
			}
			if stdout.Len() != 0 || strings.Contains(stderr.String(), testSecretSentinel) || strings.Contains(stderr.String(), "--token") {
				t.Fatalf("tampered command leaked private material: %q / %q", stdout.String(), stderr.String())
			}
			if _, err := os.Lstat(output); !os.IsNotExist(err) {
				t.Fatalf("tampered command published output: %v", err)
			}
		})
	}
}

func TestWritePublicEvidenceRejectsNonEmptyNoWriteTrace(t *testing.T) {
	fixture := authorizeOutcomeFixture(t, nil, releasedomain.OutcomeZero)
	decision := readBundleDecision(t, fixture.bundle)
	traceFile := writeCanonicalTrace(t, fixture.root, dispatchDecision{
		SelectedDomain: string(releasedomain.DomainNodeLocal),
		PlanDigest:     decision.PlanDigest,
	}, []traceStep{
		{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateStarted},
		{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateFailed},
	})
	output := newPublicOutputPath(t)
	args := publicEvidenceArgs(fixture, traceFile, output, false, false, false, false)
	var stdout, stderr bytes.Buffer
	if got := run(args, &stdout, &stderr); got != 1 {
		t.Fatalf("no-write trace exit = %d, stdout = %q, stderr = %q", got, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 || strings.Contains(stderr.String(), testSecretSentinel) {
		t.Fatalf("no-write trace failure leaked input: %q / %q", stdout.String(), stderr.String())
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		t.Fatalf("no-write trace published output: %v", err)
	}
}

func TestWriteGenesisPublicEvidenceIsDeterministicAndRedacted(t *testing.T) {
	fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
	evidencePath := flagValue(t, fixture.args, "--changed-evidence")
	// Paths are private changed-file evidence and must never be projected.
	overwritePrivateFile(t, evidencePath, testChangedEvidence(t, fixture.baseCommit, fixture.targetCommit, []releasedomain.ChangedFile{{
		Status: releasedomain.ChangeModified,
		Path:   "private/" + testSecretSentinel,
	}}))
	outputOne := newPublicOutputPath(t)
	outputTwo := newPublicOutputPath(t)
	argsOne := genesisEvidenceArgs(t, fixture, evidencePath, outputOne)
	argsTwo := genesisEvidenceArgs(t, fixture, evidencePath, outputTwo)
	for _, args := range [][]string{argsOne, argsTwo} {
		var stdout, stderr bytes.Buffer
		if got := run(args, &stdout, &stderr); got != 0 {
			t.Fatalf("write genesis evidence exit = %d, stderr = %q", got, stderr.String())
		}
		if stdout.Len() != 0 || stderr.Len() != 0 {
			t.Fatalf("genesis command output = %q / %q", stdout.String(), stderr.String())
		}
	}
	first := decodePublicArtifact(t, outputOne)
	second := decodePublicArtifact(t, outputTwo)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("genesis evidence is not deterministic: %#v / %#v", first, second)
	}
	if first.Outcome != releaseevidence.OutcomeGenesisZero || first.HelmBaseRevision != 0 || first.HelmTargetRevision != 0 || len(first.Trace) != 0 || first.WriteBoundaryCrossed || first.ReverseAuthorized {
		t.Fatalf("genesis evidence = %#v", first)
	}
	verified, err := releasedomain.DecodeAndVerifyChangedFileEvidence(bytes.NewReader(mustReadFile(t, evidencePath)), fixture.baseCommit, fixture.targetCommit)
	if err != nil {
		t.Fatal(err)
	}
	if first.ChangedEvidenceDigest != verified.Digest() {
		t.Fatalf("genesis changed evidence digest = %q", first.ChangedEvidenceDigest)
	}
	if first.OwnershipDigest != digestBytes(mustReadFile(t, flagValue(t, fixture.args, "--ownership"))) {
		t.Fatalf("genesis ownership digest = %q", first.OwnershipDigest)
	}
	assertPublicFileIsPrivateAndRedacted(t, outputOne)
}

func TestWriteGenesisPublicEvidenceRejectsRevisionSchemaAndPathAttacks(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, commandFixture, string, []string)
	}{
		{name: "expected head mismatch", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			setFlagArgument(args, "--expected-head-sha", strings.Repeat("3", 40))
		}},
		{name: "parent mismatch", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			setFlagArgument(args, "--expected-parent-sha", strings.Repeat("3", 40))
		}},
		{name: "evidence base mismatch", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			setFlagArgument(args, "--evidence-base-sha", strings.Repeat("3", 40))
		}},
		{name: "evidence target mismatch", mutate: func(t *testing.T, fixture commandFixture, evidence string, _ []string) {
			overwritePrivateFile(t, evidence, testChangedEvidence(t, fixture.baseCommit, strings.Repeat("3", 40), []releasedomain.ChangedFile{}))
		}},
		{name: "evidence mode", mutate: func(t *testing.T, _ commandFixture, evidence string, _ []string) {
			if err := os.Chmod(evidence, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "expected change mismatch", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			setFlagArgument(args, "--expected-change", "other/fixed-file.go")
		}},
		{name: "expected change whitespace", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			setFlagArgument(args, "--expected-change", "invalid path")
		}},
		{name: "expected change parent", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			setFlagArgument(args, "--expected-change", "..")
		}},
		{name: "ownership symlink", mutate: func(t *testing.T, fixture commandFixture, _ string, _ []string) {
			ownership := flagValue(t, fixture.args, "--ownership")
			real := ownership + "-real"
			if err := os.Rename(ownership, real); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, ownership); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "output aliases ownership", mutate: func(t *testing.T, fixture commandFixture, _ string, args []string) {
			setFlagArgument(args, "--output", flagValue(t, fixture.args, "--ownership"))
		}},
		{name: "evidence symlink", mutate: func(t *testing.T, _ commandFixture, evidence string, _ []string) {
			real := evidence + "-real"
			if err := os.Rename(evidence, real); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, evidence); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "output aliases evidence", mutate: func(_ *testing.T, _ commandFixture, evidence string, args []string) {
			setFlagArgument(args, "--output", evidence)
		}},
		{name: "domain bypass flag", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			renameFlag(args, "--run-id", "--domain="+testSecretSentinel)
		}},
		{name: "duplicate head", mutate: func(_ *testing.T, _ commandFixture, _ string, args []string) {
			renameFlag(args, "--expected-parent-sha", "--head-sha")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
			evidencePath := flagValue(t, fixture.args, "--changed-evidence")
			output := newPublicOutputPath(t)
			args := genesisEvidenceArgs(t, fixture, evidencePath, output)
			test.mutate(t, fixture, evidencePath, args)
			var stdout, stderr bytes.Buffer
			if got := run(args, &stdout, &stderr); got != 1 {
				t.Fatalf("tampered genesis exit = %d, stdout = %q, stderr = %q", got, stdout.String(), stderr.String())
			}
			if stdout.Len() != 0 || strings.Contains(stderr.String(), testSecretSentinel) || strings.Contains(stderr.String(), "--domain") {
				t.Fatalf("genesis error leaked caller data: %q / %q", stdout.String(), stderr.String())
			}
			if _, err := os.Lstat(output); !os.IsNotExist(err) {
				t.Fatalf("tampered genesis published output: %v", err)
			}
		})
	}
}

func TestWriteGenesisPublicEvidenceAllowsDistinctEvidenceBaseAndDirectParent(t *testing.T) {
	fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
	evidencePath := flagValue(t, fixture.args, "--changed-evidence")
	output := newPublicOutputPath(t)
	args := genesisEvidenceArgs(t, fixture, evidencePath, output)
	directParent := strings.Repeat("4", 40)
	setFlagArgument(args, "--expected-parent-sha", directParent)
	setFlagArgument(args, "--actual-parent-sha", directParent)

	var stdout, stderr bytes.Buffer
	if got := run(args, &stdout, &stderr); got != 0 {
		t.Fatalf("distinct genesis base/parent exit = %d, stdout = %q, stderr = %q", got, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("distinct genesis base/parent output = %q / %q", stdout.String(), stderr.String())
	}
	artifact := decodePublicArtifact(t, output)
	if artifact.Outcome != releaseevidence.OutcomeGenesisZero || artifact.HeadSHA != fixture.targetCommit {
		t.Fatalf("distinct genesis base/parent artifact = %#v", artifact)
	}
}

func TestWriteGenesisPublicEvidenceRequiresUniqueExpectedChanges(t *testing.T) {
	fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
	evidencePath := flagValue(t, fixture.args, "--changed-evidence")
	for _, test := range []struct {
		name   string
		mutate func([]string) []string
	}{
		{name: "missing", mutate: func(args []string) []string {
			result := make([]string, 0, len(args)-2)
			for index := 0; index < len(args); index++ {
				if args[index] == "--expected-change" {
					index++
					continue
				}
				result = append(result, args[index])
			}
			return result
		}},
		{name: "duplicate", mutate: func(args []string) []string {
			value := ""
			for index := range args {
				if args[index] == "--expected-change" {
					value = args[index+1]
					break
				}
			}
			return append(args, "--expected-change", value)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := newPublicOutputPath(t)
			args := test.mutate(genesisEvidenceArgs(t, fixture, evidencePath, output))
			var stdout, stderr bytes.Buffer
			if got := run(args, &stdout, &stderr); got != 1 {
				t.Fatalf("invalid expected-change exit = %d", got)
			}
			if stdout.Len() != 0 || strings.Contains(stderr.String(), testSecretSentinel) {
				t.Fatalf("expected-change failure leaked input: %q / %q", stdout.String(), stderr.String())
			}
			if _, err := os.Lstat(output); !os.IsNotExist(err) {
				t.Fatalf("invalid expected-change published output: %v", err)
			}
		})
	}
}

func TestWriteBlockedPublicEvidenceUsesPreRenderUnknownPlan(t *testing.T) {
	for _, domains := range [][]releasedomain.Domain{
		{releasedomain.DomainNodeLocal},
		{releasedomain.DomainNodeLocal, releasedomain.DomainBackup},
	} {
		fixture := newCommandFixture(t, domains, releasedomain.OutcomeSingle)
		output := newPublicOutputPath(t)
		args := blockedEvidenceArgs(t, fixture, output)
		var stdout, stderr bytes.Buffer
		if got := run(args, &stdout, &stderr); got != 0 {
			t.Fatalf("write blocked evidence exit = %d, stderr = %q", got, stderr.String())
		}
		artifact := decodePublicArtifact(t, output)
		if artifact.Outcome != releaseevidence.OutcomeUnknown || artifact.Domain != "" || artifact.ReverseAuthorized || len(artifact.Trace) != 0 || artifact.WriteBoundaryCrossed {
			t.Fatalf("blocked artifact = %#v", artifact)
		}
		if artifact.HelmBaseRevision != 7 || artifact.HelmTargetRevision != 7 || artifact.ForwardDigest != artifact.ReverseDigest {
			t.Fatalf("blocked artifact revisions/digests = %#v", artifact)
		}
		emptyDigest := digestBytes([]byte{})
		if artifact.BaseDigest != emptyDigest || artifact.TargetDigest != emptyDigest || artifact.RepeatDigest != emptyDigest {
			t.Fatalf("blocked artifact did not bind canonical empty renders: %#v", artifact)
		}
		assertPublicFileIsPrivateAndRedacted(t, output)
	}
}

func TestWriteBlockedPublicEvidenceRejectsZeroIncompleteBindingAndBypass(t *testing.T) {
	for _, test := range []struct {
		name    string
		outcome releasedomain.Outcome
		mutate  func(*testing.T, commandFixture, []string)
	}{
		{name: "zero outcome", outcome: releasedomain.OutcomeZero},
		{name: "missing release name binding", outcome: releasedomain.OutcomeSingle, mutate: func(_ *testing.T, _ commandFixture, args []string) {
			setFlagArgument(args, "--binding", "releaseNamespace=fugue-system")
		}},
		{name: "namespace conflict", outcome: releasedomain.OutcomeSingle, mutate: func(_ *testing.T, _ commandFixture, args []string) {
			setFlagArgument(args, "--binding", "releaseNamespace=other")
		}},
		{name: "head mismatch", outcome: releasedomain.OutcomeSingle, mutate: func(_ *testing.T, _ commandFixture, args []string) {
			setFlagArgument(args, "--head-sha", strings.Repeat("3", 40))
		}},
		{name: "outcome bypass", outcome: releasedomain.OutcomeSingle, mutate: func(_ *testing.T, _ commandFixture, args []string) {
			renameFlag(args, "--run-id", "--outcome="+testSecretSentinel)
		}},
		{name: "changed evidence mode", outcome: releasedomain.OutcomeSingle, mutate: func(t *testing.T, fixture commandFixture, _ []string) {
			if err := os.Chmod(flagValue(t, fixture.args, "--changed-evidence"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "ownership symlink", outcome: releasedomain.OutcomeSingle, mutate: func(t *testing.T, fixture commandFixture, _ []string) {
			ownership := flagValue(t, fixture.args, "--ownership")
			real := ownership + "-real"
			if err := os.Rename(ownership, real); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, ownership); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, test.outcome)
			output := newPublicOutputPath(t)
			args := blockedEvidenceArgs(t, fixture, output)
			if test.mutate != nil {
				test.mutate(t, fixture, args)
			}
			var stdout, stderr bytes.Buffer
			if got := run(args, &stdout, &stderr); got != 1 {
				t.Fatalf("rejected blocked evidence exit = %d, stdout = %q, stderr = %q", got, stdout.String(), stderr.String())
			}
			if stdout.Len() != 0 || strings.Contains(stderr.String(), testSecretSentinel) {
				t.Fatalf("blocked evidence failure leaked input: %q / %q", stdout.String(), stderr.String())
			}
			if _, err := os.Lstat(output); !os.IsNotExist(err) {
				t.Fatalf("rejected blocked evidence published output: %v", err)
			}
		})
	}
}

func TestExactRenderedObjectCountUsesOnlyExactNonIgnoredEvidence(t *testing.T) {
	classification := releasedomain.RenderedClassification{
		Domains: []releasedomain.Domain{releasedomain.DomainNodeLocal},
		Evidence: []releasedomain.Evidence{
			{Source: "rendered-object", Domains: []releasedomain.Domain{releasedomain.DomainNodeLocal}},
			{Source: "rendered-object", Domains: []releasedomain.Domain{releasedomain.DomainNodeLocal}, Ignored: true},
			{Source: "rendered-object", Domains: []releasedomain.Domain{releasedomain.DomainNodeLocal}},
		},
	}
	count, err := exactRenderedObjectCount(classification, releasedomain.DomainNodeLocal)
	if err != nil || count != 2 {
		t.Fatalf("exact object count = %d, %v", count, err)
	}
	classification.Evidence[2].Domains = []releasedomain.Domain{releasedomain.DomainBackup}
	if _, err := exactRenderedObjectCount(classification, releasedomain.DomainNodeLocal); err == nil {
		t.Fatal("cross-domain rendered evidence was counted")
	}
}

type traceStep struct {
	phase releaseadapter.TracePhase
	state releaseadapter.TraceState
}

func successTraceSteps() []traceStep {
	return []traceStep{
		{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateStarted},
		{releaseadapter.TracePhasePrepare, releaseadapter.TraceStateStarted},
		{releaseadapter.TracePhasePrepare, releaseadapter.TraceStateSucceeded},
		{releaseadapter.TracePhaseApply, releaseadapter.TraceStateStarted},
		{releaseadapter.TracePhaseApply, releaseadapter.TraceStateSucceeded},
		{releaseadapter.TracePhaseVerify, releaseadapter.TraceStateStarted},
		{releaseadapter.TracePhaseVerify, releaseadapter.TraceStateSucceeded},
		{releaseadapter.TracePhaseTransaction, releaseadapter.TraceStateSucceeded},
	}
}

func writeCanonicalTrace(t *testing.T, root string, decision dispatchDecision, steps []traceStep) string {
	t.Helper()
	directory := filepath.Join(root, "trace-"+strings.ReplaceAll(t.Name(), "/", "-"))
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	for index, step := range steps {
		event := releaseadapter.TraceEvent{
			APIVersion: releaseadapter.TraceAPIVersion,
			Kind:       releaseadapter.TraceKind,
			Sequence:   uint64(index + 1),
			Domain:     releasedomain.Domain(decision.SelectedDomain),
			PlanDigest: decision.PlanDigest,
			Phase:      step.phase,
			State:      step.state,
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		data.Write(encoded)
		data.WriteByte('\n')
	}
	path := filepath.Join(directory, "transaction.jsonl")
	writePrivateFile(t, path, data.Bytes())
	return path
}

func publicEvidenceArgs(fixture commandFixture, trace, output string, write, attempted, completed, failed bool) []string {
	return []string{
		"write-public-evidence",
		"--bundle-dir", fixture.bundle,
		"--trace-file", trace,
		"--output", output,
		"--run-id", "123456789",
		"--run-attempt", "2",
		"--head-sha", fixture.targetCommit,
		"--write-boundary-crossed", strconvBool(write),
		"--rollback-attempted", strconvBool(attempted),
		"--rollback-completed", strconvBool(completed),
		"--rollback-failed", strconvBool(failed),
	}
}

func genesisEvidenceArgs(t *testing.T, fixture commandFixture, evidence, output string) []string {
	t.Helper()
	args := []string{
		"write-genesis-public-evidence",
		"--ownership", flagValue(t, fixture.args, "--ownership"),
		"--changed-evidence", evidence,
		"--evidence-base-sha", fixture.baseCommit,
		"--output", output,
		"--run-id", "123456789",
		"--run-attempt", "2",
		"--expected-head-sha", fixture.targetCommit,
		"--head-sha", fixture.targetCommit,
		"--expected-parent-sha", fixture.baseCommit,
		"--actual-parent-sha", fixture.baseCommit,
	}
	verified, err := releasedomain.DecodeAndVerifyChangedFileEvidence(bytes.NewReader(mustReadFile(t, evidence)), fixture.baseCommit, fixture.targetCommit)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range verified.Changes() {
		args = append(args, "--expected-change", change.Path)
	}
	return args
}

func blockedEvidenceArgs(t *testing.T, fixture commandFixture, output string) []string {
	t.Helper()
	return []string{
		"write-blocked-public-evidence",
		"--ownership", flagValue(t, fixture.args, "--ownership"),
		"--changed-evidence", flagValue(t, fixture.args, "--changed-evidence"),
		"--trusted-base-commit", fixture.baseCommit,
		"--trusted-target-commit", fixture.targetCommit,
		"--helm-revision", "7",
		"--namespace", "fugue-system",
		"--binding", "releaseName=fugue",
		"--output", output,
		"--run-id", "123456789",
		"--run-attempt", "2",
		"--head-sha", fixture.targetCommit,
	}
}

func strconvBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func authorizeOutcomeFixture(t *testing.T, domains []releasedomain.Domain, outcome releasedomain.Outcome) commandFixture {
	t.Helper()
	fixture := newCommandFixture(t, domains, outcome)
	var stdout, stderr bytes.Buffer
	wantExit := 0
	if outcome == releasedomain.OutcomeMultiple || outcome == releasedomain.OutcomeUnknown {
		wantExit = 2
	}
	if got := run(fixture.args, &stdout, &stderr); got != wantExit {
		t.Fatalf("authorize outcome exit = %d, want %d, stderr = %q", got, wantExit, stderr.String())
	}
	return fixture
}

func readBundleDecision(t *testing.T, bundle string) dispatchDecision {
	t.Helper()
	var decision dispatchDecision
	if err := decodeStrictJSON(mustReadFile(t, filepath.Join(bundle, decisionFilename)), &decision); err != nil {
		t.Fatal(err)
	}
	return decision
}

func newPublicOutputPath(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(directory, "public.json")
}

func decodePublicArtifact(t *testing.T, path string) releaseevidence.PublicArtifact {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	artifact, err := releaseevidence.Decode(file)
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func assertPublicFileIsPrivateAndRedacted(t *testing.T, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("public evidence mode = %v", info.Mode())
	}
	data := mustReadFile(t, path)
	for _, forbidden := range []string{testSecretSentinel, "--set-string", "--token", "/workspace", "argv", "error", "reason", "path"} {
		if bytes.Contains(bytes.ToLower(data), bytes.ToLower([]byte(forbidden))) {
			t.Fatalf("public evidence contains forbidden private material %q", forbidden)
		}
	}
}

func setFlagArgument(args []string, name, value string) {
	for index, argument := range args {
		if argument == name && index+1 < len(args) {
			args[index+1] = value
			return
		}
	}
}

func renameFlag(args []string, name, replacement string) {
	for index, argument := range args {
		if argument == name {
			args[index] = replacement
			return
		}
	}
}
