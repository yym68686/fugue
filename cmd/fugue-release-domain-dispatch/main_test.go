package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/releasedomain"
)

const testSecretSentinel = "RAW-ARGV-MANIFEST-SENTINEL-49f8c3"

type commandFixture struct {
	root         string
	bundle       string
	args         []string
	baseCommit   string
	targetCommit string
}

type testEvidencePayload struct {
	APIVersion   string                      `json:"apiVersion"`
	Kind         string                      `json:"kind"`
	Policy       string                      `json:"policy"`
	BaseCommit   string                      `json:"baseCommit"`
	TargetCommit string                      `json:"targetCommit"`
	Changes      []releasedomain.ChangedFile `json:"changes"`
}

type testEvidenceDocument struct {
	APIVersion   string                      `json:"apiVersion"`
	Kind         string                      `json:"kind"`
	Policy       string                      `json:"policy"`
	BaseCommit   string                      `json:"baseCommit"`
	TargetCommit string                      `json:"targetCommit"`
	Changes      []releasedomain.ChangedFile `json:"changes"`
	Digest       string                      `json:"digest"`
}

func TestAuthorizeAndVerifyEverySingleDomain(t *testing.T) {
	for _, domain := range releasedomain.KnownDomains() {
		domain := domain
		t.Run(string(domain), func(t *testing.T) {
			fixture := newCommandFixture(t, []releasedomain.Domain{domain}, releasedomain.OutcomeSingle)
			var authorizeOut, authorizeErr bytes.Buffer
			if got := run(fixture.args, &authorizeOut, &authorizeErr); got != 0 {
				t.Fatalf("authorize exit = %d, stderr = %s", got, authorizeErr.String())
			}
			assertFixedResult(t, authorizeOut.String(), "single", string(domain))
			if strings.Contains(authorizeOut.String(), testSecretSentinel) {
				t.Fatal("authorize stdout leaked raw material")
			}

			var verifyOut, verifyErr bytes.Buffer
			if got := run([]string{"verify", "--bundle-dir", fixture.bundle}, &verifyOut, &verifyErr); got != 0 {
				t.Fatalf("verify exit = %d, stderr = %s", got, verifyErr.String())
			}
			assertFixedResult(t, verifyOut.String(), "single", string(domain))
			if strings.Contains(verifyOut.String(), testSecretSentinel) {
				t.Fatal("verify stdout leaked raw material")
			}
			assertBundleModes(t, fixture.bundle, expectedBundleFiles(releasedomain.OutcomeSingle))
			decision := mustReadFile(t, filepath.Join(fixture.bundle, decisionFilename))
			for _, forbidden := range []string{testSecretSentinel, "apiVersion: v1", "helm\x00upgrade", "errors", "reason"} {
				if bytes.Contains(decision, []byte(forbidden)) {
					t.Fatalf("decision contains forbidden raw material %q", forbidden)
				}
			}
		})
	}
}

func TestAuthorizeAndVerifyZeroWithoutExecutionAuthorization(t *testing.T) {
	fixture := newCommandFixture(t, nil, releasedomain.OutcomeZero)
	var stdout, stderr bytes.Buffer
	if got := run(fixture.args, &stdout, &stderr); got != 0 {
		t.Fatalf("authorize exit = %d, stderr = %s", got, stderr.String())
	}
	assertFixedResult(t, stdout.String(), "zero", "")
	for _, name := range []string{envelopeFilename, executionBindingFilename, rollbackEvidenceFilename} {
		if _, err := os.Lstat(filepath.Join(fixture.bundle, name)); !os.IsNotExist(err) {
			t.Fatalf("zero bundle unexpectedly contains %s", name)
		}
	}
	stdout.Reset()
	stderr.Reset()
	if got := run([]string{"verify", "--bundle-dir", fixture.bundle}, &stdout, &stderr); got != 0 {
		t.Fatalf("verify exit = %d, stderr = %s", got, stderr.String())
	}
	assertFixedResult(t, stdout.String(), "zero", "")
}

func TestBlockedOutcomesPersistDecisionAndExitTwo(t *testing.T) {
	for _, test := range []struct {
		name    string
		outcome releasedomain.Outcome
		domains []releasedomain.Domain
	}{
		{name: "multiple", outcome: releasedomain.OutcomeMultiple, domains: []releasedomain.Domain{releasedomain.DomainNodeLocal, releasedomain.DomainImageCache}},
		{name: "unknown", outcome: releasedomain.OutcomeUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCommandFixture(t, test.domains, test.outcome)
			var stdout, stderr bytes.Buffer
			if got := run(fixture.args, &stdout, &stderr); got != 2 {
				t.Fatalf("authorize exit = %d, stderr = %s", got, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("blocked authorize stdout = %q", stdout.String())
			}
			for _, name := range []string{envelopeFilename, executionBindingFilename, rollbackEvidenceFilename} {
				if _, err := os.Lstat(filepath.Join(fixture.bundle, name)); !os.IsNotExist(err) {
					t.Fatalf("blocked bundle unexpectedly contains %s", name)
				}
			}
			stdout.Reset()
			stderr.Reset()
			if got := run([]string{"verify", "--bundle-dir", fixture.bundle}, &stdout, &stderr); got != 2 {
				t.Fatalf("verify exit = %d, stderr = %s", got, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("blocked verify stdout = %q", stdout.String())
			}
		})
	}
}

func TestClassifyFilesIsReadOnlyHintNotAuthorization(t *testing.T) {
	for _, test := range []struct {
		name       string
		outcome    releasedomain.Outcome
		domains    []releasedomain.Domain
		wantExit   int
		wantOutput string
	}{
		{name: "zero", outcome: releasedomain.OutcomeZero, wantExit: 0, wantOutput: "zero\n"},
		{name: "single", outcome: releasedomain.OutcomeSingle, domains: []releasedomain.Domain{releasedomain.DomainNodeLocal}, wantExit: 0, wantOutput: "single\tnode-local\n"},
		{name: "multiple", outcome: releasedomain.OutcomeMultiple, domains: []releasedomain.Domain{releasedomain.DomainNodeLocal, releasedomain.DomainBackup}, wantExit: 2, wantOutput: "multiple\n"},
		{name: "unknown", outcome: releasedomain.OutcomeUnknown, wantExit: 2, wantOutput: "unknown\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCommandFixture(t, test.domains, test.outcome)
			args := []string{
				"classify-files",
				"--ownership", flagValue(t, fixture.args, "--ownership"),
				"--changed-evidence", flagValue(t, fixture.args, "--changed-evidence"),
				"--trusted-base-commit", fixture.baseCommit,
				"--trusted-target-commit", fixture.targetCommit,
			}
			var stdout, stderr bytes.Buffer
			if got := run(args, &stdout, &stderr); got != test.wantExit {
				t.Fatalf("classify-files exit = %d, want %d, stderr = %s", got, test.wantExit, stderr.String())
			}
			if stdout.String() != test.wantOutput {
				t.Fatalf("classify-files stdout = %q, want %q", stdout.String(), test.wantOutput)
			}
			if _, err := os.Lstat(fixture.bundle); !os.IsNotExist(err) {
				t.Fatalf("read-only hint created authorization bundle: %v", err)
			}
			if strings.Contains(stdout.String(), "sha256:") || strings.Contains(stdout.String(), testSecretSentinel) {
				t.Fatalf("read-only hint was confused with an authorization or leaked raw data: %q", stdout.String())
			}
		})
	}
}

func TestVerifyRejectsArtifactAndBindingTamper(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, commandFixture)
	}{
		{name: "plan bytes", mutate: func(t *testing.T, fixture commandFixture) {
			appendPrivateFile(t, filepath.Join(fixture.bundle, planFilename), []byte(" "))
		}},
		{name: "decision domain", mutate: func(t *testing.T, fixture commandFixture) {
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) { decision.SelectedDomain = string(releasedomain.DomainImageCache) })
		}},
		{name: "argv binding", mutate: func(t *testing.T, fixture commandFixture) {
			mutateBinding(t, fixture.bundle, func(binding *releasedomain.ExecutionBinding) {
				binding.UpgradeArgvDigest = "sha256:" + strings.Repeat("0", 64)
			})
		}},
		{name: "release name binding", mutate: func(t *testing.T, fixture commandFixture) {
			mutateBinding(t, fixture.bundle, func(binding *releasedomain.ExecutionBinding) { binding.ReleaseName = "other" })
		}},
		{name: "release namespace binding", mutate: func(t *testing.T, fixture commandFixture) {
			mutateBinding(t, fixture.bundle, func(binding *releasedomain.ExecutionBinding) { binding.ReleaseNamespace = "other" })
		}},
		{name: "base revision binding", mutate: func(t *testing.T, fixture commandFixture) {
			mutateBinding(t, fixture.bundle, func(binding *releasedomain.ExecutionBinding) { binding.BaseRevision = "2" })
		}},
		{name: "target revision binding", mutate: func(t *testing.T, fixture commandFixture) {
			mutateBinding(t, fixture.bundle, func(binding *releasedomain.ExecutionBinding) { binding.TargetRevision = "3" })
		}},
		{name: "hooks binding", mutate: func(t *testing.T, fixture commandFixture) {
			mutateBinding(t, fixture.bundle, func(binding *releasedomain.ExecutionBinding) { binding.HooksPolicy = "hooks" })
		}},
		{name: "argv snapshot", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, argvSnapshotFilename)
			data := []byte("helm\x00upgrade\x00different\x00")
			overwritePrivateFile(t, path, data)
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) { decision.Artifacts.UpgradeArgvSnapshot = digestBytes(data) })
		}},
		{name: "rollback evidence", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, rollbackEvidenceFilename)
			var evidence releasedomain.RollbackOwnershipEvidence
			mustDecodeJSON(t, mustReadFile(t, path), &evidence)
			evidence.Domain = releasedomain.DomainControlPlane
			encoded := mustMarshalJSON(t, evidence)
			overwritePrivateFile(t, path, encoded)
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) { decision.Artifacts.RollbackOwnershipEvidence = digestBytes(encoded) })
		}},
		{name: "transaction envelope", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, envelopeFilename)
			var envelope map[string]any
			mustDecodeJSON(t, mustReadFile(t, path), &envelope)
			envelope["expectedDomain"] = string(releasedomain.DomainBackup)
			encoded := mustMarshalJSON(t, envelope)
			overwritePrivateFile(t, path, encoded)
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) { decision.Artifacts.TransactionEnvelope = digestBytes(encoded) })
		}},
		{name: "changed evidence trailing", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, changedEvidenceFilename)
			encoded := append(mustReadFile(t, path), []byte("{}\n")...)
			overwritePrivateFile(t, path, encoded)
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) { decision.Artifacts.ChangedFileEvidence = digestBytes(encoded) })
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := authorizeSingleFixture(t)
			test.mutate(t, fixture)
			assertVerifyRejected(t, fixture.bundle)
		})
	}
}

func TestVerifyRejectsStrictDecisionAndPathAttacks(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, commandFixture)
	}{
		{name: "decision unknown field", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, decisionFilename)
			data := bytes.Replace(mustReadFile(t, path), []byte("{\n"), []byte("{\n  \"unexpected\": true,\n"), 1)
			overwritePrivateFile(t, path, data)
		}},
		{name: "decision duplicate field", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, decisionFilename)
			data := bytes.Replace(mustReadFile(t, path), []byte("{\n"), []byte("{\n  \"kind\": \"ReleaseDomainDispatchDecision\",\n"), 1)
			overwritePrivateFile(t, path, data)
		}},
		{name: "decision wrong field case", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, decisionFilename)
			data := bytes.Replace(mustReadFile(t, path), []byte("\"kind\""), []byte("\"Kind\""), 1)
			overwritePrivateFile(t, path, data)
		}},
		{name: "decision trailing value", mutate: func(t *testing.T, fixture commandFixture) {
			appendPrivateFile(t, filepath.Join(fixture.bundle, decisionFilename), []byte("{}\n"))
		}},
		{name: "decision invalid UTF-8", mutate: func(t *testing.T, fixture commandFixture) {
			overwritePrivateFile(t, filepath.Join(fixture.bundle, decisionFilename), []byte{'{', '"', 0xff, '"', ':', '1', '}'})
		}},
		{name: "unexpected file", mutate: func(t *testing.T, fixture commandFixture) {
			writePrivateFile(t, filepath.Join(fixture.bundle, "unexpected"), []byte("x"))
		}},
		{name: "file mode", mutate: func(t *testing.T, fixture commandFixture) {
			if err := os.Chmod(filepath.Join(fixture.bundle, planFilename), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "file symlink", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, planFilename)
			data := mustReadFile(t, path)
			outside := filepath.Join(fixture.root, "outside-plan")
			writePrivateFile(t, outside, data)
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "file hardlink", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, planFilename)
			if err := os.Link(path, filepath.Join(fixture.root, "second-plan-link")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "bundle symlink", mutate: func(t *testing.T, fixture commandFixture) {
			real := fixture.bundle + "-real"
			if err := os.Rename(fixture.bundle, real); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, fixture.bundle); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "bundle mode", mutate: func(t *testing.T, fixture commandFixture) {
			if err := os.Chmod(fixture.bundle, 0o755); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := authorizeSingleFixture(t)
			test.mutate(t, fixture)
			assertVerifyRejected(t, fixture.bundle)
		})
	}
}

func TestAuthorizeRejectsUnsafeInputsAndHookPolicy(t *testing.T) {
	t.Run("manifest mode", func(t *testing.T) {
		fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
		manifest := flagValue(t, fixture.args, "--base-canonical-manifest")
		if err := os.Chmod(manifest, 0o644); err != nil {
			t.Fatal(err)
		}
		assertAuthorizeRejected(t, fixture)
	})
	t.Run("manifest symlink", func(t *testing.T) {
		fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
		manifest := flagValue(t, fixture.args, "--base-canonical-manifest")
		outside := manifest + "-real"
		if err := os.Rename(manifest, outside); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, manifest); err != nil {
			t.Fatal(err)
		}
		assertAuthorizeRejected(t, fixture)
	})
	t.Run("argv mode", func(t *testing.T) {
		fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
		argv := flagValue(t, fixture.args, "--argv-snapshot")
		if err := os.Chmod(argv, 0o644); err != nil {
			t.Fatal(err)
		}
		assertAuthorizeRejected(t, fixture)
	})
	t.Run("hook true", func(t *testing.T) {
		fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
		for index, argument := range fixture.args {
			if argument == "--ignore-helm-test-hooks=false" {
				fixture.args[index] = "--ignore-helm-test-hooks=true"
			}
		}
		assertAuthorizeRejected(t, fixture)
	})
	t.Run("hook omitted", func(t *testing.T) {
		fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
		filtered := fixture.args[:0]
		for _, argument := range fixture.args {
			if argument != "--ignore-helm-test-hooks=false" {
				filtered = append(filtered, argument)
			}
		}
		fixture.args = filtered
		assertAuthorizeRejected(t, fixture)
	})
	t.Run("non-adjacent revisions", func(t *testing.T) {
		fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
		for index, argument := range fixture.args {
			if argument == "--target-revision" {
				fixture.args[index+1] = "3"
			}
		}
		assertAuthorizeRejected(t, fixture)
	})
	t.Run("zero base revision", func(t *testing.T) {
		fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
		for index, argument := range fixture.args {
			if argument == "--base-revision" {
				fixture.args[index+1] = "0"
			}
			if argument == "--target-revision" {
				fixture.args[index+1] = "1"
			}
		}
		assertAuthorizeRejected(t, fixture)
	})
	t.Run("duplicate scalar flag", func(t *testing.T) {
		fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
		fixture.args = append(fixture.args, "--release-name", "fugue")
		assertAuthorizeRejected(t, fixture)
	})
	for _, test := range []struct {
		name    string
		binding string
	}{
		{name: "conflicting release name binding", binding: "releaseName=other"},
		{name: "conflicting release namespace binding", binding: "releaseNamespace=other"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
			fixture.args = append(fixture.args, "--binding", test.binding)
			assertAuthorizeRejected(t, fixture)
		})
	}
}

func TestAuthorizeRejectsNonCanonicalUpgradeArgvSnapshot(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "not NUL terminated", mutate: func(data []byte) []byte { return data[:len(data)-1] }},
		{name: "missing no hooks", mutate: func(data []byte) []byte { return bytes.Replace(data, []byte("--no-hooks\x00"), nil, 1) }},
		{name: "duplicate no hooks", mutate: func(data []byte) []byte {
			return append(append([]byte(nil), data...), []byte("--no-hooks\x00")...)
		}},
		{name: "wrong release", mutate: func(data []byte) []byte {
			return bytes.Replace(data, []byte("fugue\x00chart"), []byte("other\x00chart"), 1)
		}},
		{name: "wrong namespace", mutate: func(data []byte) []byte {
			return bytes.Replace(data, []byte("fugue-system\x00"), []byte("other\x00"), 1)
		}},
		{name: "flag-like chart", mutate: func(data []byte) []byte {
			return bytes.Replace(data, []byte("fugue\x00chart\x00"), []byte("fugue\x00--chart\x00"), 1)
		}},
		{name: "string flag swallows no hooks", mutate: func(data []byte) []byte {
			return bytes.Replace(
				data,
				[]byte("--reset-then-reuse-values\x00--no-hooks\x00"),
				[]byte("--reset-then-reuse-values\x00--description\x00--no-hooks\x00"),
				1,
			)
		}},
		{name: "attached namespace equals", mutate: func(data []byte) []byte {
			return append(append([]byte(nil), data...), []byte("-n=other\x00")...)
		}},
		{name: "attached namespace shorthand", mutate: func(data []byte) []byte {
			return append(append([]byte(nil), data...), []byte("-nother\x00")...)
		}},
		{name: "combined shorthand overrides namespace", mutate: func(data []byte) []byte {
			return append(append([]byte(nil), data...), []byte("-inother\x00")...)
		}},
		{name: "empty argument", mutate: func(data []byte) []byte {
			return bytes.Replace(data, []byte("chart\x00-n"), []byte("chart\x00\x00-n"), 1)
		}},
		{name: "invalid UTF-8", mutate: func(data []byte) []byte { return bytes.Replace(data, []byte("chart"), []byte{'c', 0xff}, 1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
			argv := flagValue(t, fixture.args, "--argv-snapshot")
			overwritePrivateFile(t, argv, test.mutate(mustReadFile(t, argv)))
			assertAuthorizeRejected(t, fixture)
		})
	}
}

func TestAuthorizeAcceptsCanonicalValuesFileShorthand(t *testing.T) {
	fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
	argvPath := flagValue(t, fixture.args, "--argv-snapshot")
	argv := append(mustReadFile(t, argvPath), []byte("-f\x00/secure/override-values.yaml\x00")...)
	overwritePrivateFile(t, argvPath, argv)
	var stdout, stderr bytes.Buffer
	if got := run(fixture.args, &stdout, &stderr); got != 0 {
		t.Fatalf("authorize exit = %d, stderr = %q", got, stderr.String())
	}
}

func TestVerifyRejectsAttachedNamespaceShorthandAfterDigestRebinding(t *testing.T) {
	for _, attached := range []string{"-n=other", "-nother", "-inother"} {
		t.Run(attached, func(t *testing.T) {
			fixture := authorizeSingleFixture(t)
			argvPath := filepath.Join(fixture.bundle, argvSnapshotFilename)
			argv := append(mustReadFile(t, argvPath), []byte(attached+"\x00")...)
			overwritePrivateFile(t, argvPath, argv)
			argvDigest := digestBytes(argv)
			mutateBinding(t, fixture.bundle, func(binding *releasedomain.ExecutionBinding) {
				binding.UpgradeArgvDigest = argvDigest
			})
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) {
				decision.Artifacts.UpgradeArgvSnapshot = argvDigest
			})
			assertVerifyRejected(t, fixture.bundle)
		})
	}
}

func TestVerifyRejectsNoHooksConsumedAsStringFlagAfterDigestRebinding(t *testing.T) {
	fixture := authorizeSingleFixture(t)
	argvPath := filepath.Join(fixture.bundle, argvSnapshotFilename)
	argv := bytes.Replace(
		mustReadFile(t, argvPath),
		[]byte("--reset-then-reuse-values\x00--no-hooks\x00"),
		[]byte("--reset-then-reuse-values\x00--description\x00--no-hooks\x00"),
		1,
	)
	overwritePrivateFile(t, argvPath, argv)
	argvDigest := digestBytes(argv)
	mutateBinding(t, fixture.bundle, func(binding *releasedomain.ExecutionBinding) {
		binding.UpgradeArgvDigest = argvDigest
	})
	mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) {
		decision.Artifacts.UpgradeArgvSnapshot = argvDigest
	})
	assertVerifyRejected(t, fixture.bundle)
}

func TestVerifyUsesSealedBundleArgvAsOnlyExecutableSource(t *testing.T) {
	fixture := authorizeSingleFixture(t)
	source := flagValue(t, fixture.args, "--argv-snapshot")
	sealed := filepath.Join(fixture.bundle, argvSnapshotFilename)
	want := mustReadFile(t, sealed)
	if !bytes.Equal(mustReadFile(t, source), want) {
		t.Fatal("authorized bundle did not preserve the exact argv snapshot")
	}

	// The original renderer snapshot is discarded after authorize. Verify and
	// the production Apply boundary consume only the sealed bundle copy.
	overwritePrivateFile(t, source, []byte("helm\x00upgrade\x00other\x00chart\x00-n\x00other\x00--no-hooks\x00"))
	var stdout, stderr bytes.Buffer
	if got := run([]string{"verify", "--bundle-dir", fixture.bundle}, &stdout, &stderr); got != 0 {
		t.Fatalf("verify sealed argv exit = %d, stderr = %s", got, stderr.String())
	}
	if !bytes.Equal(mustReadFile(t, sealed), want) {
		t.Fatal("discarded source mutated the sealed executable argv")
	}
}

func TestVerifyRejectsTamperedDecisionDigests(t *testing.T) {
	fields := []struct {
		name   string
		mutate func(*dispatchDecision)
	}{
		{name: "plan digest", mutate: func(decision *dispatchDecision) { decision.PlanDigest = "sha256:" + strings.Repeat("0", 64) }},
		{name: "base commit", mutate: func(decision *dispatchDecision) { decision.TrustedBaseCommit = strings.Repeat("3", 40) }},
		{name: "ownership digest", mutate: func(decision *dispatchDecision) { decision.Artifacts.Ownership = "sha256:" + strings.Repeat("0", 64) }},
		{name: "argv digest", mutate: func(decision *dispatchDecision) {
			decision.Artifacts.UpgradeArgvSnapshot = "sha256:" + strings.Repeat("0", 64)
		}},
	}
	for _, test := range fields {
		t.Run(test.name, func(t *testing.T) {
			fixture := authorizeSingleFixture(t)
			mutateDecision(t, fixture.bundle, test.mutate)
			assertVerifyRejected(t, fixture.bundle)
		})
	}
}

func authorizeSingleFixture(t *testing.T) commandFixture {
	t.Helper()
	fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
	var stdout, stderr bytes.Buffer
	if got := run(fixture.args, &stdout, &stderr); got != 0 {
		t.Fatalf("authorize exit = %d, stderr = %s", got, stderr.String())
	}
	return fixture
}

func newCommandFixture(t *testing.T, domains []releasedomain.Domain, outcome releasedomain.Outcome) commandFixture {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	baseCommit := strings.Repeat("1", 40)
	targetCommit := strings.Repeat("2", 40)
	ownership := testOwnership(t)
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatal(err)
	}

	manifestDomains := domains
	changes := make([]releasedomain.ChangedFile, 0)
	switch outcome {
	case releasedomain.OutcomeZero:
		manifestDomains = []releasedomain.Domain{releasedomain.DomainNodeLocal}
		changes = append(changes, releasedomain.ChangedFile{Status: releasedomain.ChangeModified, Path: "docs/only.md"})
	case releasedomain.OutcomeUnknown:
		manifestDomains = []releasedomain.Domain{releasedomain.DomainNodeLocal}
		changes = append(changes, releasedomain.ChangedFile{Status: releasedomain.ChangeModified, Path: "unowned/runtime.bin"})
	default:
		for _, domain := range domains {
			changes = append(changes, releasedomain.ChangedFile{Status: releasedomain.ChangeModified, Path: "change/" + string(domain)})
		}
	}
	base := canonicalTestManifest(t, spec, manifestDomains, "base-"+testSecretSentinel)
	targetVersion := "target-" + testSecretSentinel
	if outcome == releasedomain.OutcomeZero || outcome == releasedomain.OutcomeUnknown {
		targetVersion = "base-" + testSecretSentinel
	}
	target := canonicalTestManifest(t, spec, manifestDomains, targetVersion)
	evidence := testChangedEvidence(t, baseCommit, targetCommit, changes)

	paths := map[string]string{
		"ownership": filepath.Join(root, "ownership.yaml"),
		"evidence":  filepath.Join(root, "changed.json"),
		"base":      filepath.Join(root, "base.yaml"),
		"target":    filepath.Join(root, "target.yaml"),
		"repeat":    filepath.Join(root, "repeat.yaml"),
		"argv":      filepath.Join(root, "argv.snapshot"),
	}
	writePrivateFile(t, paths["ownership"], ownership)
	writePrivateFile(t, paths["evidence"], evidence)
	writePrivateFile(t, paths["base"], base)
	writePrivateFile(t, paths["target"], target)
	writePrivateFile(t, paths["repeat"], target)
	writePrivateFile(t, paths["argv"], []byte("helm\x00upgrade\x00fugue\x00chart\x00-n\x00fugue-system\x00--reset-then-reuse-values\x00--no-hooks\x00--set-string\x00value="+testSecretSentinel+"\x00"))
	bundle := filepath.Join(root, "authorization-bundle")
	args := []string{
		"authorize",
		"--ownership", paths["ownership"],
		"--changed-evidence", paths["evidence"],
		"--trusted-base-commit", baseCommit,
		"--trusted-target-commit", targetCommit,
		"--base-canonical-manifest", paths["base"],
		"--target-canonical-manifest", paths["target"],
		"--repeated-target-canonical-manifest", paths["repeat"],
		"--argv-snapshot", paths["argv"],
		"--bundle-dir", bundle,
		"--release-name", "fugue",
		"--release-namespace", "fugue-system",
		"--base-revision", "1",
		"--target-revision", "2",
		"--ignore-helm-test-hooks=false",
	}
	return commandFixture{root: root, bundle: bundle, args: args, baseCommit: baseCommit, targetCommit: targetCommit}
}

func testOwnership(t *testing.T) []byte {
	t.Helper()
	spec := releasedomain.OwnershipSpec{
		APIVersion:       releasedomain.OwnershipAPIVersion,
		Kind:             releasedomain.OwnershipKind,
		Domains:          releasedomain.KnownDomains(),
		RequiredBindings: []string{"releaseName", "releaseNamespace"},
		FileRules: []releasedomain.FileRule{{
			ID: "non-runtime-docs", Exact: "docs/only.md", NonRuntime: true,
		}},
	}
	for _, domain := range releasedomain.KnownDomains() {
		spec.FileRules = append(spec.FileRules, releasedomain.FileRule{
			ID: "file-" + string(domain), Exact: "change/" + string(domain), Domains: []releasedomain.Domain{domain},
		})
		spec.ObjectRules = append(spec.ObjectRules, releasedomain.ObjectRule{
			ID:        "object-" + string(domain),
			Domain:    domain,
			Version:   "v1",
			Kind:      "ConfigMap",
			Scope:     releasedomain.ScopeNamespaced,
			Namespace: "${releaseNamespace}",
			Name:      "fixture-" + string(domain),
			RequiredLabels: map[string]string{
				"test.fugue.dev/domain": string(domain),
			},
		})
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return append(encoded, '\n')
}

func canonicalTestManifest(t *testing.T, spec *releasedomain.OwnershipSpec, domains []releasedomain.Domain, version string) []byte {
	t.Helper()
	var raw strings.Builder
	for index, domain := range domains {
		if index != 0 {
			raw.WriteString("---\n")
		}
		fmt.Fprintf(&raw, `apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture-%s
  namespace: fugue-system
  labels:
    test.fugue.dev/domain: %s
data:
  version: %s
`, domain, domain, version)
	}
	canonical, err := releasedomain.CanonicalizeRenderedManifest([]byte(raw.String()), spec, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

func testChangedEvidence(t *testing.T, baseCommit, targetCommit string, changes []releasedomain.ChangedFile) []byte {
	t.Helper()
	payload := testEvidencePayload{
		APIVersion:   releasedomain.ChangedFileEvidenceAPIVersion,
		Kind:         releasedomain.ChangedFileEvidenceKind,
		Policy:       releasedomain.ChangedFileEvidencePolicy,
		BaseCommit:   baseCommit,
		TargetCommit: targetCommit,
		Changes:      changes,
	}
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encodedPayload)
	document := testEvidenceDocument{
		APIVersion:   payload.APIVersion,
		Kind:         payload.Kind,
		Policy:       payload.Policy,
		BaseCommit:   payload.BaseCommit,
		TargetCommit: payload.TargetCommit,
		Changes:      payload.Changes,
		Digest:       "sha256:" + hex.EncodeToString(digest[:]),
	}
	return mustMarshalJSON(t, document)
}

func assertFixedResult(t *testing.T, output, outcome, domain string) {
	t.Helper()
	fields := strings.Split(strings.TrimSuffix(output, "\n"), "\t")
	wantLength := 2
	if domain != "" {
		wantLength = 3
	}
	if len(fields) != wantLength || fields[0] != outcome {
		t.Fatalf("fixed output = %q", output)
	}
	if domain != "" && fields[1] != domain {
		t.Fatalf("fixed output domain = %q", output)
	}
	digest := fields[len(fields)-1]
	if err := validateCanonicalDigest(digest); err != nil {
		t.Fatalf("fixed output digest = %q: %v", digest, err)
	}
}

func assertBundleModes(t *testing.T, bundle string, expected map[string]struct{}) {
	t.Helper()
	info, err := os.Lstat(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("bundle mode = %v", info.Mode())
	}
	entries, err := os.ReadDir(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(expected) {
		t.Fatalf("bundle entries = %d, want %d", len(entries), len(expected))
	}
	for _, entry := range entries {
		if _, ok := expected[entry.Name()]; !ok {
			t.Fatalf("unexpected bundle entry %s", entry.Name())
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("bundle file %s mode = %v", entry.Name(), info.Mode())
		}
	}
}

func assertVerifyRejected(t *testing.T, bundle string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if got := run([]string{"verify", "--bundle-dir", bundle}, &stdout, &stderr); got != 1 {
		t.Fatalf("tampered verify exit = %d, stdout = %q, stderr = %s", got, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("tampered verify emitted stdout %q", stdout.String())
	}
}

func assertAuthorizeRejected(t *testing.T, fixture commandFixture) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if got := run(fixture.args, &stdout, &stderr); got != 1 {
		t.Fatalf("unsafe authorize exit = %d, stdout = %q, stderr = %s", got, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("unsafe authorize emitted stdout %q", stdout.String())
	}
	if _, err := os.Lstat(fixture.bundle); !os.IsNotExist(err) {
		t.Fatalf("unsafe authorize created a bundle: %v", err)
	}
}

func mutateDecision(t *testing.T, bundle string, mutate func(*dispatchDecision)) {
	t.Helper()
	path := filepath.Join(bundle, decisionFilename)
	var decision dispatchDecision
	mustDecodeJSON(t, mustReadFile(t, path), &decision)
	mutate(&decision)
	overwritePrivateFile(t, path, mustMarshalJSON(t, decision))
}

func mutateBinding(t *testing.T, bundle string, mutate func(*releasedomain.ExecutionBinding)) {
	t.Helper()
	path := filepath.Join(bundle, executionBindingFilename)
	var binding releasedomain.ExecutionBinding
	mustDecodeJSON(t, mustReadFile(t, path), &binding)
	mutate(&binding)
	encoded := mustMarshalJSON(t, binding)
	overwritePrivateFile(t, path, encoded)
	mutateDecision(t, bundle, func(decision *dispatchDecision) {
		decision.Artifacts.ExecutionBinding = digestBytes(encoded)
	})
}

func flagValue(t *testing.T, args []string, name string) string {
	t.Helper()
	for index, argument := range args {
		if argument == name && index+1 < len(args) {
			return args[index+1]
		}
	}
	t.Fatalf("flag %s missing", name)
	return ""
}

func writePrivateFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}

func overwritePrivateFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}

func appendPrivateFile(t *testing.T, path string, suffix []byte) {
	t.Helper()
	data := append(mustReadFile(t, path), suffix...)
	overwritePrivateFile(t, path, data)
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustMarshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := marshalPrivateJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustDecodeJSON(t *testing.T, data []byte, destination any) {
	t.Helper()
	if err := json.Unmarshal(data, destination); err != nil {
		t.Fatal(err)
	}
}

func TestExpectedBundleFileSetsDoNotDrift(t *testing.T) {
	common := expectedBundleFiles(releasedomain.OutcomeZero)
	single := expectedBundleFiles(releasedomain.OutcomeSingle)
	if len(single) != len(common)+3 {
		t.Fatalf("single/common bundle file counts = %d/%d", len(single), len(common))
	}
	for name := range common {
		if _, ok := single[name]; !ok {
			t.Fatalf("single bundle omits common file %s", name)
		}
	}
	if reflect.DeepEqual(single, common) {
		t.Fatal("single and zero bundle sets unexpectedly equal")
	}
}

func TestAuthorizePrivateArtifactFailuresAreRedacted(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, commandFixture)
	}{
		{name: "changed evidence schema", mutate: func(t *testing.T, fixture commandFixture) {
			path := flagValue(t, fixture.args, "--changed-evidence")
			overwritePrivateFile(t, path, bytes.Replace(mustReadFile(t, path), []byte(releasedomain.ChangedFileEvidenceAPIVersion), []byte(testSecretSentinel), 1))
		}},
		{name: "ownership schema", mutate: func(t *testing.T, fixture commandFixture) {
			path := flagValue(t, fixture.args, "--ownership")
			overwritePrivateFile(t, path, bytes.Replace(mustReadFile(t, path), []byte(releasedomain.OwnershipAPIVersion), []byte(testSecretSentinel), 1))
		}},
		{name: "manifest payload", mutate: func(t *testing.T, fixture commandFixture) {
			overwritePrivateFile(t, flagValue(t, fixture.args, "--base-canonical-manifest"), []byte(testSecretSentinel))
		}},
		{name: "argv payload", mutate: func(t *testing.T, fixture commandFixture) {
			path := flagValue(t, fixture.args, "--argv-snapshot")
			overwritePrivateFile(t, path, append(mustReadFile(t, path), []byte("-n="+testSecretSentinel+"\x00")...))
		}},
		{name: "flag payload", mutate: func(_ *testing.T, fixture commandFixture) {
			fixture.args[len(fixture.args)-1] = "--" + testSecretSentinel
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
			test.mutate(t, fixture)
			assertFixedPrivateFailure(t, fixture.args, "fugue-release-domain-dispatch: authorize failed\n", fixture.root)
		})
	}
}

func TestVerifyPrivateBundleFailuresAreRedacted(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, commandFixture)
	}{
		{name: "decision strict JSON", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, decisionFilename)
			data := bytes.Replace(mustReadFile(t, path), []byte("{\n"), []byte("{\n  \""+testSecretSentinel+"\": \""+testSecretSentinel+"\",\n"), 1)
			overwritePrivateFile(t, path, data)
		}},
		{name: "changed evidence schema", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, changedEvidenceFilename)
			data := bytes.Replace(mustReadFile(t, path), []byte(releasedomain.ChangedFileEvidenceAPIVersion), []byte(testSecretSentinel), 1)
			overwritePrivateFile(t, path, data)
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) { decision.Artifacts.ChangedFileEvidence = digestBytes(data) })
		}},
		{name: "ownership schema", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, ownershipFilename)
			data := bytes.Replace(mustReadFile(t, path), []byte(releasedomain.OwnershipAPIVersion), []byte(testSecretSentinel), 1)
			overwritePrivateFile(t, path, data)
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) { decision.Artifacts.Ownership = digestBytes(data) })
		}},
		{name: "manifest payload", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, baseManifestFilename)
			data := []byte(testSecretSentinel)
			overwritePrivateFile(t, path, data)
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) { decision.Artifacts.BaseManifest = digestBytes(data) })
		}},
		{name: "argv payload", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, argvSnapshotFilename)
			data := append(mustReadFile(t, path), []byte("-n="+testSecretSentinel+"\x00")...)
			overwritePrivateFile(t, path, data)
			digest := digestBytes(data)
			mutateBinding(t, fixture.bundle, func(binding *releasedomain.ExecutionBinding) { binding.UpgradeArgvDigest = digest })
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) { decision.Artifacts.UpgradeArgvSnapshot = digest })
		}},
		{name: "envelope strict JSON", mutate: func(t *testing.T, fixture commandFixture) {
			path := filepath.Join(fixture.bundle, envelopeFilename)
			data := bytes.Replace(mustReadFile(t, path), []byte("{\n"), []byte("{\n  \""+testSecretSentinel+"\": \""+testSecretSentinel+"\",\n"), 1)
			overwritePrivateFile(t, path, data)
			mutateDecision(t, fixture.bundle, func(decision *dispatchDecision) { decision.Artifacts.TransactionEnvelope = digestBytes(data) })
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := authorizeSingleFixture(t)
			test.mutate(t, fixture)
			assertFixedPrivateFailure(
				t,
				[]string{"verify", "--bundle-dir", fixture.bundle},
				"fugue-release-domain-dispatch: verify failed\n",
				fixture.root,
			)
		})
	}
}

func TestClassifyFilesPrivateArtifactFailuresAreRedacted(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, commandFixture, *[]string)
	}{
		{name: "changed evidence schema", mutate: func(t *testing.T, fixture commandFixture, _ *[]string) {
			path := flagValue(t, fixture.args, "--changed-evidence")
			overwritePrivateFile(t, path, bytes.Replace(mustReadFile(t, path), []byte(releasedomain.ChangedFileEvidenceAPIVersion), []byte(testSecretSentinel), 1))
		}},
		{name: "ownership schema", mutate: func(t *testing.T, fixture commandFixture, _ *[]string) {
			path := flagValue(t, fixture.args, "--ownership")
			overwritePrivateFile(t, path, bytes.Replace(mustReadFile(t, path), []byte(releasedomain.OwnershipAPIVersion), []byte(testSecretSentinel), 1))
		}},
		{name: "flag payload", mutate: func(_ *testing.T, _ commandFixture, args *[]string) {
			*args = append(*args, "--"+testSecretSentinel)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCommandFixture(t, []releasedomain.Domain{releasedomain.DomainNodeLocal}, releasedomain.OutcomeSingle)
			args := []string{
				"classify-files",
				"--ownership", flagValue(t, fixture.args, "--ownership"),
				"--changed-evidence", flagValue(t, fixture.args, "--changed-evidence"),
				"--trusted-base-commit", fixture.baseCommit,
				"--trusted-target-commit", fixture.targetCommit,
			}
			test.mutate(t, fixture, &args)
			assertFixedPrivateFailure(t, args, "fugue-release-domain-dispatch: classify-files failed\n", fixture.root)
		})
	}
}

func assertFixedPrivateFailure(t *testing.T, args []string, wantStderr string, forbiddenPath string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if got := run(args, &stdout, &stderr); got != 1 {
		t.Fatalf("private failure exit = %d, stdout = %q, stderr = %q", got, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 || stderr.String() != wantStderr {
		t.Fatalf("private failure output = %q / %q, want empty / %q", stdout.String(), stderr.String(), wantStderr)
	}
	for _, forbidden := range []string{testSecretSentinel, forbiddenPath, "apiVersion", "ownership.yaml", "base-manifest", "argv.snapshot"} {
		if forbidden != "" && strings.Contains(stderr.String(), forbidden) {
			t.Fatalf("private failure leaked %q in stderr %q", forbidden, stderr.String())
		}
	}
}
