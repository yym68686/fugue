package releaseevidence

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/releasedomain"

	"golang.org/x/sys/unix"
)

const publicEvidenceSentinel = "PRIVATE-SENTINEL-/workspace/secret --token=value"

func TestValidateAcceptsCanonicalArtifacts(t *testing.T) {
	tests := map[string]PublicArtifact{
		"single success": validSingleArtifact(),
		"independent changed evidence digest": func() PublicArtifact {
			artifact := validSingleArtifact()
			artifact.ChangedEvidenceDigest = digestOf('b')
			return artifact
		}(),
		"single rollback": validRollbackArtifact(),
		"zero":            validNoWriteArtifact(OutcomeZero, 17, 17),
		"multiple":        validNoWriteArtifact(OutcomeMultiple, 17, 17),
		"unknown": func() PublicArtifact {
			artifact := validNoWriteArtifact(OutcomeUnknown, 17, 17)
			artifact.RepeatDigest = digestOf('b')
			return artifact
		}(),
		"genesis zero": validNoWriteArtifact(OutcomeGenesisZero, 0, 0),
	}
	for name, artifact := range tests {
		t.Run(name, func(t *testing.T) {
			if err := Validate(artifact); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestValidateRejectsNonCanonicalArtifacts(t *testing.T) {
	tests := map[string]func(*PublicArtifact){
		"api version":         func(a *PublicArtifact) { a.APIVersion = "v2" },
		"kind":                func(a *PublicArtifact) { a.Kind = "Other" },
		"empty run id":        func(a *PublicArtifact) { a.RunID = "" },
		"leading zero run id": func(a *PublicArtifact) { a.RunID = "001" },
		"non-decimal run id":  func(a *PublicArtifact) { a.RunID = "1a" },
		"zero run attempt":    func(a *PublicArtifact) { a.RunAttempt = 0 },
		"uppercase head sha":  func(a *PublicArtifact) { a.HeadSHA = strings.Repeat("A", 40) },
		"short head sha":      func(a *PublicArtifact) { a.HeadSHA = strings.Repeat("a", 39) },
		"unsupported outcome": func(a *PublicArtifact) { a.Outcome = "other" },
		"single revision gap": func(a *PublicArtifact) { a.HelmTargetRevision++ },
		"single missing base": func(a *PublicArtifact) { a.HelmBaseRevision, a.HelmTargetRevision = 0, 1 },
		"single revision overflow": func(a *PublicArtifact) {
			a.HelmBaseRevision, a.HelmTargetRevision = ^uint64(0), 0
		},
		"single no domain":        func(a *PublicArtifact) { a.Domain = "" },
		"single unknown domain":   func(a *PublicArtifact) { a.Domain = "shared" },
		"bad plan digest":         func(a *PublicArtifact) { a.PlanDigest = strings.Repeat("a", 64) },
		"uppercase digest":        func(a *PublicArtifact) { a.ContextDigest = "sha256:" + strings.Repeat("A", 64) },
		"nondeterministic single": func(a *PublicArtifact) { a.RepeatDigest = digestOf('b') },
		"asymmetric reverse":      func(a *PublicArtifact) { a.ReverseDigest = digestOf('b') },
		"asymmetric forward":      func(a *PublicArtifact) { a.ForwardDigest = digestOf('b') },
		"reverse wrong domain":    func(a *PublicArtifact) { a.ReverseDomain = releasedomain.DomainBackup },
		"reverse zero objects":    func(a *PublicArtifact) { a.ReverseObjectCount = 0 },
		"write without reverse": func(a *PublicArtifact) {
			a.ReverseAuthorized = false
			a.ReverseDomain = ""
			a.ReverseObjectCount = 0
		},
		"rollback result conflict": func(a *PublicArtifact) {
			a.RollbackAttempted = true
			a.RollbackCompleted = true
			a.RollbackFailed = true
			a.Trace = rollbackTrace(TraceStateSucceeded)
		},
		"rollback without attempt": func(a *PublicArtifact) { a.RollbackCompleted = true },
		"nil trace":                func(a *PublicArtifact) { a.Trace = nil },
		"trace wrong first event": func(a *PublicArtifact) {
			a.Trace[0] = TraceEvent{Phase: TracePhasePrepare, State: TraceStateStarted}
		},
		"trace unknown phase": func(a *PublicArtifact) {
			a.Trace[1].Phase = "command"
		},
		"trace unknown state": func(a *PublicArtifact) {
			a.Trace[1].State = "pending"
		},
		"trace apply before prepare": func(a *PublicArtifact) {
			a.Trace = []TraceEvent{
				{Phase: TracePhaseTransaction, State: TraceStateStarted},
				{Phase: TracePhaseApply, State: TraceStateStarted},
			}
		},
		"trace after terminal": func(a *PublicArtifact) {
			a.Trace = append(a.Trace, TraceEvent{Phase: TracePhaseRollback, State: TraceStateStarted})
		},
		"trace missing transaction terminal": func(a *PublicArtifact) {
			a.Trace = a.Trace[:len(a.Trace)-1]
		},
		"write flag without apply trace": func(a *PublicArtifact) {
			a.Trace = []TraceEvent{
				{Phase: TracePhaseTransaction, State: TraceStateStarted},
				{Phase: TracePhaseTransaction, State: TraceStateFailed},
			}
		},
		"rollback flags without rollback trace": func(a *PublicArtifact) {
			a.RollbackAttempted = true
			a.RollbackCompleted = true
		},
		"failed after apply without rollback": func(a *PublicArtifact) {
			a.Trace[len(a.Trace)-1].State = TraceStateFailed
		},
		"empty trace with write boundary": func(a *PublicArtifact) {
			a.Trace = []TraceEvent{}
		},
		"apply lacks terminal before rollback": func(a *PublicArtifact) {
			a.RollbackAttempted = true
			a.RollbackCompleted = true
			a.Trace = []TraceEvent{
				{Phase: TracePhaseTransaction, State: TraceStateStarted},
				{Phase: TracePhasePrepare, State: TraceStateStarted},
				{Phase: TracePhasePrepare, State: TraceStateSucceeded},
				{Phase: TracePhaseApply, State: TraceStateStarted},
				{Phase: TracePhaseRollback, State: TraceStateStarted},
				{Phase: TracePhaseRollback, State: TraceStateSucceeded},
				{Phase: TracePhaseTransaction, State: TraceStateFailed},
			}
		},
		"verify lacks terminal before rollback": func(a *PublicArtifact) {
			a.RollbackAttempted = true
			a.RollbackCompleted = true
			a.Trace = []TraceEvent{
				{Phase: TracePhaseTransaction, State: TraceStateStarted},
				{Phase: TracePhasePrepare, State: TraceStateStarted},
				{Phase: TracePhasePrepare, State: TraceStateSucceeded},
				{Phase: TracePhaseApply, State: TraceStateStarted},
				{Phase: TracePhaseApply, State: TraceStateSucceeded},
				{Phase: TracePhaseVerify, State: TraceStateStarted},
				{Phase: TracePhaseRollback, State: TraceStateStarted},
				{Phase: TracePhaseRollback, State: TraceStateSucceeded},
				{Phase: TracePhaseTransaction, State: TraceStateFailed},
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			artifact := validSingleArtifact()
			mutate(&artifact)
			if err := Validate(artifact); err == nil {
				t.Fatal("Validate() unexpectedly succeeded")
			}
		})
	}
}

func TestValidateRejectsNonWritingContradictions(t *testing.T) {
	tests := map[string]func(*PublicArtifact){
		"zero revision changed": func(a *PublicArtifact) { a.HelmTargetRevision++ },
		"zero missing base":     func(a *PublicArtifact) { a.HelmBaseRevision, a.HelmTargetRevision = 0, 0 },
		"non-single domain":     func(a *PublicArtifact) { a.Domain = releasedomain.DomainNodeLocal },
		"non-single reverse": func(a *PublicArtifact) {
			a.ReverseAuthorized = true
			a.ReverseDomain = releasedomain.DomainNodeLocal
			a.ReverseObjectCount = 1
		},
		"non-single write": func(a *PublicArtifact) { a.WriteBoundaryCrossed = true },
		"non-single trace": func(a *PublicArtifact) {
			a.Trace = []TraceEvent{{Phase: TracePhaseTransaction, State: TraceStateStarted}}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			artifact := validNoWriteArtifact(OutcomeZero, 17, 17)
			mutate(&artifact)
			if err := Validate(artifact); err == nil {
				t.Fatal("Validate() unexpectedly succeeded")
			}
		})
	}
}

func TestDecodeStrictRoundTrip(t *testing.T) {
	want := validSingleArtifact()
	encoded := mustMarshalArtifact(t, want)
	got, err := Decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Decode() = %#v, want %#v", got, want)
	}

	var nilBuffer *bytes.Buffer
	if _, err := Decode(nilBuffer); err == nil {
		t.Fatal("Decode(typed nil) unexpectedly succeeded")
	}
}

func TestDecodeRejectsAmbiguousOrMalformedJSON(t *testing.T) {
	valid := string(mustMarshalArtifact(t, validSingleArtifact()))
	tests := map[string][]byte{
		"unknown": []byte(strings.Replace(valid, "{", `{"reason":"`+publicEvidenceSentinel+`",`, 1)),
		"duplicate root": []byte(strings.Replace(
			valid,
			`"runId":"123456789"`,
			`"runId":"123456789","runId":"987654321"`,
			1,
		)),
		"escaped duplicate root": []byte(strings.Replace(
			valid,
			`"runId":"123456789"`,
			`"runId":"123456789","run\u0049d":"987654321"`,
			1,
		)),
		"duplicate nested": []byte(strings.Replace(
			valid,
			`"phase":"transaction","state":"started"`,
			`"phase":"transaction","phase":"prepare","state":"started"`,
			1,
		)),
		"null root":        []byte("null"),
		"null string":      []byte(strings.Replace(valid, `"runId":"123456789"`, `"runId":null`, 1)),
		"null integer":     []byte(strings.Replace(valid, `"runAttempt":2`, `"runAttempt":null`, 1)),
		"null boolean":     []byte(strings.Replace(valid, `"reverseAuthorized":true`, `"reverseAuthorized":null`, 1)),
		"null trace":       []byte(strings.Replace(valid, `"trace":[`, `"trace":null,"discarded":[`, 1)),
		"missing required": []byte(strings.Replace(valid, `"kind":"ReleaseDomainPublicEvidence",`, "", 1)),
		"trailing object":  []byte(valid + `{}`),
		"decimal integer":  []byte(strings.Replace(valid, `"runAttempt":2`, `"runAttempt":2.0`, 1)),
		"exponent integer": []byte(strings.Replace(valid, `"runAttempt":2`, `"runAttempt":2e0`, 1)),
		"negative integer": []byte(strings.Replace(valid, `"runAttempt":2`, `"runAttempt":-2`, 1)),
		"isolated surrogate": []byte(strings.Replace(
			valid,
			`"runId":"123456789"`,
			`"runId":"\ud800"`,
			1,
		)),
		"invalid utf8": append([]byte(`{"apiVersion":"`), 0xff),
		"oversize":     bytes.Repeat([]byte{' '}, maxPublicArtifactBytes+1),
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Decode(bytes.NewReader(data))
			if err == nil {
				t.Fatal("Decode() unexpectedly succeeded")
			}
			if strings.Contains(err.Error(), publicEvidenceSentinel) {
				t.Fatalf("Decode() error leaked sentinel: %v", err)
			}
		})
	}
}

func TestPublicArtifactSchemaCannotRepresentFreeText(t *testing.T) {
	expectedFields := map[string]struct{}{
		"apiVersion": {}, "kind": {}, "runId": {}, "runAttempt": {}, "headSHA": {},
		"helmBaseRevision": {}, "helmTargetRevision": {}, "outcome": {}, "domain": {},
		"planDigest": {}, "changedEvidenceDigest": {}, "baseDigest": {}, "targetDigest": {},
		"repeatDigest": {}, "ownershipDigest": {}, "contextDigest": {}, "forwardDigest": {}, "reverseDigest": {},
		"reverseAuthorized": {}, "reverseDomain": {}, "reverseObjectCount": {}, "trace": {},
		"writeBoundaryCrossed": {}, "rollbackAttempted": {}, "rollbackCompleted": {}, "rollbackFailed": {},
	}
	typeOfArtifact := reflect.TypeOf(PublicArtifact{})
	if typeOfArtifact.NumField() != len(expectedFields) {
		t.Fatalf("PublicArtifact has %d fields, want %d fixed fields", typeOfArtifact.NumField(), len(expectedFields))
	}
	for index := 0; index < typeOfArtifact.NumField(); index++ {
		field := typeOfArtifact.Field(index)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if _, ok := expectedFields[name]; !ok {
			t.Errorf("unexpected public schema field %q", name)
		}
		if field.Type.Kind() == reflect.Map || field.Type.Kind() == reflect.Interface {
			t.Errorf("public schema field %q can carry arbitrary data", name)
		}
	}
	traceType := reflect.TypeOf(TraceEvent{})
	if traceType.NumField() != 2 || traceType.Field(0).Tag.Get("json") != "phase" || traceType.Field(1).Tag.Get("json") != "state" {
		t.Fatalf("TraceEvent schema is not the fixed phase/state pair: %#v", traceType)
	}
}

func TestSentinelCannotEnterValidatedOrEncodedPublicEvidence(t *testing.T) {
	base := validSingleArtifact()
	valueType := reflect.TypeOf(base)
	for index := 0; index < valueType.NumField(); index++ {
		field := valueType.Field(index)
		if field.Type.Kind() != reflect.String {
			continue
		}
		t.Run(field.Name, func(t *testing.T) {
			mutated := base
			reflect.ValueOf(&mutated).Elem().Field(index).SetString(publicEvidenceSentinel)
			err := Validate(mutated)
			if err == nil {
				t.Fatal("sentinel-bearing artifact unexpectedly validated")
			}
			if strings.Contains(err.Error(), publicEvidenceSentinel) {
				t.Fatalf("validation error leaked sentinel: %v", err)
			}
		})
	}
	for name, mutate := range map[string]func(*PublicArtifact){
		"trace phase": func(a *PublicArtifact) { a.Trace[0].Phase = TracePhase(publicEvidenceSentinel) },
		"trace state": func(a *PublicArtifact) { a.Trace[0].State = TraceState(publicEvidenceSentinel) },
	} {
		t.Run(name, func(t *testing.T) {
			mutated := base
			mutated.Trace = append([]TraceEvent(nil), base.Trace...)
			mutate(&mutated)
			err := Validate(mutated)
			if err == nil || strings.Contains(err.Error(), publicEvidenceSentinel) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
	encoded := mustMarshalArtifact(t, base)
	if bytes.Contains(encoded, []byte(publicEvidenceSentinel)) {
		t.Fatal("canonical public evidence contains private sentinel")
	}
}

func TestWritePrivateAtomicCreatesAndReplacesValidatedEvidence(t *testing.T) {
	directory := t.TempDir()
	outputPath := filepath.Join(directory, "public.json")
	firstArtifact := validSingleArtifact()
	if err := WritePrivateAtomic(outputPath, firstArtifact); err != nil {
		t.Fatalf("WritePrivateAtomic(first) error = %v", err)
	}
	first := statPrivateOutput(t, outputPath)
	assertDecodedArtifact(t, outputPath, firstArtifact)
	assertNoEvidenceTemps(t, directory)

	secondArtifact := validRollbackArtifact()
	if err := WritePrivateAtomic(outputPath, secondArtifact); err != nil {
		t.Fatalf("WritePrivateAtomic(second) error = %v", err)
	}
	second := statPrivateOutput(t, outputPath)
	assertDecodedArtifact(t, outputPath, secondArtifact)
	assertNoEvidenceTemps(t, directory)
	if sameFileIdentity(first, second) {
		t.Fatal("atomic replacement reused the existing inode")
	}
}

func TestWritePrivateAtomicRejectsInvalidArtifactWithoutCreatingFile(t *testing.T) {
	directory := t.TempDir()
	outputPath := filepath.Join(directory, "public.json")
	artifact := validSingleArtifact()
	artifact.RunID = publicEvidenceSentinel
	if err := WritePrivateAtomic(outputPath, artifact); err == nil {
		t.Fatal("WritePrivateAtomic() unexpectedly succeeded")
	}
	if _, err := os.Lstat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("invalid artifact created output: %v", err)
	}
}

func TestWritePrivateAtomicRejectsPathAttacks(t *testing.T) {
	t.Run("symlink destination", func(t *testing.T) {
		directory := t.TempDir()
		target := filepath.Join(directory, "target")
		output := filepath.Join(directory, "public.json")
		if err := os.WriteFile(target, []byte(publicEvidenceSentinel), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, output); err != nil {
			t.Fatal(err)
		}
		if err := WritePrivateAtomic(output, validSingleArtifact()); err == nil {
			t.Fatal("symlink destination unexpectedly succeeded")
		}
		assertFileText(t, target, publicEvidenceSentinel)
		assertNoEvidenceTemps(t, directory)
	})

	t.Run("hardlink destination", func(t *testing.T) {
		directory := t.TempDir()
		target := filepath.Join(directory, "target")
		output := filepath.Join(directory, "public.json")
		if err := os.WriteFile(target, []byte(publicEvidenceSentinel), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(target, output); err != nil {
			t.Fatal(err)
		}
		if err := WritePrivateAtomic(output, validSingleArtifact()); err == nil {
			t.Fatal("hardlink destination unexpectedly succeeded")
		}
		assertFileText(t, target, publicEvidenceSentinel)
		assertFileText(t, output, publicEvidenceSentinel)
		assertNoEvidenceTemps(t, directory)
	})

	t.Run("unsafe destination mode", func(t *testing.T) {
		directory := t.TempDir()
		output := filepath.Join(directory, "public.json")
		if err := os.WriteFile(output, []byte(publicEvidenceSentinel), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(output, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := WritePrivateAtomic(output, validSingleArtifact()); err == nil {
			t.Fatal("unsafe destination unexpectedly succeeded")
		}
		assertFileText(t, output, publicEvidenceSentinel)
		assertNoEvidenceTemps(t, directory)
	})

	t.Run("unsafe parent mode", func(t *testing.T) {
		directory := t.TempDir()
		if err := os.Chmod(directory, 0o777); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(directory, 0o700) })
		output := filepath.Join(directory, "public.json")
		if err := WritePrivateAtomic(output, validSingleArtifact()); err == nil {
			t.Fatal("unsafe parent unexpectedly succeeded")
		}
		if _, err := os.Lstat(output); !os.IsNotExist(err) {
			t.Fatalf("unsafe parent received output: %v", err)
		}
	})

	t.Run("symlink parent", func(t *testing.T) {
		root := t.TempDir()
		realParent := filepath.Join(root, "real")
		linkedParent := filepath.Join(root, "linked")
		if err := os.Mkdir(realParent, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(realParent, linkedParent); err != nil {
			t.Fatal(err)
		}
		if err := WritePrivateAtomic(filepath.Join(linkedParent, "public.json"), validSingleArtifact()); err == nil {
			t.Fatal("symlink parent unexpectedly succeeded")
		}
		if _, err := os.Lstat(filepath.Join(realParent, "public.json")); !os.IsNotExist(err) {
			t.Fatalf("symlink parent received output: %v", err)
		}
	})
}

func TestAtomicOutputRejectsDestinationAndParentReplacement(t *testing.T) {
	t.Run("destination created", func(t *testing.T) {
		directory := t.TempDir()
		outputPath := filepath.Join(directory, "public.json")
		output, err := createAtomicOutput(outputPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := output.close(); err != nil {
				t.Errorf("close() error = %v", err)
			}
		}()
		if err := os.WriteFile(outputPath, []byte(publicEvidenceSentinel), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := output.publish(mustMarshalArtifact(t, validSingleArtifact())); err == nil {
			t.Fatal("publish after destination creation unexpectedly succeeded")
		}
		assertFileText(t, outputPath, publicEvidenceSentinel)
	})

	t.Run("destination replaced", func(t *testing.T) {
		directory := t.TempDir()
		outputPath := filepath.Join(directory, "public.json")
		displacedPath := filepath.Join(directory, "displaced")
		if err := os.WriteFile(outputPath, []byte("original"), 0o600); err != nil {
			t.Fatal(err)
		}
		output, err := createAtomicOutput(outputPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := output.close(); err != nil {
				t.Errorf("close() error = %v", err)
			}
		}()
		if err := os.Rename(outputPath, displacedPath); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(outputPath, []byte(publicEvidenceSentinel), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := output.publish(mustMarshalArtifact(t, validSingleArtifact())); err == nil {
			t.Fatal("publish after destination replacement unexpectedly succeeded")
		}
		assertFileText(t, outputPath, publicEvidenceSentinel)
		assertFileText(t, displacedPath, "original")
	})

	t.Run("parent replaced", func(t *testing.T) {
		root := t.TempDir()
		directory := filepath.Join(root, "evidence")
		displaced := filepath.Join(root, "displaced")
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		output, err := createAtomicOutput(filepath.Join(directory, "public.json"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = output.close() }()
		if err := os.Rename(directory, displaced); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := output.publish(mustMarshalArtifact(t, validSingleArtifact())); err == nil {
			t.Fatal("publish after parent replacement unexpectedly succeeded")
		}
		if _, err := os.Lstat(filepath.Join(directory, "public.json")); !os.IsNotExist(err) {
			t.Fatalf("replacement parent received output: %v", err)
		}
	})

	t.Run("parent replaced by symlink to opened directory", func(t *testing.T) {
		root := t.TempDir()
		directory := filepath.Join(root, "evidence")
		displaced := filepath.Join(root, "displaced")
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		output, err := createAtomicOutput(filepath.Join(directory, "public.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(directory, displaced); err != nil {
			_ = output.close()
			t.Fatal(err)
		}
		if err := os.Symlink(displaced, directory); err != nil {
			_ = output.close()
			t.Fatal(err)
		}
		if err := output.publish(mustMarshalArtifact(t, validSingleArtifact())); err == nil {
			_ = output.close()
			t.Fatal("publish after parent symlink replacement unexpectedly succeeded")
		}
		if err := output.close(); err != nil {
			t.Fatalf("close() error = %v", err)
		}
		if _, err := os.Lstat(filepath.Join(displaced, "public.json")); !os.IsNotExist(err) {
			t.Fatalf("opened parent received output through replacement symlink: %v", err)
		}
		assertNoEvidenceTemps(t, displaced)
	})
}

func validSingleArtifact() PublicArtifact {
	digest := digestOf('a')
	return PublicArtifact{
		APIVersion:            PublicAPIVersion,
		Kind:                  PublicKind,
		RunID:                 "123456789",
		RunAttempt:            2,
		HeadSHA:               strings.Repeat("1", 40),
		HelmBaseRevision:      41,
		HelmTargetRevision:    42,
		Outcome:               OutcomeSingle,
		Domain:                releasedomain.DomainNodeLocal,
		PlanDigest:            digest,
		ChangedEvidenceDigest: digest,
		BaseDigest:            digest,
		TargetDigest:          digest,
		RepeatDigest:          digest,
		OwnershipDigest:       digest,
		ContextDigest:         digest,
		ForwardDigest:         digest,
		ReverseDigest:         digest,
		ReverseAuthorized:     true,
		ReverseDomain:         releasedomain.DomainNodeLocal,
		ReverseObjectCount:    1,
		Trace:                 successTrace(),
		WriteBoundaryCrossed:  true,
	}
}

func validRollbackArtifact() PublicArtifact {
	artifact := validSingleArtifact()
	artifact.Trace = rollbackTrace(TraceStateSucceeded)
	artifact.RollbackAttempted = true
	artifact.RollbackCompleted = true
	return artifact
}

func validNoWriteArtifact(outcome Outcome, base, target uint64) PublicArtifact {
	artifact := validSingleArtifact()
	artifact.Outcome = outcome
	artifact.HelmBaseRevision = base
	artifact.HelmTargetRevision = target
	artifact.Domain = ""
	artifact.ReverseAuthorized = false
	artifact.ReverseDomain = ""
	artifact.ReverseObjectCount = 0
	artifact.Trace = []TraceEvent{}
	artifact.WriteBoundaryCrossed = false
	return artifact
}

func successTrace() []TraceEvent {
	return []TraceEvent{
		{Phase: TracePhaseTransaction, State: TraceStateStarted},
		{Phase: TracePhasePrepare, State: TraceStateStarted},
		{Phase: TracePhasePrepare, State: TraceStateSucceeded},
		{Phase: TracePhaseApply, State: TraceStateStarted},
		{Phase: TracePhaseApply, State: TraceStateSucceeded},
		{Phase: TracePhaseVerify, State: TraceStateStarted},
		{Phase: TracePhaseVerify, State: TraceStateSucceeded},
		{Phase: TracePhaseTransaction, State: TraceStateSucceeded},
	}
}

func rollbackTrace(state TraceState) []TraceEvent {
	return []TraceEvent{
		{Phase: TracePhaseTransaction, State: TraceStateStarted},
		{Phase: TracePhasePrepare, State: TraceStateStarted},
		{Phase: TracePhasePrepare, State: TraceStateSucceeded},
		{Phase: TracePhaseApply, State: TraceStateStarted},
		{Phase: TracePhaseApply, State: TraceStateFailed},
		{Phase: TracePhaseRollback, State: TraceStateStarted},
		{Phase: TracePhaseRollback, State: state},
		{Phase: TracePhaseTransaction, State: TraceStateFailed},
	}
}

func digestOf(character byte) string {
	return "sha256:" + strings.Repeat(string(character), 64)
}

func mustMarshalArtifact(t *testing.T, artifact PublicArtifact) []byte {
	t.Helper()
	if err := Validate(artifact); err != nil {
		t.Fatalf("test artifact is invalid: %v", err)
	}
	encoded, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func statPrivateOutput(t *testing.T, path string) unix.Stat_t {
	t.Helper()
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		t.Fatal(err)
	}
	if err := validatePrivateFile(stat, "test output"); err != nil {
		t.Fatal(err)
	}
	return stat
}

func assertDecodedArtifact(t *testing.T, path string, want PublicArtifact) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got, err := Decode(file)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded artifact = %#v, want %#v", got, want)
	}
}

func assertFileText(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("file contents = %q, want %q", data, want)
	}
}

func assertNoEvidenceTemps(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fugue-release-public-evidence-") {
			t.Errorf("temporary evidence file remains: %s", entry.Name())
		}
	}
}
