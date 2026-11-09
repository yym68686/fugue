package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/inputarchive"
)

func pendingSourceFixture(t *testing.T) (inputarchive.PendingRootRequest, *GroupPendingRootSourceVerifier, string) {
	t.Helper()
	s, _, group, keyDir, _ := publicationInputFixture(t, true)
	state, err := s.readGroupState(s.groupStatePath(group), group)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := loadGroupBundleSigningKeyring(filepath.Join(keyDir, group+".json"), group)
	if err != nil {
		t.Fatal(err)
	}
	pub := state.Published
	input := state.RetentionInputs[0]
	publication := state.AuthorityLedger[pub.PublicationSequence-1]
	binding, err := bindGroupOriginalPublication(group, input, publication, pub.Bundle, pub.RecoveryEpoch, keys)
	if err != nil {
		t.Fatal(err)
	}
	request, err := pendingGroupRootRequest(group, input, publication, pub.Bundle, pub.RecoveryEpoch, binding)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewGroupPendingRootSourceVerifier(keyDir, group)
	if err != nil {
		t.Fatal(err)
	}
	return request, verifier, keyDir
}

func TestGroupPendingRootSourceAuthenticatesOriginalAndExactDerivedRoots(t *testing.T) {
	request, verifier, _ := pendingSourceFixture(t)
	witness, err := verifier.VerifyPendingRootSource(t.Context(), request)
	if err != nil || witness.CheckPendingRootSource(t.Context(), request) != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"owner", "generation", "source-digest", "ref", "schema", "input", "attestation", "publication", "bundle", "group", "unknown", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			copy := request
			copy.Registration.References = append([]inputarchive.HotReference(nil), request.Registration.References...)
			var source groupPendingRootSource
			if err := json.Unmarshal(request.Source, &source); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "owner":
				copy.Registration.Owner += "-other"
			case "generation":
				copy.Registration.Generation += "-other"
			case "source-digest":
				copy.Registration.SourceDigest = "sha256:" + string(bytes.Repeat([]byte{'f'}, 64))
			case "ref":
				copy.Registration.References[0].ID += "-other"
			case "schema":
				copy.SourceSchema += "-other"
			case "input":
				source.Input.Entry.RecordedAt = source.Input.Entry.RecordedAt.Add(time.Second)
			case "attestation":
				source.Input.Attestation = nil
			case "publication":
				source.Publication.PublishedBundleDigest += "-other"
			case "bundle":
				source.Bundle.Signature += "-other"
			case "group":
				source.GroupID += "-other"
			}
			copy.Source, _ = json.Marshal(source)
			if mode == "unknown" {
				copy.Source = append([]byte(`{"signature_verified":true,`), copy.Source[1:]...)
			}
			if mode == "duplicate" {
				copy.Source = append([]byte(`{"edge_group_id":"discarded",`), copy.Source[1:]...)
			}
			if _, err := verifier.VerifyPendingRootSource(t.Context(), copy); err == nil {
				t.Fatal("rebound source accepted")
			}
			if err := witness.CheckPendingRootSource(t.Context(), copy); err == nil {
				t.Fatal("witness reused for changed request")
			}
		})
	}
}

func TestGroupPendingRootSourceRechecksCurrentIndependentPolicy(t *testing.T) {
	request, verifier, keyDir := pendingSourceFixture(t)
	witness, err := verifier.VerifyPendingRootSource(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(keyDir, verifier.group+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var keys groupBundleSigningKeyringFile
	if err := json.Unmarshal(raw, &keys); err != nil {
		t.Fatal(err)
	}
	keys.Generation++
	raw, _ = json.Marshal(keys)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := witness.CheckPendingRootSource(t.Context(), request); err == nil {
		t.Fatal("old policy witness survived rotation")
	}
	if _, err := verifier.VerifyPendingRootSource(t.Context(), request); err != nil {
		t.Fatal("fresh current trust rejected authentic retained source", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := verifier.VerifyPendingRootSource(ctx, request); err == nil {
		t.Fatal("canceled authentication accepted")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifyPendingRootSource(t.Context(), request); err == nil {
		t.Fatal("untrusted projected key permissions accepted")
	}
}

type pendingRequestRegistrarFunc func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReceipt, error)

func (f pendingRequestRegistrarFunc) RetainPendingCompilerInputExternalRoot(ctx context.Context, r inputarchive.PendingRootRequest) (inputarchive.PendingRootReceipt, error) {
	return f(ctx, r)
}

func TestGroupPendingRootNativeCASForwardsIndependentlyVerifiableOriginal(t *testing.T) {
	s, signer, group, now, batch := pendingPublicationFixture(t, true)
	verifier, err := NewGroupPendingRootSourceVerifier(s.compileInputKeyringDir, group)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	s = reopenPendingPublication(t, s, pendingRequestRegistrarFunc(func(ctx context.Context, request inputarchive.PendingRootRequest) (inputarchive.PendingRootReceipt, error) {
		calls++
		witness, err := verifier.VerifyPendingRootSource(ctx, request)
		if err != nil {
			return inputarchive.PendingRootReceipt{}, err
		}
		if err := witness.CheckPendingRootSource(ctx, request); err != nil {
			return inputarchive.PendingRootReceipt{}, err
		}
		return exactPendingReceipt(request.Registration), nil
	}))
	result, err := (GroupAuthorityPublisher{Store: s, Signer: signer, Now: func() time.Time { return now }}).Publish(t.Context(), batch)
	if err != nil || result.Published != 1 || calls != 1 {
		t.Fatal(result, calls, err)
	}
}
