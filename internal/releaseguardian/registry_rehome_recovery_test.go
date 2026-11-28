package releaseguardian

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
)

func TestRegistryRehomeRecoveryReconstructsBothSealedVersions(t *testing.T) {
	ctx := t.Context()
	objects := map[string]registryTestContent{}
	const repoPath = "/v2/fixture/rehome/manifests/"
	old, oldLayer := registryImageFixture(t, objects, "fixture/rehome", strings.Repeat("2", 40), false)
	current, _ := registryImageFixture(t, objects, "fixture/rehome", strings.Repeat("3", 40), true)
	for _, ref := range []string{old.TopDigest, current.TopDigest} {
		objects[repoPath+"fugue-recovery-sha256-"+strings.TrimPrefix(ref, "sha256:")] = objects[repoPath+ref]
	}
	archive, _, _ := rehomeArchiveFixture(t, old, current, "")
	archivePin := digest(archive)
	sealed, err := readReleaseRecoveryArchive(archive, archivePin)
	if err != nil {
		t.Fatal(err)
	}
	client := fakeRehomeProvider(t, archive, sealed)
	source, err := ObserveRegistryRehomeSource(ctx, client, archive, archivePin, "global")
	if err != nil {
		t.Fatal(err)
	}
	journal := rehomeResourceStub(func(_ context.Context, scope productioncontrol.Scope) (productioncontrol.ResourceState, error) {
		return productioncontrol.ResourceState{Baseline: productioncontrol.Baseline{Scope: scope}, LastVerified: source.Version}, nil
	})
	registry, err := NewIndependentRegistry(testNativeRegistry, nil, registryRoundTrip(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Host != testNativeRegistry || r.URL.Scheme != "https" {
			t.Errorf("unexpected registry request: %s %s", r.Method, r.URL)
		}
		o, found := objects[r.URL.Path]
		status := http.StatusOK
		if !found && r.URL.Path != "/v2/" {
			status = http.StatusNotFound
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {string(o.mediaType)}},
			ContentLength: int64(len(o.body)), Body: io.NopCloser(bytes.NewReader(o.body)), Request: r}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	s, err := DraftRegistryRehomeSubmission(ctx, client, journal, registry, archive, archivePin, "global", "fixture", "recovery-test", current.Repository)
	if err != nil {
		t.Fatal(err)
	}
	raw, report, err := BuildRegistryRehomeRecoveryBundle(archive, archivePin, s)
	if err != nil || report.ArchiveDigest != archivePin || report.BundleDigest != digest(raw) ||
		report.Current.SourceRecordDigest != sealed.forward.version.Monitor.RecordDigest || report.Rollback.SourceRecordDigest != sealed.rollback.version.Monitor.RecordDigest {
		t.Fatalf("initial recovery projection: %+v %v", report, err)
	}
	var recovered registryRehomeRecoveryBundle
	if err := decodeStrict(raw, &recovered); err != nil || !bytes.Equal(recovered.Archive, archive) || digest(archive) != archivePin {
		t.Fatalf("original sealed archive did not survive unchanged: %v", err)
	}
	for _, v := range []struct {
		template, original []byte
		image, source      string
	}{
		{recovered.Current, sealed.forward.forward, current.ImmutableRef, sealed.forward.version.Receipt.ImmutableRef},
		{recovered.Rollback, sealed.rollback.forward, old.ImmutableRef, sealed.rollback.version.Receipt.ImmutableRef},
	} {
		ids, err := declarativerelease.ResourceSetIdentities(v.template)
		if err != nil || len(ids) != 1 || ids[0] != report.Workload {
			t.Fatalf("the recovery output must contain only the enrolled Deployment: %+v %v", ids, err)
		}
		actual, err := declarativerelease.ResourceSetItem(v.template, ids[0])
		if err != nil {
			t.Fatal(err)
		}
		original, err := declarativerelease.ResourceSetItem(v.original, ids[0])
		if err != nil {
			t.Fatal(err)
		}
		// The rendered artifact image annotation and both container image
		// fields are the only differences from the sealed Deployment.
		expected, _ := declarativerelease.CanonicalJSON(original)
		ours, _ := declarativerelease.CanonicalJSON(actual)
		if bytes.Count(expected, []byte(v.source)) != 3 || !bytes.Equal(ours, bytes.ReplaceAll(expected, []byte(v.source), []byte(v.image))) {
			t.Fatal("recovery changed sealed Deployment metadata or non-image fields")
		}
	}
	for _, r := range []*IndependentRegistry{nil, registry} {
		if observed, err := VerifyRegistryRehomeRecoveryBundle(ctx, raw, report.BundleDigest, r); err != nil || observed != report {
			t.Fatalf("verification failed: %+v %v", observed, err)
		}
	}
	for _, v := range []struct {
		role    string
		record  sealedRecoveryRecord
		version RegistryRehomeRecoveryVersion
	}{
		{"current", sealed.forward, report.Current},
		{"rollback", sealed.rollback, report.Rollback},
	} {
		t.Run("stage "+v.role, func(t *testing.T) {
			manifest, input, err := ExportRegistryRehomeReleaseInput(raw, report.BundleDigest, v.role)
			wantTarget := v.record.prepared.Forward
			wantTarget.ImageRef, wantTarget.ManifestDigest = v.version.Image, digest(manifest)
			if err != nil || input.Role != v.role || input.BundleDigest != report.BundleDigest || input.IntentDigest != report.IntentDigest ||
				input.Workload != report.Workload || input.SourceRecordDigest != v.version.SourceRecordDigest || input.Target != wantTarget ||
				len(input.Companions) != 1 || input.Companions[0].Kind != "Service" {
				t.Fatalf("wrong staged input: %+v %v", input, err)
			}
			// Test bytes and roles, not just the manifest digest: the exact
			// original Service must survive, including its historical stamps.
			want := bytes.ReplaceAll(v.record.forward, []byte(v.record.version.Receipt.ImmutableRef), []byte(v.version.Image))
			if !bytes.Equal(want, manifest) {
				t.Fatal("the staged set changed more than the selected release's Deployment images")
			}
			for _, id := range input.Companions {
				original, _ := declarativerelease.ResourceSetItem(v.record.forward, id)
				derived, err := declarativerelease.ResourceSetItem(manifest, id)
				if err != nil {
					t.Fatal(err)
				}
				a, _ := declarativerelease.CanonicalJSON(original)
				b, _ := declarativerelease.CanonicalJSON(derived)
				if !bytes.Equal(a, b) {
					t.Fatal("staging changed a companion object")
				}
			}
			if err := VerifyNativeDeploymentImages(manifest, report.Workload, input.Target); err == nil {
				t.Fatal("the static input must not bypass the current single-Deployment executor")
			}
			key := Key{Component: sealed.report.Pair.Component, Group: "global"}
			next := guardianPlanRelease(key, strings.Repeat("a", 40), wantTarget.ConfigSHA, v.record.version.Receipt.TopDigest,
				&declarativerelease.Delivery{Writer: "guardian", Group: key.Group})
			next.Artifact.Repository = current.Repository
			plan := guardianPlan(t, wantTarget.ConfigSHA, strings.Repeat("a", 40), next)
			bound, err := declarativerelease.BindGuardianLKG(plan, key.Component, declarativerelease.RenderedManifests{Forward: manifest}, manifest)
			if err != nil || !bytes.Equal(bound.LKG, manifest) || bound.LKGDigest != input.Target.ManifestDigest {
				t.Fatalf("ordinary Guardian exact-LKG binder rejected the staged input: %v", err)
			}
		})
	}
	if staged, _, err := ExportRegistryRehomeReleaseInput(raw, otherDigest, "current"); err == nil || staged != nil {
		t.Fatal("staged a manifest from an unpinned recovery bundle")
	}
	if staged, _, err := ExportRegistryRehomeReleaseInput(raw, report.BundleDigest, "unknown"); err == nil || staged != nil {
		t.Fatal("staged a manifest with an ambiguous recovery role")
	}
	delete(objects, oldLayer)
	if _, err := VerifyRegistryRehomeRecoveryBundle(ctx, raw, report.BundleDigest, registry); err == nil || !strings.Contains(err.Error(), "verify rollback recovery mirror") {
		t.Fatalf("mirror verification did not detect missing rollback layer: %v", err)
	}

	reject := func(change func(*registryRehomeRecoveryBundle), want string) {
		t.Helper()
		altered := recovered
		change(&altered)
		tampered, err := declarativerelease.CanonicalJSON(altered)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyRegistryRehomeRecoveryBundle(ctx, tampered, digest(tampered), nil); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("recomputed pin let a modified bundle pass: %v; wanted %s", err, want)
		}
		if manifest, _, err := ExportRegistryRehomeReleaseInput(tampered, digest(tampered), "rollback"); err == nil || manifest != nil {
			t.Fatal("staged an input from an invalid projection")
		}
	}
	reject(func(b *registryRehomeRecoveryBundle) { b.Rollback = b.Current }, "differs from the sealed projection")
	reject(func(b *registryRehomeRecoveryBundle) { b.Rollback = sealed.rollback.forward }, "differs from the sealed projection")
	reject(func(b *registryRehomeRecoveryBundle) {
		review, err := InspectRegistryRehomeSubmission(s)
		if err != nil {
			t.Fatal(err)
		}
		review.Draft.Mirror.Rollback.SourceReceiptDigest = otherDigest
		review.Draft.DraftDigest, err = registryRehomeDraftDigest(review.Draft)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := declarativerelease.CanonicalJSON(review)
		if err != nil {
			t.Fatal(err)
		}
		i := s.Intent
		i, err = productioncontrol.NewIntent(i.ClusterID, i.ResourceID, i.RequestID, i.Operation,
			i.Generation, i.CurrentDigest, review.Draft.DraftDigest, i.RollbackDigest, payload)
		if err != nil {
			t.Fatal(err)
		}
		b.Review = productioncontrol.Submission{Intent: i, Payload: payload}
		if _, err := InspectRegistryRehomeSubmission(b.Review); err != nil {
			t.Fatalf("test needs a structurally valid review: %v", err)
		}
	}, "rollback recovery: reviewed projection or mirror does not match its sealed manifest and artifact identity")
}
