package appimages

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDistributedEvidenceIndexesOnlyAuthorizedApps(t *testing.T) {
	now := time.Date(2026, 4, 5, 10, 0, 0, 0, time.UTC)
	ref := "registry.example/fugue-apps/own:v1"
	foreignRef := "registry.example/fugue-apps/foreign:v1"
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fresh := now.Add(-2 * time.Minute)
	foreignFresh := now.Add(-time.Minute)
	stale := now.Add(-time.Hour)
	manifestTime := now.Add(-3 * time.Minute)
	evidence := BuildDistributedEvidence(map[string]struct{}{"own": {}},
		[]model.Image{{AppID: "own", ImageRef: ref, CanonicalDigest: digest, LifecycleState: model.ImageLifecycleAvailable}},
		[]model.ImageLocation{
			{AppID: "own", ImageRef: ref, Digest: digest, Status: model.ImageLocationStatusPresent, LastSeenAt: &fresh},
			{AppID: "own", ImageRef: ref, Digest: digest, Status: model.ImageLocationStatusPresent, LastSeenAt: &stale},
			{AppID: "foreign", ImageRef: foreignRef, Digest: digest, Status: model.ImageLocationStatusPresent, LastSeenAt: &foreignFresh},
		},
		[]model.ImageCacheManifest{{
			ImageRef: ref, Digest: digest, GraphStatus: "complete", LastSeenAt: manifestTime,
			ManifestSizeBytes: 10, TotalBlobBytes: 100,
		}},
		now.Add(-10*time.Minute),
	)
	if got := evidence.ObservedAt(); !got.Equal(fresh) {
		t.Fatalf("snapshot timestamp = %s, want %s", got, fresh)
	}
	if got := evidence.ObservedAtForApp("foreign"); !got.Equal(manifestTime) {
		t.Fatalf("foreign location affected scoped timestamp: %s", got)
	}
	measurement := evidence.Measure("own", Candidate{ImageRef: ref, Current: true})
	if !measurement.Available || !measurement.HasSize || measurement.SizeBytes != 110 ||
		measurement.Digest != digest || measurement.Status != ProjectImageUsageMeasurementComplete {
		t.Fatalf("authorized measurement: %+v", measurement)
	}
	measurement = evidence.Measure("foreign", Candidate{ImageRef: foreignRef})
	if measurement.Available || measurement.HadFreshEvidence || measurement.HasSize {
		t.Fatalf("foreign app evidence was retained: %+v", measurement)
	}
}

func TestMergeProjectImageMeasurementReasonsIsStableAndUnique(t *testing.T) {
	t.Parallel()

	got := MergeMeasurementReasons(
		[]string{ProjectImageUsageReasonSizeConflict, ProjectImageUsageReasonDigestConflict},
		[]string{ProjectImageUsageReasonStaleInventory, ProjectImageUsageReasonSizeConflict, ""},
	)
	want := []string{
		ProjectImageUsageReasonDigestConflict,
		ProjectImageUsageReasonSizeConflict,
		ProjectImageUsageReasonStaleInventory,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("measurement reasons = %#v, want stable unique order %#v", got, want)
	}
}

func TestDistributedImageCandidateMeasurementReportsKnownDigestLocationConflict(t *testing.T) {
	t.Parallel()

	const (
		appID             = "app-conflict"
		imageRef          = "registry.example/fugue-apps/example:git-111111111111"
		canonicalDigest   = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		conflictingDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	)
	candidate := Candidate{ImageRef: imageRef, Current: true}
	manifest := model.ImageCacheManifest{
		Repo:              "fugue-apps/example",
		Target:            "git-111111111111",
		Digest:            canonicalDigest,
		ManifestSizeBytes: 12,
		TotalBlobBytes:    180,
		Present:           true,
	}
	manifestsByKey := map[string][]model.ImageCacheManifest{}
	for _, key := range distributedImageManifestKeys(manifest) {
		manifestsByKey[key] = append(manifestsByKey[key], manifest)
	}
	evidence := DistributedEvidence{
		imagesByAppID: map[string][]model.Image{
			appID: {{
				AppID:           appID,
				ImageRef:        imageRef,
				CanonicalDigest: canonicalDigest,
				LifecycleState:  model.ImageLifecycleAvailable,
			}},
		},
		locationsByAppID: map[string][]model.ImageLocation{
			appID: {
				{AppID: appID, ImageRef: imageRef, Digest: canonicalDigest, Status: model.ImageLocationStatusPresent},
				{AppID: appID, ImageRef: imageRef, Digest: conflictingDigest, Status: model.ImageLocationStatusPresent},
			},
		},
		staleLocationsByAppID: map[string][]model.ImageLocation{},
		manifestsByKey:        manifestsByKey,
		staleManifestsByKey:   map[string][]model.ImageCacheManifest{},
	}

	measurement := distributedImageCandidateMeasurementFor(model.App{ID: appID}, candidate, evidence)
	if measurement.digest != canonicalDigest || measurement.hasSize || !measurement.digestConflict {
		t.Fatalf("expected canonical digest with unavailable conflicting location evidence, got %#v", measurement)
	}
	if want := []string{ProjectImageUsageReasonDigestConflict}; !reflect.DeepEqual(measurement.reasons, want) {
		t.Fatalf("expected stable location digest-conflict attribution, got %#v", measurement.reasons)
	}
}

func TestDistributedImageCandidateMeasurementIgnoresMismatchedRuntimeGeneration(t *testing.T) {
	t.Parallel()

	const (
		appID         = "app-ref-coherence"
		imageRef      = "registry.push.example/fugue-apps/example:build-source"
		runtimeRef    = "registry.pull.example/fugue-apps/example:build-other@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		sourceDigest  = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		runtimeDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	manifests := []model.ImageCacheManifest{
		{
			Repo:              "fugue-apps/example",
			Target:            "build-source",
			Digest:            sourceDigest,
			ManifestSizeBytes: 10,
			TotalBlobBytes:    100,
			Present:           true,
		},
		{
			Repo:              "fugue-apps/example",
			Target:            "build-other",
			Digest:            runtimeDigest,
			ManifestSizeBytes: 20,
			TotalBlobBytes:    200,
			Present:           true,
		},
	}
	manifestsByKey := map[string][]model.ImageCacheManifest{}
	for _, manifest := range manifests {
		for _, key := range distributedImageManifestKeys(manifest) {
			manifestsByKey[key] = append(manifestsByKey[key], manifest)
		}
	}
	evidence := DistributedEvidence{
		imagesByAppID:         map[string][]model.Image{},
		locationsByAppID:      map[string][]model.ImageLocation{},
		staleLocationsByAppID: map[string][]model.ImageLocation{},
		manifestsByKey:        manifestsByKey,
		staleManifestsByKey:   map[string][]model.ImageCacheManifest{},
	}
	candidate := Candidate{
		ImageRef:        imageRef,
		RuntimeImageRef: runtimeRef,
		Current:         true,
	}

	measurement := distributedImageCandidateMeasurementFor(model.App{ID: appID}, candidate, evidence)
	if measurement.digest != sourceDigest || measurement.sizeBytes != 110 || !measurement.hasSize || !measurement.complete {
		t.Fatalf("expected only the source generation's complete measurement, got %#v", measurement)
	}
	if measurement.digestConflict || measurement.sizeConflict || len(measurement.reasons) != 0 {
		t.Fatalf("expected mismatched runtime generation to be excluded from measurement evidence, got %#v", measurement)
	}
}

func TestDistributedRuntimeImageReferenceCoherence(t *testing.T) {
	t.Parallel()

	const (
		digestA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		digestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	tests := []struct {
		name       string
		imageRef   string
		runtimeRef string
		want       bool
	}{
		{
			name:       "same tag across configured hosts",
			imageRef:   "registry.push.example/fugue-apps/example:build-a",
			runtimeRef: "registry.pull.example/fugue-apps/example:build-a",
			want:       true,
		},
		{
			name:       "same tag with runtime digest pin",
			imageRef:   "registry.push.example/fugue-apps/example:build-a",
			runtimeRef: "registry.pull.example/fugue-apps/example:build-a@" + digestA,
			want:       true,
		},
		{
			name:       "same digest across configured hosts",
			imageRef:   "registry.push.example/fugue-apps/example@" + digestA,
			runtimeRef: "registry.pull.example/fugue-apps/example@" + digestA,
			want:       true,
		},
		{
			name:       "same digest under different tags",
			imageRef:   "registry.push.example/fugue-apps/example:build-a@" + digestA,
			runtimeRef: "registry.pull.example/fugue-apps/example:build-b@" + digestA,
			want:       true,
		},
		{
			name:       "different tags without shared digest",
			imageRef:   "registry.push.example/fugue-apps/example:build-a",
			runtimeRef: "registry.pull.example/fugue-apps/example:build-b@" + digestB,
			want:       false,
		},
		{
			name:       "tag and uncorroborated digest",
			imageRef:   "registry.push.example/fugue-apps/example:build-a",
			runtimeRef: "registry.pull.example/fugue-apps/example@" + digestA,
			want:       false,
		},
		{
			name:       "same tag in different repositories",
			imageRef:   "registry.push.example/fugue-apps/example-a:build-a",
			runtimeRef: "registry.pull.example/fugue-apps/example-b:build-a",
			want:       false,
		},
		{
			name:       "conflicting explicit digests",
			imageRef:   "registry.push.example/fugue-apps/example:build-a@" + digestA,
			runtimeRef: "registry.pull.example/fugue-apps/example:build-a@" + digestB,
			want:       false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := distributedRuntimeImageRefMatchesCandidate(test.imageRef, test.runtimeRef); got != test.want {
				t.Fatalf("coherence(%q, %q) = %t, want %t", test.imageRef, test.runtimeRef, got, test.want)
			}
		})
	}
}
