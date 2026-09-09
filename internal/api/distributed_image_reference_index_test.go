package api

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func referenceIndexFixture(versions int) (distributedImageUsageEvidence, []appImageCandidate) {
	e := distributedImageUsageEvidence{
		imagesByAppID: make(map[string][]model.Image), locationsByAppID: make(map[string][]model.ImageLocation),
		staleLocationsByAppID: make(map[string][]model.ImageLocation), manifestsByKey: make(map[string][]model.ImageCacheManifest),
	}
	var candidates []appImageCandidate
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := 0; i < versions; i++ {
		ref := fmt.Sprintf("registry.example/fugue-apps/example:v%d", i)
		digest := fmt.Sprintf("sha256:%064x", i+1)
		candidate := appImageCandidate{ImageRef: ref, RuntimeImageRef: fmt.Sprintf("localhost:5000/fugue-apps/example:v%d", i)}
		candidates = append(candidates, candidate)
		e.imagesByAppID["app"] = append(e.imagesByAppID["app"], model.Image{ImageRef: ref, CanonicalDigest: digest, LifecycleState: model.ImageLifecycleAvailable, UpdatedAt: now})
		for j := 0; j < 3; j++ {
			location := model.ImageLocation{AppID: "app", ImageRef: ref, Digest: digest, Status: model.ImageLocationStatusPresent, UpdatedAt: now, RuntimeID: fmt.Sprintf("runtime-%d", j)}
			e.locationsByAppID["app"] = append(e.locationsByAppID["app"], location)
			e.staleLocationsByAppID["app"] = append(e.staleLocationsByAppID["app"], location)
		}
		manifest := model.ImageCacheManifest{ID: ref, ImageRef: ref, Digest: digest, GraphStatus: "complete", ManifestSizeBytes: 10, TotalBlobBytes: 100, LastSeenAt: now}
		for _, key := range distributedImageManifestKeys(manifest) {
			e.manifestsByKey[key] = append(e.manifestsByKey[key], manifest)
		}
	}
	return e, candidates
}

func TestDistributedImageReferenceIndexPreservesMeasurements(t *testing.T) {
	e, candidates := referenceIndexFixture(40)
	// Include duplicate IDs, missing IDs, equal timestamps, conflicting digest
	// evidence and a foreign app. None may be silently merged or re-attributed.
	duplicate := e.locationsByAppID["app"][0]
	duplicate.Digest = fmt.Sprintf("sha256:%064x", 999)
	e.locationsByAppID["app"] = append(e.locationsByAppID["app"], duplicate, duplicate)
	e.locationsByAppID["other"] = append(e.locationsByAppID["other"], duplicate)
	indexed := e
	indexed.buildReferenceIndexes()
	for _, appID := range []string{"app", "other", "absent"} {
		for _, candidate := range append(candidates, appImageCandidate{ImageRef: "registry.example/fugue-apps/example:absent"}) {
			app := model.App{ID: appID}
			want := distributedImageCandidateMeasurementFor(app, candidate, e)
			got := distributedImageCandidateMeasurementFor(app, candidate, indexed)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("measurement changed for %s %s: got=%+v want=%+v", appID, candidate.ImageRef, got, want)
			}
		}
	}
}

func BenchmarkDistributedImageReferenceMatch(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprintf("indexed=%t", indexed), func(b *testing.B) {
			e, candidates := referenceIndexFixture(300)
			if indexed {
				e.buildReferenceIndexes()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, candidate := range candidates {
					_ = distributedImageCandidateMeasurementFor(model.App{ID: "app"}, candidate, e)
				}
			}
		})
	}
}
