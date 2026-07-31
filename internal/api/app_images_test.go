package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func mustRuntimeDigestRefForAppImagesTest(t *testing.T, pushBase, pullBase, imageRef, digest string) string {
	t.Helper()
	server := &Server{
		registryPushBase: pushBase,
		registryPullBase: pullBase,
	}
	runtimeImageRef := server.runtimeImageRefFromManagedRefWithDigest(imageRef, digest)
	if runtimeImageRef == "" {
		t.Fatalf("expected runtime digest ref for %q with digest %q", imageRef, digest)
	}
	return runtimeImageRef
}

func TestMergeProjectImageMeasurementReasonsIsStableAndUnique(t *testing.T) {
	t.Parallel()

	got := mergeProjectImageMeasurementReasons(
		[]string{projectImageUsageReasonSizeConflict, projectImageUsageReasonDigestConflict},
		[]string{projectImageUsageReasonStaleInventory, projectImageUsageReasonSizeConflict, ""},
	)
	want := []string{
		projectImageUsageReasonDigestConflict,
		projectImageUsageReasonSizeConflict,
		projectImageUsageReasonStaleInventory,
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
	candidate := appImageCandidate{ImageRef: imageRef, Current: true}
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
	evidence := distributedImageUsageEvidence{
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
	if want := []string{projectImageUsageReasonDigestConflict}; !reflect.DeepEqual(measurement.reasons, want) {
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
	evidence := distributedImageUsageEvidence{
		imagesByAppID:         map[string][]model.Image{},
		locationsByAppID:      map[string][]model.ImageLocation{},
		staleLocationsByAppID: map[string][]model.ImageLocation{},
		manifestsByKey:        manifestsByKey,
		staleManifestsByKey:   map[string][]model.ImageCacheManifest{},
	}
	candidate := appImageCandidate{
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

func TestHandleGetAppImagesReturnsCurrentAndHistoricalVersions(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, project, app, _, oldImageRef, newImageRef, _ := setupAppImagesTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)

	if response.AppID != app.ID {
		t.Fatalf("expected app id %q, got %q", app.ID, response.AppID)
	}
	if !response.RegistryConfigured {
		t.Fatal("expected registry inventory to be configured")
	}
	if response.Summary.VersionCount != 2 {
		t.Fatalf("expected two image versions, got %#v", response.Summary)
	}
	if response.Summary.CurrentVersionCount != 1 {
		t.Fatalf("expected one current version, got %#v", response.Summary)
	}
	if response.Summary.StaleVersionCount != 1 {
		t.Fatalf("expected one stale version, got %#v", response.Summary)
	}
	if response.Summary.TotalSizeBytes != 240 {
		t.Fatalf("expected total size 240, got %d", response.Summary.TotalSizeBytes)
	}
	if response.Summary.CurrentSizeBytes != 180 {
		t.Fatalf("expected current size 180, got %d", response.Summary.CurrentSizeBytes)
	}
	if response.Summary.StaleSizeBytes != 160 {
		t.Fatalf("expected stale size 160, got %d", response.Summary.StaleSizeBytes)
	}
	if response.Summary.ReclaimableSizeBytes != 60 {
		t.Fatalf("expected reclaimable size 60, got %d", response.Summary.ReclaimableSizeBytes)
	}
	if len(response.Versions) != 2 {
		t.Fatalf("expected two versions in response, got %#v", response.Versions)
	}

	versionByImageRef := make(map[string]appImageVersion, len(response.Versions))
	for _, version := range response.Versions {
		versionByImageRef[version.ImageRef] = version
	}

	currentVersion, ok := versionByImageRef[newImageRef]
	if !ok {
		t.Fatalf("expected current image %q in response", newImageRef)
	}
	if !currentVersion.Current {
		t.Fatalf("expected %q to be current: %#v", newImageRef, currentVersion)
	}
	if currentVersion.Status != appImageStatusAvailable {
		t.Fatalf("expected current version to be available, got %#v", currentVersion)
	}
	if currentVersion.DeleteSupported {
		t.Fatalf("expected current version to be non-deletable, got %#v", currentVersion)
	}
	if !currentVersion.RedeploySupported {
		t.Fatalf("expected current version to be redeployable, got %#v", currentVersion)
	}

	staleVersion, ok := versionByImageRef[oldImageRef]
	if !ok {
		t.Fatalf("expected stale image %q in response", oldImageRef)
	}
	if staleVersion.Current {
		t.Fatalf("expected %q to be stale: %#v", oldImageRef, staleVersion)
	}
	if staleVersion.ReclaimableSizeBytes != 60 {
		t.Fatalf("expected stale reclaimable size 60, got %#v", staleVersion)
	}
	if !staleVersion.DeleteSupported {
		t.Fatalf("expected stale version to be deletable, got %#v", staleVersion)
	}
	if staleVersion.Source == nil || staleVersion.Source.CommitSHA == "" {
		t.Fatalf("expected stale version source metadata, got %#v", staleVersion)
	}
	if response.ReclaimRequiresGC {
		t.Fatalf("expected inventory to report immediate cleanup, got %#v", response)
	}
	if response.ReclaimNote != "" {
		t.Fatalf("expected no reclaim note for project %s, got %q", project.ID, response.ReclaimNote)
	}
}

func TestHandleGetAppImagesUsesRuntimeImageLocationEvidence(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, fakeRegistry, _, newImageRef, _ := setupAppImagesTestServer(t)
	delete(fakeRegistry.images, newImageRef)
	runtimeImageRef := server.runtimeImageRefFromManagedRef(newImageRef)
	if runtimeImageRef == "" || runtimeImageRef == newImageRef {
		t.Fatalf("expected distinct runtime image ref for %q, got %q", newImageRef, runtimeImageRef)
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		ImageRef:        runtimeImageRef,
		RuntimeID:       app.Spec.RuntimeID,
		ClusterNodeName: "worker-a",
		Status:          model.ImageLocationStatusPresent,
		SizeBytes:       123,
	}); err != nil {
		t.Fatalf("upsert image location: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)

	versionByImageRef := make(map[string]appImageVersion, len(response.Versions))
	for _, version := range response.Versions {
		versionByImageRef[version.ImageRef] = version
	}
	currentVersion, ok := versionByImageRef[newImageRef]
	if !ok {
		t.Fatalf("expected current image %q in response: %#v", newImageRef, response.Versions)
	}
	if currentVersion.Status != appImageStatusAvailable {
		t.Fatalf("expected runtime image location evidence to mark current image available, got %#v", currentVersion)
	}
	if !currentVersion.RedeploySupported {
		t.Fatalf("expected current image with location evidence to be redeployable, got %#v", currentVersion)
	}
	if currentVersion.DeleteSupported {
		t.Fatalf("expected image without registry manifest to be non-deletable, got %#v", currentVersion)
	}
	if currentVersion.SizeBytes != 123 {
		t.Fatalf("expected image location size evidence, got %#v", currentVersion)
	}
	if response.MeasurementStatus != projectImageUsageMeasurementPartial {
		t.Fatalf("expected mixed registry and location evidence to be marked partial, got %#v", response)
	}
	if want := []string{projectImageUsageReasonMissingManifestEvidence}; !reflect.DeepEqual(currentVersion.SizeMeasurementReasons, want) || !reflect.DeepEqual(response.MeasurementReasons, want) {
		t.Fatalf("expected stable missing-manifest attribution, version=%#v response=%#v", currentVersion.SizeMeasurementReasons, response.MeasurementReasons)
	}
}

func TestHandleListProjectImageUsageReturnsProjectSummary(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, project, app, _, _, _, _ := setupAppImagesTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/projects/image-usage", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response projectImageUsageResponse
	mustDecodeJSON(t, recorder, &response)

	if !response.RegistryConfigured {
		t.Fatal("expected registry inventory to be configured")
	}
	if response.ReclaimRequiresGC {
		t.Fatalf("expected project image usage to report immediate cleanup, got %#v", response)
	}
	if response.ReclaimNote != "" {
		t.Fatalf("expected no reclaim note in project usage response, got %q", response.ReclaimNote)
	}
	if len(response.Projects) != 1 {
		t.Fatalf("expected one project summary, got %#v", response.Projects)
	}

	summary := response.Projects[0]
	if summary.ProjectID != project.ID {
		t.Fatalf("expected project id %q, got %#v", project.ID, summary)
	}
	if summary.VersionCount != 2 || summary.StaleVersionCount != 1 {
		t.Fatalf("expected one stale and two total versions, got %#v", summary)
	}
	if summary.TotalSizeBytes != 240 || summary.ReclaimableSizeBytes != 60 {
		t.Fatalf("expected project summary sizes 240/60, got %#v", summary)
	}
	if len(summary.Apps) != 1 {
		t.Fatalf("expected one app summary, got %#v", summary.Apps)
	}
	if summary.Apps[0].AppID != app.ID {
		t.Fatalf("expected app summary for %q, got %#v", app.ID, summary.Apps[0])
	}
}

func TestCachedProjectImageUsageColdMissWaitsForInitialSnapshot(t *testing.T) {
	t.Parallel()

	_, server, _, tenant, project, app, fakeRegistry, _, _, _ := setupAppImagesTestServer(t)
	inspectStarted := make(chan struct{}, 1)
	inspectRelease := make(chan struct{})
	fakeRegistry.inspectStarted = inspectStarted
	fakeRegistry.inspectRelease = inspectRelease

	principal := model.Principal{TenantID: tenant.ID}
	resultCh := make(chan projectImageUsageLoadResult, 1)
	go func() {
		response, err := server.cachedProjectImageUsageResponse(context.Background(), principal)
		resultCh <- projectImageUsageLoadResult{response: response, err: err}
	}()

	select {
	case <-inspectStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the initial image inventory load to start")
	}

	select {
	case result := <-resultCh:
		close(inspectRelease)
		t.Fatalf("cold-cache request returned before its first truthful snapshot: response=%#v err=%v", result.response, result.err)
	case <-time.After(2 * projectImageUsageSoftWait):
	}

	close(inspectRelease)
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("load initial project image usage: %v", result.err)
		}
		if len(result.response.Projects) != 1 {
			t.Fatalf("expected the initial snapshot to retain project %q, got %#v", project.ID, result.response.Projects)
		}
		if result.response.Projects[0].ProjectID != project.ID || len(result.response.Projects[0].Apps) != 1 || result.response.Projects[0].Apps[0].AppID != app.ID {
			t.Fatalf("expected the initial snapshot to retain project/current app, got %#v", result.response.Projects[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the initial image usage snapshot")
	}
}

func TestCachedProjectImageUsageExpiredEntryUsesStaleWhileRevalidate(t *testing.T) {
	t.Parallel()

	_, server, _, tenant, project, app, fakeRegistry, _, _, _ := setupAppImagesTestServer(t)
	principal := model.Principal{TenantID: tenant.ID}
	key := projectImageUsageCacheKey(principal)
	observedAt := time.Now().UTC().Add(-time.Hour)
	stale := projectImageUsageResponse{
		RegistryConfigured: true,
		MeasurementStatus:  projectImageUsageMeasurementComplete,
		ObservedAt:         &observedAt,
		Projects: []projectImageUsageSummary{{
			ProjectID: project.ID,
			Apps: []projectImageUsageAppSummary{{
				AppID:   app.ID,
				AppName: app.Name,
			}},
		}},
	}
	server.projectImageUsageCache.set(key, stale)
	server.projectImageUsageCache.mu.Lock()
	entry := server.projectImageUsageCache.byKey[key]
	entry.expiresAt = time.Now().Add(-time.Second)
	server.projectImageUsageCache.byKey[key] = entry
	server.projectImageUsageCache.mu.Unlock()

	inspectStarted := make(chan struct{}, 1)
	inspectRelease := make(chan struct{})
	fakeRegistry.inspectStarted = inspectStarted
	fakeRegistry.inspectRelease = inspectRelease

	resultCh := make(chan projectImageUsageLoadResult, 1)
	go func() {
		response, err := server.cachedProjectImageUsageResponse(context.Background(), principal)
		resultCh <- projectImageUsageLoadResult{response: response, err: err}
	}()

	select {
	case <-inspectStarted:
	case <-time.After(time.Second):
		close(inspectRelease)
		t.Fatal("timed out waiting for the stale image usage refresh to start")
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			close(inspectRelease)
			t.Fatalf("load stale project image usage: %v", result.err)
		}
		if !reflect.DeepEqual(result.response, stale) {
			close(inspectRelease)
			t.Fatalf("expected the expired snapshot while refresh is blocked, got %#v", result.response)
		}
	case <-time.After(5 * projectImageUsageSoftWait):
		close(inspectRelease)
		t.Fatal("timed out waiting for stale-while-revalidate response")
	}

	close(inspectRelease)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if fresh, ok := server.projectImageUsageCache.get(key); ok && !reflect.DeepEqual(fresh, stale) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the background image usage refresh to finish")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHandleListProjectImageUsageCachesRegistryFanout(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, _, _, fakeRegistry, _, _, _ := setupAppImagesTestServer(t)

	first := performJSONRequest(t, server, http.MethodGet, "/v1/projects/image-usage", apiKey, nil)
	if first.Code != http.StatusOK {
		t.Fatalf("expected first status %d, got %d body=%s", http.StatusOK, first.Code, first.Body.String())
	}

	second := performJSONRequest(t, server, http.MethodGet, "/v1/projects/image-usage", apiKey, nil)
	if second.Code != http.StatusOK {
		t.Fatalf("expected second status %d, got %d body=%s", http.StatusOK, second.Code, second.Body.String())
	}

	if got := fakeRegistry.inspectCalls.Load(); got != 2 {
		t.Fatalf("expected cached project image usage to inspect two images once, got %d calls", got)
	}
}

func TestHandleListProjectImageUsageUsesDistributedCacheInventory(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, project, app, _, oldImageRef, newImageRef, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	now := time.Now().UTC()
	oldDigest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	newDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:          "node-a",
		ClusterNodeName: "worker-a",
		ObservedAt:      now,
	}, []model.ImageCacheManifest{
		{
			Repo:              "fugue-apps/example-demo-web",
			Target:            "git-111111111111",
			Digest:            oldDigest,
			ManifestSizeBytes: 10,
			TotalBlobBytes:    160,
			LastSeenAt:        now,
			Present:           true,
		},
		{
			Repo:              "fugue-apps/example-demo-web",
			Target:            "git-222222222222",
			Digest:            newDigest,
			ManifestSizeBytes: 12,
			TotalBlobBytes:    180,
			LastSeenAt:        now,
			Present:           true,
		},
	}); err != nil {
		t.Fatalf("upsert distributed image cache inventory: %v", err)
	}
	// The same digest is present on a second node. It must not be counted twice
	// in one app/project summary.
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:          "node-b",
		ClusterNodeName: "worker-b",
		ObservedAt:      now,
	}, []model.ImageCacheManifest{{
		Repo:              "fugue-apps/example-demo-web",
		Target:            "git-222222222222",
		Digest:            newDigest,
		ManifestSizeBytes: 12,
		TotalBlobBytes:    180,
		LastSeenAt:        now,
		Present:           true,
	}}); err != nil {
		t.Fatalf("upsert duplicate distributed image cache inventory: %v", err)
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		ImageRef:        newImageRef,
		NodeID:          "node-a",
		ClusterNodeName: "worker-a",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &now,
	}); err != nil {
		t.Fatalf("upsert distributed image location: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/projects/image-usage", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response projectImageUsageResponse
	mustDecodeJSON(t, recorder, &response)
	if response.ImageStoreMode != projectImageUsageModeDistributed {
		t.Fatalf("expected distributed image store mode, got %#v", response)
	}
	if response.MeasurementStatus != projectImageUsageMeasurementComplete {
		t.Fatalf("expected complete distributed measurement, got %#v", response)
	}
	if len(response.Projects) != 1 || response.Projects[0].ProjectID != project.ID {
		t.Fatalf("expected project summary for %q, got %#v", project.ID, response.Projects)
	}
	if got := response.Projects[0].TotalSizeBytes; got != 362 {
		t.Fatalf("expected distributed logical image bytes 362, got %d (old=%s new=%s)", got, oldImageRef, newImageRef)
	}
	if response.Projects[0].MeasurementStatus != projectImageUsageMeasurementComplete {
		t.Fatalf("expected complete project measurement, got %#v", response.Projects[0])
	}
}

func TestHandleGetAppImagesUsesDistributedCacheInventory(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, _, _, newImageRef, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:          "node-a",
		ClusterNodeName: "worker-a",
		ObservedAt:      now,
	}, []model.ImageCacheManifest{{
		Repo:              "fugue-apps/example-demo-web",
		Target:            "git-222222222222",
		Digest:            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ManifestSizeBytes: 12,
		TotalBlobBytes:    180,
		LastSeenAt:        now,
		Present:           true,
	}}); err != nil {
		t.Fatalf("upsert distributed image cache inventory: %v", err)
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		ImageRef:        newImageRef,
		NodeID:          "node-a",
		ClusterNodeName: "worker-a",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &now,
	}); err != nil {
		t.Fatalf("upsert distributed image location: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Versions) != 1 || response.Versions[0].ImageRef != newImageRef {
		t.Fatalf("expected only current distributed version with cache evidence, got %#v", response.Versions)
	}
	if response.Versions[0].SizeBytes != 192 || response.Versions[0].SizeMeasurementStatus != projectImageUsageMeasurementComplete {
		t.Fatalf("expected measured distributed version size 192, got %#v", response.Versions[0])
	}
	if response.Versions[0].RedeploySupported || response.Versions[0].DeleteSupported {
		t.Fatalf("expected distributed read inventory to expose no unsupported image actions, got %#v", response.Versions[0])
	}
	if response.Summary.TotalSizeBytes != 192 {
		t.Fatalf("expected app distributed total 192, got %#v", response.Summary)
	}
	if response.Summary.ReclaimableSizeBytes != 0 || response.ReclaimNote == "" {
		t.Fatalf("expected distributed reclaimability to remain explicitly unsupported, got %#v", response)
	}
}

func TestHandleGetAppImagesUsesKnownDigestEvidenceAcrossCacheNodes(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, _, _, newImageRef, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	now := time.Now().UTC()
	digest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := stateStore.UpsertImage(model.Image{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		ImageRef:        newImageRef,
		CanonicalDigest: digest,
		LifecycleState:  model.ImageLifecycleAvailable,
	}); err != nil {
		t.Fatalf("upsert distributed image metadata: %v", err)
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		ImageRef:        newImageRef,
		NodeID:          "node-a",
		ClusterNodeName: "worker-a",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &now,
	}); err != nil {
		t.Fatalf("upsert distributed image location: %v", err)
	}
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:          "node-a",
		ClusterNodeName: "worker-a",
		ObservedAt:      now,
	}, []model.ImageCacheManifest{{
		Repo:              "fugue-apps/example-demo-web",
		Target:            "git-222222222222",
		Digest:            digest,
		ManifestSizeBytes: 12,
		LastSeenAt:        now,
		Present:           true,
	}}); err != nil {
		t.Fatalf("upsert partial cache inventory: %v", err)
	}
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:          "node-b",
		ClusterNodeName: "worker-b",
		ObservedAt:      now,
	}, []model.ImageCacheManifest{{
		Repo:              "fugue-apps/example-demo-web",
		Target:            "git-222222222222",
		Digest:            digest,
		ManifestSizeBytes: 12,
		TotalBlobBytes:    180,
		LastSeenAt:        now,
		Present:           true,
	}}); err != nil {
		t.Fatalf("upsert complete cache inventory: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Versions) != 1 || response.Versions[0].Digest != digest {
		t.Fatalf("expected known digest version, got %#v", response.Versions)
	}
	if response.Versions[0].SizeBytes != 192 || response.MeasurementStatus != projectImageUsageMeasurementComplete {
		t.Fatalf("expected complete same-digest evidence from either cache node, got %#v", response)
	}
}

func TestHandleGetAppImagesMapsSortedUniqueGraphFailureReasonsWithoutCountingIncompleteBytes(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, _, _, newImageRef, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	now := time.Now().UTC()
	digest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := stateStore.UpsertImage(model.Image{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		ImageRef:        newImageRef,
		CanonicalDigest: digest,
		LifecycleState:  model.ImageLifecycleAvailable,
	}); err != nil {
		t.Fatalf("upsert distributed image metadata: %v", err)
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		ImageRef:        newImageRef,
		NodeID:          "node-complete",
		ClusterNodeName: "worker-complete",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &now,
	}); err != nil {
		t.Fatalf("upsert distributed image location: %v", err)
	}
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:          "node-complete",
		ClusterNodeName: "worker-complete",
		ObservedAt:      now,
	}, []model.ImageCacheManifest{{
		Repo:              "fugue-apps/example-demo-web",
		Target:            "git-222222222222",
		Digest:            digest,
		ManifestSizeBytes: 12,
		TotalBlobBytes:    180,
		LastSeenAt:        now,
		Present:           true,
	}}); err != nil {
		t.Fatalf("upsert complete cache inventory: %v", err)
	}

	for index, reason := range []string{"missing_child_manifest", "missing_blob", "missing_blob"} {
		raw, err := json.Marshal(map[string]any{
			"repo":                 "fugue-apps/example-demo-web",
			"target":               "git-222222222222",
			"digest":               digest,
			"manifest_size_bytes":  999,
			"total_blob_bytes":     999,
			"referenced_blobs":     []string{"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},
			"graph_status":         "incomplete",
			"graph_failure_reason": reason,
			"last_seen_at":         now,
			"present":              true,
		})
		if err != nil {
			t.Fatalf("encode incomplete manifest: %v", err)
		}
		var manifest model.ImageCacheManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			t.Fatalf("decode incomplete manifest: %v", err)
		}
		nodeID := "node-incomplete-" + string(rune('a'+index))
		if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
			NodeID:          nodeID,
			ClusterNodeName: "worker-incomplete-" + string(rune('a'+index)),
			ObservedAt:      now,
		}, []model.ImageCacheManifest{manifest}); err != nil {
			t.Fatalf("upsert incomplete cache inventory %d: %v", index, err)
		}
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Versions) != 1 {
		t.Fatalf("expected current version, got %#v", response.Versions)
	}
	version := response.Versions[0]
	wantReasons := []string{"missing_blob", "missing_child_manifest"}
	if version.SizeBytes != 192 || version.SizeMeasurementStatus != projectImageUsageMeasurementPartial || !reflect.DeepEqual(version.SizeMeasurementReasons, wantReasons) {
		t.Fatalf("incomplete graph evidence was counted or not attributed: %#v", version)
	}
	if response.MeasurementStatus != projectImageUsageMeasurementPartial || !reflect.DeepEqual(response.MeasurementReasons, wantReasons) {
		t.Fatalf("graph failure reasons were not propagated in sorted unique form: %#v", response)
	}
}

func TestHandleGetAppImagesReportsDigestConflictAgainstKnownCanonicalDigest(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, _, _, newImageRef, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	now := time.Now().UTC()
	canonicalDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	conflictingDigest := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if _, err := stateStore.UpsertImage(model.Image{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		ImageRef:        newImageRef,
		CanonicalDigest: canonicalDigest,
		LifecycleState:  model.ImageLifecycleAvailable,
	}); err != nil {
		t.Fatalf("upsert canonical distributed image metadata: %v", err)
	}
	for _, inventory := range []struct {
		nodeID   string
		nodeName string
		digest   string
	}{
		{nodeID: "node-a", nodeName: "worker-a", digest: canonicalDigest},
		{nodeID: "node-b", nodeName: "worker-b", digest: conflictingDigest},
	} {
		if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
			NodeID:          inventory.nodeID,
			ClusterNodeName: inventory.nodeName,
			ObservedAt:      now,
		}, []model.ImageCacheManifest{{
			Repo:              "fugue-apps/example-demo-web",
			Target:            "git-222222222222",
			Digest:            inventory.digest,
			ManifestSizeBytes: 12,
			TotalBlobBytes:    180,
			LastSeenAt:        now,
			Present:           true,
		}}); err != nil {
			t.Fatalf("upsert distributed image cache inventory: %v", err)
		}
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Versions) != 1 {
		t.Fatalf("expected current version, got %#v", response.Versions)
	}
	version := response.Versions[0]
	want := []string{projectImageUsageReasonDigestConflict}
	if version.Digest != canonicalDigest || version.SizeBytes != 0 || version.SizeMeasurementStatus != projectImageUsageMeasurementUnavailable || !reflect.DeepEqual(version.SizeMeasurementReasons, want) {
		t.Fatalf("expected canonical digest with unavailable conflicting size evidence, got %#v", version)
	}
	if response.MeasurementStatus != projectImageUsageMeasurementUnavailable || !reflect.DeepEqual(response.MeasurementReasons, want) {
		t.Fatalf("expected app-level digest conflict attribution, got %#v", response)
	}
}

func TestHandleGetAppImagesDoesNotCountModelMetadataWithoutFreshPhysicalEvidence(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, _, _, newImageRef, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	digest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := stateStore.UpsertImage(model.Image{
		TenantID:          app.TenantID,
		AppID:             app.ID,
		ImageRef:          newImageRef,
		CanonicalDigest:   digest,
		ManifestSizeBytes: 12,
		BlobBytes:         180,
		LifecycleState:    model.ImageLifecycleAvailable,
	}); err != nil {
		t.Fatalf("upsert distributed image metadata: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Versions) != 1 || !response.Versions[0].Current {
		t.Fatalf("expected current generation to remain visible, got %#v", response.Versions)
	}
	version := response.Versions[0]
	if version.SizeBytes != 0 || version.SizeMeasurementStatus != projectImageUsageMeasurementUnavailable {
		t.Fatalf("expected stale model-only bytes to remain unavailable, got %#v", version)
	}
	if version.Status != appImageStatusMissing || response.Summary.TotalSizeBytes != 0 || response.MeasurementStatus != projectImageUsageMeasurementUnavailable {
		t.Fatalf("expected no physical cache claim from model metadata alone, got %#v", response)
	}
	if want := []string{projectImageUsageReasonNoStorageEvidence}; !reflect.DeepEqual(version.SizeMeasurementReasons, want) || !reflect.DeepEqual(response.MeasurementReasons, want) {
		t.Fatalf("expected stable no-storage attribution, version=%#v response=%#v", version.SizeMeasurementReasons, response.MeasurementReasons)
	}
}

func TestHandleGetAppImagesReportsMissingBlobSizeEvidence(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, _, _, _, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:          "node-a",
		ClusterNodeName: "worker-a",
		ObservedAt:      now,
	}, []model.ImageCacheManifest{{
		Repo:              "fugue-apps/example-demo-web",
		Target:            "git-222222222222",
		Digest:            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ManifestSizeBytes: 12,
		LastSeenAt:        now,
		Present:           true,
	}}); err != nil {
		t.Fatalf("upsert manifest-only cache inventory: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Versions) != 1 {
		t.Fatalf("expected current version, got %#v", response.Versions)
	}
	version := response.Versions[0]
	want := []string{projectImageUsageReasonMissingBlobSize}
	if version.SizeBytes != 12 || version.SizeMeasurementStatus != projectImageUsageMeasurementPartial || !reflect.DeepEqual(version.SizeMeasurementReasons, want) {
		t.Fatalf("expected manifest-only lower bound with a stable reason, got %#v", version)
	}
	if response.MeasurementStatus != projectImageUsageMeasurementPartial || !reflect.DeepEqual(response.MeasurementReasons, want) {
		t.Fatalf("expected app-level missing-blob-size attribution, got %#v", response)
	}
}

func TestHandleGetAppImagesRejectsConflictingCompleteSizesForSameDigest(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, _, _, _, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	now := time.Now().UTC()
	digest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	for index, blobBytes := range []int64{180, 280} {
		if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
			NodeID:          "node-" + string(rune('a'+index)),
			ClusterNodeName: "worker-" + string(rune('a'+index)),
			ObservedAt:      now,
		}, []model.ImageCacheManifest{{
			Repo:              "fugue-apps/example-demo-web",
			Target:            "git-222222222222",
			Digest:            digest,
			ManifestSizeBytes: 12,
			TotalBlobBytes:    blobBytes,
			LastSeenAt:        now,
			Present:           true,
		}}); err != nil {
			t.Fatalf("upsert conflicting same-digest cache inventory %d: %v", index, err)
		}
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Versions) != 1 {
		t.Fatalf("expected current version to remain visible, got %#v", response.Versions)
	}
	version := response.Versions[0]
	if version.SizeBytes != 0 || version.SizeMeasurementStatus != projectImageUsageMeasurementUnavailable {
		t.Fatalf("expected inconsistent complete reports to remain unavailable, got %#v", version)
	}
	if response.MeasurementStatus != projectImageUsageMeasurementUnavailable {
		t.Fatalf("expected unavailable app measurement, got %#v", response)
	}
	if want := []string{projectImageUsageReasonSizeConflict}; !reflect.DeepEqual(version.SizeMeasurementReasons, want) || !reflect.DeepEqual(response.MeasurementReasons, want) {
		t.Fatalf("expected stable size-conflict attribution, version=%#v response=%#v", version.SizeMeasurementReasons, response.MeasurementReasons)
	}
}

func TestHandleGetAppImagesRejectsConflictingUnpinnedTagSizes(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, _, _, _, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	now := time.Now().UTC()
	for index, manifest := range []model.ImageCacheManifest{
		{
			Repo:              "fugue-apps/example-demo-web",
			Target:            "git-222222222222",
			Digest:            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ManifestSizeBytes: 10,
			TotalBlobBytes:    100,
			LastSeenAt:        now,
			Present:           true,
		},
		{
			Repo:              "fugue-apps/example-demo-web",
			Target:            "git-222222222222",
			Digest:            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			ManifestSizeBytes: 20,
			TotalBlobBytes:    200,
			LastSeenAt:        now,
			Present:           true,
		},
	} {
		if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
			NodeID:          "node-" + string(rune('a'+index)),
			ClusterNodeName: "worker-" + string(rune('a'+index)),
			ObservedAt:      now,
		}, []model.ImageCacheManifest{manifest}); err != nil {
			t.Fatalf("upsert conflicting cache inventory %d: %v", index, err)
		}
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Versions) != 1 {
		t.Fatalf("expected current version to remain visible, got %#v", response.Versions)
	}
	version := response.Versions[0]
	if version.SizeBytes != 0 || version.Digest != "" || version.SizeMeasurementStatus != projectImageUsageMeasurementUnavailable {
		t.Fatalf("expected conflicting unpinned cache evidence to remain unavailable, got %#v", version)
	}
	if response.MeasurementStatus != projectImageUsageMeasurementUnavailable {
		t.Fatalf("expected unavailable app measurement, got %#v", response)
	}
	if want := []string{projectImageUsageReasonDigestConflict}; !reflect.DeepEqual(version.SizeMeasurementReasons, want) || !reflect.DeepEqual(response.MeasurementReasons, want) {
		t.Fatalf("expected stable digest-conflict attribution, version=%#v response=%#v", version.SizeMeasurementReasons, response.MeasurementReasons)
	}
}

func TestHandleGetAppImagesIgnoresStaleDistributedLocationEvidence(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, _, _, newImageRef, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	staleSeen := time.Now().UTC().Add(-defaultImageCacheInventoryTTL - time.Minute)
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		ImageRef:        newImageRef,
		NodeID:          "node-a",
		ClusterNodeName: "worker-a",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &staleSeen,
	}); err != nil {
		t.Fatalf("upsert stale image location: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Versions) != 1 || !response.Versions[0].Current {
		t.Fatalf("expected only the current generation when historical location evidence is stale, got %#v", response.Versions)
	}
	if response.MeasurementStatus != projectImageUsageMeasurementUnavailable {
		t.Fatalf("expected current generation without fresh size evidence to be unavailable, got %#v", response)
	}
	if want := []string{projectImageUsageReasonStaleInventory}; !reflect.DeepEqual(response.Versions[0].SizeMeasurementReasons, want) || !reflect.DeepEqual(response.MeasurementReasons, want) {
		t.Fatalf("expected stable stale-inventory attribution, version=%#v response=%#v", response.Versions[0].SizeMeasurementReasons, response.MeasurementReasons)
	}
}

func TestHandleListProjectImageUsageDoesNotSerializeNullProjectsWhenDistributedEvidenceMissing(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, _, _, _, _, _, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/projects/image-usage", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var raw map[string]any
	mustDecodeJSON(t, recorder, &raw)
	projects, ok := raw["projects"].([]any)
	if !ok {
		t.Fatalf("expected projects array, got %#v", raw["projects"])
	}
	if len(projects) != 1 {
		t.Fatalf("expected current app to remain visible with unavailable size evidence, got %#v", projects)
	}
	if raw["measurement_status"] != projectImageUsageMeasurementUnavailable {
		t.Fatalf("expected unavailable measurement status, got %#v", raw["measurement_status"])
	}
	var response projectImageUsageResponse
	mustDecodeJSON(t, recorder, &response)
	want := []string{projectImageUsageReasonNoStorageEvidence}
	if !reflect.DeepEqual(response.MeasurementReasons, want) || len(response.Projects) != 1 || !reflect.DeepEqual(response.Projects[0].MeasurementReasons, want) || len(response.Projects[0].Apps) != 1 || !reflect.DeepEqual(response.Projects[0].Apps[0].MeasurementReasons, want) {
		t.Fatalf("expected stable reasons at app, project, and response levels, got %#v", response)
	}
}

func TestHandleGetBillingCountsManagedImageInventoryStorage(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, tenant, _, _, _, _, _, _ := setupAppImagesTestServer(t)
	server.billingImageStorageRefresh = newBillingImageStorageRefreshScheduler(5*time.Millisecond, time.Second)
	t.Cleanup(func() {
		server.billingImageStorageRefresh.wait()
	})

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/billing", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	deadline := time.Now().Add(time.Second)
	for {
		summary, err := stateStore.GetTenantBillingSummary(tenant.ID)
		if err != nil {
			t.Fatalf("reload billing summary: %v", err)
		}
		if summary.ManagedCommitted.StorageGibibytes == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected async billing refresh to persist 1 GiB of image inventory, got %d", summary.ManagedCommitted.StorageGibibytes)
		}
		time.Sleep(10 * time.Millisecond)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/billing", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected refreshed status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Billing model.TenantBillingSummary `json:"billing"`
	}
	mustDecodeJSON(t, recorder, &response)

	if got := response.Billing.ManagedCommitted.StorageGibibytes; got != 1 {
		t.Fatalf("expected billing committed storage to include 1 GiB of image inventory, got %d", got)
	}
	if response.Billing.OverCap {
		t.Fatalf("expected default 5 GiB storage cap to absorb 1 GiB of image inventory, got %#v", response.Billing)
	}
	priceBook := model.DefaultBillingPriceBook()
	freeCap := model.DefaultTenantFreeManagedCap()
	expectedHourly := freeCap.CPUMilliCores*priceBook.CPUMicroCentsPerMilliCoreHour +
		freeCap.MemoryMebibytes*priceBook.MemoryMicroCentsPerMiBHour +
		freeCap.StorageGibibytes*priceBook.StorageMicroCentsPerGiBHour
	if response.Billing.HourlyRateMicroCents != expectedHourly {
		t.Fatalf("expected hourly rate %d with image inventory storage, got %d", expectedHourly, response.Billing.HourlyRateMicroCents)
	}

	summary, err := stateStore.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatalf("reload billing summary: %v", err)
	}
	if got := summary.ManagedCommitted.StorageGibibytes; got != 1 {
		t.Fatalf("expected synced store billing summary to retain 1 GiB image storage, got %d", got)
	}
}

func TestHandleRedeployAppImageQueuesHistoricalDeploy(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, _, app, _, oldImageRef, _, oldRuntimeImageRef := setupAppImagesTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/images/redeploy", apiKey, map[string]any{
		"image_ref": oldImageRef,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response appImageRedeployResponse
	mustDecodeJSON(t, recorder, &response)

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.Type != model.OperationTypeDeploy {
		t.Fatalf("expected deploy operation, got %#v", op)
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Image != oldRuntimeImageRef {
		t.Fatalf("expected desired runtime image %q, got %#v", oldRuntimeImageRef, op.DesiredSpec)
	}
	if op.DesiredSource == nil || op.DesiredSource.ResolvedImageRef != oldImageRef {
		t.Fatalf("expected desired source resolved image ref %q, got %#v", oldImageRef, op.DesiredSource)
	}
}

func TestHandleDeleteAppImageDeletesHistoricalRegistryVersion(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, _, app, fakeRegistry, oldImageRef, _, _ := setupAppImagesTestServer(t)
	gcRequests := 0
	server.requestRegistryGC = func(_ context.Context, reason string) error {
		gcRequests++
		if reason == "" {
			t.Fatal("expected registry GC request reason")
		}
		return nil
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/images/delete", apiKey, map[string]any{
		"image_ref": oldImageRef,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response appImageDeleteResponse
	mustDecodeJSON(t, recorder, &response)

	if !response.Deleted {
		t.Fatalf("expected delete response to mark deleted, got %#v", response)
	}
	if response.ReclaimedSizeBytes != 60 {
		t.Fatalf("expected reclaimed size estimate 60, got %#v", response)
	}
	if !response.ReclaimRequiresGC {
		t.Fatalf("expected delete response to report queued protected cleanup, got %#v", response)
	}
	if response.ReclaimNote == "" {
		t.Fatal("expected delete response to explain queued registry GC")
	}
	if len(fakeRegistry.deleted) != 1 || fakeRegistry.deleted[0] != oldImageRef {
		t.Fatalf("expected fake registry delete for %q, got %#v", oldImageRef, fakeRegistry.deleted)
	}
	if gcRequests != 1 {
		t.Fatalf("expected one registry GC request, got %d", gcRequests)
	}
}

func TestHandleDeleteAppImageReturnsBadGatewayWhenRegistryGCRequestFails(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, _, app, fakeRegistry, oldImageRef, _, _ := setupAppImagesTestServer(t)
	server.requestRegistryGC = func(context.Context, string) error {
		return errors.New("queue failed")
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/images/delete", apiKey, map[string]any{
		"image_ref": oldImageRef,
	})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadGateway, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Error string `json:"error"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Error == "" {
		t.Fatalf("expected delete error message, got %#v", response)
	}
	if len(fakeRegistry.deleted) != 1 || fakeRegistry.deleted[0] != oldImageRef {
		t.Fatalf("expected manifest delete to happen before GC failure, got %#v", fakeRegistry.deleted)
	}
}

type fakeAppImageRegistry struct {
	deleted        []string
	images         map[string]appImageRegistryInspectResult
	inspectCalls   atomic.Int64
	inspectStarted chan<- struct{}
	inspectRelease <-chan struct{}
}

func (f *fakeAppImageRegistry) InspectImage(ctx context.Context, imageRef string) (appImageRegistryInspectResult, error) {
	f.inspectCalls.Add(1)
	if f.inspectStarted != nil {
		select {
		case f.inspectStarted <- struct{}{}:
		default:
		}
	}
	if f.inspectRelease != nil {
		select {
		case <-f.inspectRelease:
		case <-ctx.Done():
			return appImageRegistryInspectResult{}, ctx.Err()
		}
	}
	if result, ok := f.images[imageRef]; ok {
		return cloneAppImageRegistryInspectResult(result), nil
	}
	return appImageRegistryInspectResult{
		ImageRef: imageRef,
		Exists:   false,
	}, nil
}

func (f *fakeAppImageRegistry) DeleteImage(_ context.Context, imageRef string) (appImageRegistryDeleteResult, error) {
	result, ok := f.images[imageRef]
	if !ok {
		return appImageRegistryDeleteResult{
			ImageRef:       imageRef,
			AlreadyMissing: true,
		}, nil
	}
	delete(f.images, imageRef)
	f.deleted = append(f.deleted, imageRef)
	return appImageRegistryDeleteResult{
		ImageRef: imageRef,
		Digest:   result.Digest,
		Deleted:  true,
	}, nil
}

func setupAppImagesTestServer(t *testing.T) (*store.Store, *Server, string, model.Tenant, model.Project, model.App, *fakeAppImageRegistry, string, string, string) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Images Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "gallery", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.deploy", "app.write", "app.delete"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	const (
		pushBase       = "registry.push.example"
		pullBase       = "registry.pull.example"
		oldCommit      = "111111111111aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		newCommit      = "222222222222bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		imageRepoPath  = "example-demo-web"
		oldImageDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
		newImageDigest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	)
	oldImageRef := pushBase + "/fugue-apps/" + imageRepoPath + ":git-111111111111"
	newImageRef := pushBase + "/fugue-apps/" + imageRepoPath + ":git-222222222222"
	oldRuntimeImageRef := pullBase + "/fugue-apps/" + imageRepoPath + ":git-111111111111"
	newRuntimeImageRef := pullBase + "/fugue-apps/" + imageRepoPath + ":git-222222222222"
	oldPinnedRuntimeImageRef := mustRuntimeDigestRefForAppImagesTest(t, pushBase, pullBase, oldImageRef, oldImageDigest)

	oldSource := model.AppSource{
		Type:              model.AppSourceTypeGitHubPublic,
		RepoURL:           "https://github.com/example/demo",
		RepoBranch:        "main",
		BuildStrategy:     model.AppBuildStrategyStaticSite,
		CommitSHA:         oldCommit,
		CommitCommittedAt: "2026-03-01T08:00:00Z",
		ImageNameSuffix:   "web",
	}
	oldSpec := model.AppSpec{
		Image:     oldRuntimeImageRef,
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", oldSpec, oldSource, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	oldDeployOp, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "tester",
		AppID:           app.ID,
		DesiredSpec:     &oldSpec,
		DesiredSource:   &oldSource,
	})
	if err != nil {
		t.Fatalf("create old deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperationWithResult(oldDeployOp.ID, "/tmp/old.yaml", "old deployed", &oldSpec, &oldSource); err != nil {
		t.Fatalf("complete old deploy operation: %v", err)
	}

	newSource := oldSource
	newSource.CommitSHA = newCommit
	newSource.CommitCommittedAt = "2026-03-02T08:00:00Z"
	newSpec := oldSpec
	newSpec.Image = newRuntimeImageRef

	newDeployOp, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "tester",
		AppID:           app.ID,
		DesiredSpec:     &newSpec,
		DesiredSource:   &newSource,
	})
	if err != nil {
		t.Fatalf("create new deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperationWithResult(newDeployOp.ID, "/tmp/new.yaml", "new deployed", &newSpec, &newSource); err != nil {
		t.Fatalf("complete new deploy operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ControlPlaneNamespace:       "fugue-system",
		ControlPlaneReleaseInstance: "fugue",
		RegistryGCLeaseName:         "fugue-fugue-registry-gc",
		RegistryPushBase:            pushBase,
		RegistryPullBase:            pullBase,
	})
	server.requestRegistryGC = func(context.Context, string) error { return nil }
	fakeRegistry := &fakeAppImageRegistry{
		images: map[string]appImageRegistryInspectResult{
			oldImageRef: {
				ImageRef:  oldImageRef,
				Digest:    oldImageDigest,
				Exists:    true,
				SizeBytes: 160,
				BlobSizes: map[string]int64{
					"sha256:manifest-old": 10,
					"sha256:config-old":   20,
					"sha256:layer-base":   100,
					"sha256:layer-old":    30,
				},
			},
			newImageRef: {
				ImageRef:  newImageRef,
				Digest:    newImageDigest,
				Exists:    true,
				SizeBytes: 180,
				BlobSizes: map[string]int64{
					"sha256:manifest-new": 10,
					"sha256:config-new":   20,
					"sha256:layer-base":   100,
					"sha256:layer-new":    50,
				},
			},
		},
	}
	server.appImageRegistry = fakeRegistry

	return s, server, apiKey, tenant, project, app, fakeRegistry, oldImageRef, newImageRef, oldPinnedRuntimeImageRef
}
