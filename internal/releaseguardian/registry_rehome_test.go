package releaseguardian

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

func TestDraftRegistryRehomeReadsBothImagesAndDoesNotGrantSharedServiceWrites(t *testing.T) {
	for _, mode := range []string{"verified", "rbac", "rbac changed", "rbac aggregated", "service changed", "additional workload", "foreign container image", "missing rollback layer", "wrong digest", "wrong mirror"} {
		t.Run(mode, func(t *testing.T) {
			objects := map[string]registryTestContent{}
			const repoPath = "fixture/rehome"
			rollbackImage, rollbackLayer := registryImageFixture(t, objects, repoPath, strings.Repeat("2", 40), false)
			forwardImage, forwardLayer := registryImageFixture(t, objects, repoPath, strings.Repeat("3", 40), true)
			for _, image := range []declarativerelease.ArtifactReceipt{rollbackImage, forwardImage} {
				prefix := "/v2/" + repoPath + "/manifests/"
				objects[prefix+"fugue-recovery-sha256-"+strings.TrimPrefix(image.TopDigest, "sha256:")] = objects[prefix+image.TopDigest]
			}
			if mode == "missing rollback layer" {
				delete(objects, rollbackLayer)
			}
			raw, currentRaw, rollbackRaw := rehomeArchiveFixture(t, rollbackImage, forwardImage, mode)
			pin := digest(raw)
			if mode == "wrong digest" {
				pin = otherDigest
			}
			repo := forwardImage.Repository
			if mode == "wrong mirror" {
				repo = "elsewhere.invalid/fixture/rehome"
			}
			reads := map[string]int{}
			registry, err := NewIndependentRegistry(testNativeRegistry, nil, registryRoundTrip(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet || req.URL.Host != testNativeRegistry || req.URL.Scheme != "https" {
					t.Errorf("unexpected request: %s %s", req.Method, req.URL)
				}
				reads[req.URL.Path]++
				content, found := objects[req.URL.Path]
				code := http.StatusOK
				if !found && req.URL.Path != "/v2/" {
					code = http.StatusNotFound
				}
				header := http.Header{"Content-Type": {string(content.mediaType)}}
				return &http.Response{StatusCode: code, Header: header, ContentLength: int64(len(content.body)),
					Body: io.NopCloser(bytes.NewReader(content.body)), Request: req}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			draft, err := registry.DraftRegistryRehome(context.Background(), raw, pin, repo, repo)
			if mode != "verified" && mode != "rbac" {
				want := map[string]string{"service changed": "cannot introduce, remove or change a Service", "additional workload": "can only carry Service or static RBAC companions",
					"rbac changed": "cannot introduce, remove or change", "rbac aggregated": "explicit static ClusterRole rules",
					"foreign container image": "outside its artifact approval", "missing rollback layer": "verify rollback recovery mirror",
					"wrong digest": "independently pinned digest", "wrong mirror": "configured independent origin"}[mode]
				if err == nil || !strings.Contains(err.Error(), want) || !reflect.DeepEqual(draft, RegistryRehomeDraft{}) {
					t.Fatalf("unsafe draft was accepted: %+v err=%v", draft, err)
				}
				if mode != "missing rollback layer" && len(reads) != 0 {
					t.Fatalf("invalid input reached the registry: %v", reads)
				}
				return
			}
			if err != nil || draft.ArchiveDigest != pin || draft.Workload.Name != "edge-control-de" ||
				len(draft.Services) != 1 || draft.Services[0].Identity.Name != "test-tls" ||
				draft.Current.SourceManifestDigest != digest(currentRaw) || draft.Rollback.SourceManifestDigest != digest(rollbackRaw) ||
				draft.Current.IndependentImage != forwardImage.ImmutableRef || draft.Rollback.IndependentImage != rollbackImage.ImmutableRef ||
				reads[forwardLayer] != 1 || reads[rollbackLayer] != 1 {
				t.Fatalf("incomplete draft: %+v requests=%v err=%v", draft, reads, err)
			}
			if mode == "rbac" && (draft.Schema != registryRehomeDraftV2 || len(draft.RBAC) != 2 ||
				draft.RBAC[0].Identity.Kind != "ClusterRole" || draft.RBAC[1].Identity.Kind != "ClusterRoleBinding") {
				t.Fatalf("incomplete RBAC companion draft: %+v", draft)
			}
			if mode == "verified" && (draft.Schema != registryRehomeDraftV1 || len(draft.RBAC) != 0) {
				t.Fatalf("legacy draft changed formats: %+v", draft)
			}
			for _, projection := range []RegistryRehomeProjection{draft.Current, draft.Rollback} {
				want := []RegistryRehomeContainer{{Field: "containers", Name: "edge-control"}, {Field: "initContainers", Name: "fixture-init"}}
				if !reflect.DeepEqual(projection.Containers, want) || projection.SourcePodSpecDigest == projection.TargetPodSpecDigest ||
					!strings.HasSuffix(projection.SourceImage, "@"+strings.Split(projection.IndependentImage, "@")[1]) {
					t.Fatalf("draft changed more than registry identity or lost a container: %+v", projection)
				}
			}
			if digest(raw) != pin {
				t.Fatal("original sealed archive was mutated")
			}
			draftPin := draft.DraftDigest
			draft.DraftDigest = ""
			unsigned, err := declarativerelease.CanonicalJSON(draft)
			if err != nil || digest(unsigned) != draftPin {
				t.Fatal("review digest did not bind the complete rehome draft")
			}
		})
	}
}

func rehomeArchiveFixture(t *testing.T, rollbackImage, forwardImage declarativerelease.ArtifactReceipt, mode string) ([]byte, []byte, []byte) {
	t.Helper()
	key := Key{Component: "api", Group: "global"}
	now := time.Unix(100, 0).UTC()
	ancestor, _, previousArtifact, predecessor := guardianStableFixture(t, key, now)
	makeRelease := func(image declarativerelease.ArtifactReceipt, previous declarativerelease.TargetIdentity, previousDigest string, previousRaw []byte, candidate bool) (archivedRecoveryRecord, declarativerelease.MonitorRecord, declarativerelease.TargetIdentity, []byte) {
		release := guardianPlanRelease(key, image.OCIRevision, previous.ConfigSHA, previousDigest, nil)
		plan := guardianPlan(t, previous.ConfigSHA, image.OCIRevision, release)
		receipt, err := declarativerelease.MaterializeArtifactReceipt(plan, key.Component, declarativerelease.RegistryVerification{
			Image: release.Artifact.Repository + "@" + image.TopDigest, IndexDigest: image.TopDigest, ManifestDigest: image.PlatformManifestDigest,
			ConfigDigest: image.ConfigDigest, OCIRevision: image.OCIRevision, Platform: image.Platform, Verification: "registry_manifest_config_and_layer_get",
			BlobCount: 2, LayerProbeCount: 1, RequestCount: 4, TotalLayerBytes: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		canonical, _ := guardianManifests(t, plan, receipt)
		set, err := declarativerelease.DecodeResourceSet(bytes.NewReader(canonical))
		if err != nil {
			t.Fatal(err)
		}
		deployment, _ := set.Primary(release.Workload)
		pod := deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		initImage := receipt.ImmutableRef
		if candidate && mode == "foreign container image" {
			initImage = "ghcr.io/example/foreign@" + image.TopDigest
		}
		pod["initContainers"] = []any{map[string]any{"name": "fixture-init", "image": initImage}}
		port := 8443
		if candidate && mode == "service changed" {
			port++
		}
		service := map[string]any{"apiVersion": "v1", "kind": "Service",
			"metadata": map[string]any{"name": "test-tls", "namespace": release.Workload.Namespace,
				"annotations": map[string]any{"fugue.pro/production-config-sha": image.OCIRevision, "fugue.pro/release-plan-digest": plan.PlanDigest, "fugue.pro/artifact-receipt-digest": receipt.ReceiptDigest}},
			"spec": map[string]any{"ports": []any{map[string]any{"port": port}}}}
		if candidate && mode == "additional workload" {
			service["kind"] = "ConfigMap"
		}
		if strings.HasPrefix(mode, "rbac") {
			verbs := []any{"get"}
			if candidate && mode == "rbac changed" {
				verbs = append(verbs, "list")
			}
			role := map[string]any{"apiVersion": registryRehomeRBACVersion, "kind": "ClusterRole",
				"metadata": map[string]any{"name": "fixture-read"},
				"rules":    []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"pods"}, "verbs": verbs}}}
			if mode == "rbac aggregated" {
				role["aggregationRule"] = map[string]any{"clusterRoleSelectors": []any{}}
			}
			binding := map[string]any{"apiVersion": registryRehomeRBACVersion, "kind": "ClusterRoleBinding",
				"metadata": map[string]any{"name": "fixture-bind"},
				"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "fixture-read"},
				"subjects": []any{map[string]any{"kind": "ServiceAccount", "name": "fixture-sa", "namespace": release.Workload.Namespace}}}
			set.Items = append(set.Items, role, binding)
		}
		set.Items = append(set.Items, service)
		canonical, err = declarativerelease.CanonicalJSON(set)
		if err != nil {
			t.Fatal(err)
		}
		target := declarativerelease.TargetIdentity{Present: true, ImageRef: receipt.ImmutableRef, ConfigSHA: image.OCIRevision,
			ManifestSHA: image.OCIRevision, OCIRevision: image.OCIRevision, ManifestDigest: digest(canonical)}
		prepared := guardianPrepared(t, plan, release, receipt, target, previous, now)
		files := map[string][]byte{"forward.json": canonical, "lkg.json": previousRaw}
		for name, value := range map[string]any{"release-plan.json": plan, "artifact-receipt.json": receipt, "execution-plan.json": prepared} {
			files[name], _ = declarativerelease.CanonicalJSON(value)
		}
		data, monitor, _ := sealedRecoverySuccessor(t, files)
		ref := RecoveryRecordRef{Name: monitorRecordNameFromDigest(key.Component, monitor.RecordDigest), UID: image.OCIRevision, ResourceVersion: "10"}
		return archivedRecoveryRecord{Ref: ref, Files: data}, monitor, target, canonical
	}
	rollback, _, rollbackTarget, rollbackRaw := makeRelease(rollbackImage, predecessor, previousArtifact.TopDigest, []byte(ancestor["forward.json"]), false)
	forward, monitor, _, forwardRaw := makeRelease(forwardImage, rollbackTarget, rollbackImage.TopDigest, rollbackRaw, true)
	state, _, err := declarativerelease.NewMonitorState(monitor, declarativerelease.MonitorState{}, true, "", now)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := declarativerelease.CanonicalJSON(sealedRecoveryArchive{Schema: recoveryArchiveSchema, Namespace: "fugue-system",
		Component: key.Component, Pointer: RecoveryRecordRef{Name: "fugue-release-monitor-api", UID: "pointer", ResourceVersion: "10"},
		State: state, Forward: forward, Rollback: rollback})
	if err != nil {
		t.Fatal(err)
	}
	return raw, forwardRaw, rollbackRaw
}
