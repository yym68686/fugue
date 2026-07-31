package backuprelease_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/backuprelease"
	"fugue/internal/componentmanifest"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	testBaseCommit   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testTargetCommit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testImageDigest  = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	testChartDigest  = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	testCellKey      = "backup/app-database/0123456789abcdef"
	testReleaseName  = "backup-app-database-0123456789abcdef"
)

func TestBuildCandidateBindsCellArtifactsManifestAndLocks(t *testing.T) {
	request := validCandidateRequest(t)
	manifest := validRenderedManifest(t, request)
	first, err := backuprelease.BuildCandidate(request, manifest)
	if err != nil {
		t.Fatalf("build candidate: %v", err)
	}
	second, err := backuprelease.BuildCandidate(request, append([]byte("\n"), manifest...))
	if err != nil {
		t.Fatalf("rebuild candidate: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("candidate is not deterministic:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if err := backuprelease.VerifyCandidate(first); err != nil {
		t.Fatalf("verify candidate: %v", err)
	}
	if first.CellID != "app-database-0123456789abcdef" || first.CellLockKey != "lane/backup/cell/app-database-0123456789abcdef" ||
		first.PlanLaneLockKey != "lane/backup" || first.ReleaseName != testReleaseName || first.WorkloadName != "fugue-backup-observer-app-database-0123456789abcdef" ||
		first.ImageDigest != testImageDigest || first.ChartDigest != testChartDigest || first.ReleaseNamespace != "fugue-system" ||
		first.LKGClaimName != "fugue-backup-observer-app-database-0123456789abcdef-lkg" ||
		first.BackupSpecContract != backuprelease.BackupSpecContractV1 || first.BackupStatusContract != backuprelease.BackupStatusContractV1 ||
		first.ObserverStatusContract != backuprelease.BackupObserverStatusContractV2 || !first.ObservationOnly || first.ExecutionAllowed ||
		first.ProductionMutationAllowed || !first.RollbackRequired || !first.LastKnownGoodRequired {
		t.Fatalf("candidate boundary drifted: %+v", first)
	}
	wantLocks := []string{
		"legacy-release/fugue",
		"resource/control-plane-postgres",
		"resource/legacy-fugue-helm-release",
	}
	if !reflect.DeepEqual(first.SharedResourceLockKeys, wantLocks) ||
		!strings.HasPrefix(first.IdempotencyKey, "backup-observer-shadow/app-database-0123456789abcdef/") ||
		first.Digest != backuprelease.DigestCandidate(first) {
		t.Fatalf("candidate coordination identity drifted: %+v", first)
	}
	if !contains(first.Blockers, "production release freeze must be cleared by the unique coordinator") {
		t.Fatalf("candidate lost production freeze blocker: %+v", first.Blockers)
	}
}

func TestBuildCandidateAcceptsExactRealHelmRender(t *testing.T) {
	request := validCandidateRequest(t)
	manifest := validRenderedManifest(t, request)
	candidate, err := backuprelease.BuildCandidate(request, manifest)
	if err != nil {
		t.Fatalf("build candidate from real Helm render: %v\n%s", err, manifest)
	}
	if candidate.WorkloadName != "fugue-backup-observer-app-database-0123456789abcdef" || candidate.ReleaseNamespace != "fugue-system" {
		t.Fatalf("real render identity drifted: %+v", candidate)
	}
}

func TestCandidateRejectsRequestBindingDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*backuprelease.CandidateRequest)
	}{
		{name: "source", mutate: func(r *backuprelease.CandidateRequest) { r.SourceCommit = testBaseCommit }},
		{name: "cell", mutate: func(r *backuprelease.CandidateRequest) { r.CellKey = "backup/all/0123456789abcdef" }},
		{name: "release", mutate: func(r *backuprelease.CandidateRequest) { r.ReleaseName = "backup-other" }},
		{name: "namespace", mutate: func(r *backuprelease.CandidateRequest) { r.ReleaseNamespace = "default" }},
		{name: "legacy image", mutate: func(r *backuprelease.CandidateRequest) { r.ImageRepository = "registry.example.test/fugue/fugue-api" }},
		{name: "tag", mutate: func(r *backuprelease.CandidateRequest) { r.ImageRepository += ":latest" }},
		{name: "digest", mutate: func(r *backuprelease.CandidateRequest) { r.ImageDigest = strings.ToUpper(testImageDigest) }},
		{name: "chart", mutate: func(r *backuprelease.CandidateRequest) { r.ChartDigest = "sha256:short" }},
		{name: "plaintext API", mutate: func(r *backuprelease.CandidateRequest) { r.APIBaseURL = "http://api.example.test" }},
		{name: "credential API", mutate: func(r *backuprelease.CandidateRequest) { r.APIBaseURL = "https://user@api.example.test" }},
		{name: "spec key traversal", mutate: func(r *backuprelease.CandidateRequest) { r.SpecConfigMapKey = "../spec" }},
		{name: "token key path", mutate: func(r *backuprelease.CandidateRequest) { r.TokenSecretKey = "token/path" }},
		{name: "missing LKG claim", mutate: func(r *backuprelease.CandidateRequest) { r.LKGClaimName = "" }},
		{name: "cross-cell LKG claim", mutate: func(r *backuprelease.CandidateRequest) {
			r.LKGClaimName = "fugue-backup-observer-registry-0123456789abcdef-lkg"
		}},
		{name: "request timeout", mutate: func(r *backuprelease.CandidateRequest) { r.RequestTimeout = "20s" }},
		{name: "status fence", mutate: func(r *backuprelease.CandidateRequest) { r.ComponentPlanStatus.FencingToken++ }},
		{name: "status contract", mutate: func(r *backuprelease.CandidateRequest) { r.ComponentPlanStatus.Kind = "Other" }},
		{name: "plan lane", mutate: func(r *backuprelease.CandidateRequest) {
			r.ComponentPlanEnvelope.ChangePlan.ImpactedComponents[0].ReleaseLane = "other"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validCandidateRequest(t)
			test.mutate(&request)
			if _, err := backuprelease.BuildCandidate(request, validRenderedManifest(t, validCandidateRequest(t))); err == nil {
				t.Fatal("drifted request produced a candidate")
			}
		})
	}
}

func TestCandidateRejectsRenderedBoundaryDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*appsv1.Deployment)
	}{
		{name: "namespace", mutate: func(deployment *appsv1.Deployment) { deployment.Namespace = "other" }},
		{name: "name", mutate: func(deployment *appsv1.Deployment) { deployment.Name = "other" }},
		{name: "cell annotation", mutate: func(deployment *appsv1.Deployment) { deployment.Annotations["fugue.io/backup-cell-key"] = "other" }},
		{name: "LKG annotation", mutate: func(deployment *appsv1.Deployment) { deployment.Annotations["fugue.io/backup-lkg-claim"] = "other" }},
		{name: "Pod LKG annotation", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Annotations["fugue.io/backup-lkg-claim"] = "other"
		}},
		{name: "cell label", mutate: func(deployment *appsv1.Deployment) { deployment.Labels["fugue.io/backup-cell-id"] = "other" }},
		{name: "selector widening", mutate: func(deployment *appsv1.Deployment) { deployment.Spec.Selector.MatchLabels["extra"] = "value" }},
		{name: "replicas", mutate: func(deployment *appsv1.Deployment) { replicas := int32(2); deployment.Spec.Replicas = &replicas }},
		{name: "status", mutate: func(deployment *appsv1.Deployment) { deployment.Status.Replicas = 1 }},
		{name: "paused", mutate: func(deployment *appsv1.Deployment) { deployment.Spec.Paused = true }},
		{name: "rolling update", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Strategy.Type = appsv1.RollingUpdateDeploymentStrategyType
		}},
		{name: "published port", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8092}}
		}},
		{name: "service account", mutate: func(deployment *appsv1.Deployment) { deployment.Spec.Template.Spec.ServiceAccountName = "broad" }},
		{name: "broad listen", mutate: func(deployment *appsv1.Deployment) {
			setEnv(deployment, "FUGUE_BACKUP_OBSERVER_BIND_ADDR", "0.0.0.0:8092")
		}},
		{name: "image", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Containers[0].Image = "registry.example.test/fugue/fugue-backup-observer@" + testChartDigest
		}},
		{name: "subPath", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Containers[0].VolumeMounts[0].SubPath = "spec.json"
		}},
		{name: "mixed volume source", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Volumes[1].HostPath = &corev1.HostPathVolumeSource{Path: "/tmp"}
		}},
		{name: "broad secret mode", mutate: func(deployment *appsv1.Deployment) {
			*deployment.Spec.Template.Spec.Volumes[1].Secret.DefaultMode = 0o444
		}},
		{name: "cross-cell LKG volume", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Volumes[2].PersistentVolumeClaim.ClaimName = "fugue-backup-observer-registry-0123456789abcdef-lkg"
		}},
		{name: "read-only LKG volume", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Volumes[2].PersistentVolumeClaim.ReadOnly = true
		}},
		{name: "read-only LKG mount", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Containers[0].VolumeMounts[2].ReadOnly = true
		}},
		{name: "LKG subPath", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Containers[0].VolumeMounts[2].SubPath = "lkg.json"
		}},
		{name: "mixed LKG volume source", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Volumes[2].EmptyDir = &corev1.EmptyDirVolumeSource{}
		}},
		{name: "privileged", mutate: func(deployment *appsv1.Deployment) {
			value := true
			deployment.Spec.Template.Spec.Containers[0].SecurityContext.Privileged = &value
		}},
		{name: "unsafe sysctl", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.SecurityContext.Sysctls = []corev1.Sysctl{{Name: "net.ipv4.ip_forward", Value: "1"}}
		}},
		{name: "external probe", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.Exec.Command[0] = "/bin/sh"
		}},
		{name: "probe delay", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.InitialDelaySeconds = 3600
		}},
		{name: "secret environment", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Containers[0].Env = append(deployment.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "TOKEN", Value: "secret"})
		}},
		{name: "pull secret", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry"}}
		}},
		{name: "unbounded resources", mutate: func(deployment *appsv1.Deployment) {
			deployment.Spec.Template.Spec.Containers[0].Resources.Limits = nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validCandidateRequest(t)
			deployment := decodeDeployment(t, validRenderedManifest(t, request))
			test.mutate(&deployment)
			encoded, err := json.Marshal(deployment)
			if err != nil {
				t.Fatalf("marshal drifted Deployment: %v", err)
			}
			if _, err := backuprelease.BuildCandidate(request, encoded); err == nil {
				t.Fatal("drifted render produced a candidate")
			}
		})
	}
	request := validCandidateRequest(t)
	manifest := validRenderedManifest(t, request)
	if _, err := backuprelease.BuildCandidate(request, append(manifest, manifest...)); err == nil {
		t.Fatal("multi-object render produced a candidate")
	}
}

func TestVerifyCandidateRejectsRecomputedUnsafeState(t *testing.T) {
	request := validCandidateRequest(t)
	candidate, err := backuprelease.BuildCandidate(request, validRenderedManifest(t, request))
	if err != nil {
		t.Fatalf("build candidate: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*backuprelease.Candidate)
	}{
		{name: "execution", mutate: func(candidate *backuprelease.Candidate) { candidate.ExecutionAllowed = true }},
		{name: "production", mutate: func(candidate *backuprelease.Candidate) { candidate.ProductionMutationAllowed = true }},
		{name: "rollback", mutate: func(candidate *backuprelease.Candidate) { candidate.RollbackRequired = false }},
		{name: "cell lock", mutate: func(candidate *backuprelease.Candidate) { candidate.CellLockKey = "lane/control-plane" }},
		{name: "resource lock", mutate: func(candidate *backuprelease.Candidate) { candidate.SharedResourceLockKeys[0] = "resource/other" }},
		{name: "contract", mutate: func(candidate *backuprelease.Candidate) { candidate.BackupSpecContract = "backup/v2" }},
		{name: "LKG claim", mutate: func(candidate *backuprelease.Candidate) { candidate.LKGClaimName = "fugue-backup-observer-other-lkg" }},
		{name: "status fence", mutate: func(candidate *backuprelease.Candidate) { candidate.ComponentPlanStatus.FencingToken++ }},
		{name: "blocker", mutate: func(candidate *backuprelease.Candidate) {
			candidate.Blockers = []string{"candidate is observation-only and cannot authorize a cluster mutation"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := candidate
			mutated.SharedResourceLockKeys = append([]string(nil), candidate.SharedResourceLockKeys...)
			mutated.Blockers = append([]string(nil), candidate.Blockers...)
			test.mutate(&mutated)
			mutated.Digest = backuprelease.DigestCandidate(mutated)
			if err := backuprelease.VerifyCandidate(mutated); err == nil {
				t.Fatal("re-digested unsafe candidate verified")
			}
		})
	}
}

func TestBackupReleaseCandidateDependencyBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "fugue/internal/backuprelease")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list candidate dependencies: %v", err)
	}
	for _, dependency := range strings.Fields(string(output)) {
		for _, forbidden := range []string{
			"fugue/internal/api", "fugue/internal/auth", "fugue/internal/controller", "fugue/internal/model",
			"fugue/internal/releasecontrol", "fugue/internal/store", "database/sql", "k8s.io/client-go", "helm.sh/", "os/exec",
		} {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden+"/") {
				t.Fatalf("candidate crossed mutation boundary through %q", dependency)
			}
		}
	}
}

func validCandidateRequest(t *testing.T) backuprelease.CandidateRequest {
	t.Helper()
	manifestFile, err := os.Open("../../docs/architecture/component-ownership-v1.yaml")
	if err != nil {
		t.Fatalf("open ownership manifest: %v", err)
	}
	defer manifestFile.Close()
	manifest, err := componentmanifest.Load(manifestFile)
	if err != nil {
		t.Fatalf("load ownership manifest: %v", err)
	}
	plan, err := componentmanifest.PlanChanges(manifest, []string{"deploy/helm/fugue-backup-observer/templates/deployment.yaml"})
	if err != nil {
		t.Fatalf("plan backup chart: %v", err)
	}
	coordination, err := componentmanifest.BuildShadowCoordinationPlan(plan)
	if err != nil {
		t.Fatalf("build backup coordination: %v", err)
	}
	envelope, err := componentmanifest.BuildShadowArtifactEnvelope(manifest, plan, coordination, testBaseCommit, testTargetCommit)
	if err != nil {
		t.Fatalf("build component envelope: %v", err)
	}
	identity, err := envelope.ArtifactIdentity()
	if err != nil {
		t.Fatalf("artifact identity: %v", err)
	}
	content, err := envelope.Content()
	if err != nil {
		t.Fatalf("artifact content: %v", err)
	}
	contentBytes, err := json.Marshal(content)
	if err != nil {
		t.Fatalf("marshal artifact content: %v", err)
	}
	contentDigest := sha256.Sum256(contentBytes)
	status := backuprelease.ComponentPlanStatusV1{
		APIVersion:                backuprelease.ComponentPlanStatusAPIVersionV1,
		Kind:                      backuprelease.ComponentPlanStatusKindV1,
		Policy:                    backuprelease.ComponentPlanStatusPolicyV1,
		State:                     "observed",
		ArtifactID:                "component-release-plan-artifact",
		ContentHash:               "sha256:" + hex.EncodeToString(contentDigest[:]),
		ScopeKey:                  identity.ScopeKey,
		Generation:                identity.Generation,
		PlanDigest:                plan.PlanDigest,
		CoordinationDigest:        coordination.CoordinationDigest,
		ReleaseID:                 "release-observation-1",
		LaneKey:                   "component_release_plan|" + identity.ScopeKey + "|shadow",
		FencingToken:              7,
		LaneVersion:               3,
		IdempotencyKey:            coordination.IdempotencyKey,
		ObservationOnly:           true,
		ProductionMutationAllowed: false,
	}
	status.Digest = backuprelease.DigestComponentPlanStatusV1(status)
	return backuprelease.CandidateRequest{
		APIVersion:            backuprelease.CandidateRequestAPIVersion,
		Kind:                  backuprelease.CandidateRequestKind,
		SourceCommit:          testTargetCommit,
		CellKey:               testCellKey,
		ReleaseName:           testReleaseName,
		ReleaseNamespace:      "fugue-system",
		ImageRepository:       "registry.example.test/fugue/fugue-backup-observer",
		ImageDigest:           testImageDigest,
		ChartDigest:           testChartDigest,
		APIBaseURL:            "https://api.fugue-system.svc.cluster.local:8443",
		SpecConfigMapName:     "backup-app-database-spec",
		SpecConfigMapKey:      "desired.json",
		TokenSecretName:       "backup-app-database-token",
		TokenSecretKey:        "observer-token",
		LKGClaimName:          "fugue-backup-observer-app-database-0123456789abcdef-lkg",
		ReconcileInterval:     "30s",
		AttemptTimeout:        "20s",
		RequestTimeout:        "10s",
		ShutdownTimeout:       "10s",
		MaxResponseBytes:      65536,
		ComponentPlanEnvelope: envelope,
		ComponentPlanStatus:   status,
	}
}

func validRenderedManifest(t *testing.T, request backuprelease.CandidateRequest) []byte {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skipf("helm is required for rendered candidate tests: %v", err)
	}
	command := exec.Command(
		"helm", "template", request.ReleaseName, "../../deploy/helm/fugue-backup-observer",
		"--namespace", request.ReleaseNamespace,
		"--set", "enabled=true",
		"--set-string", "image.repository="+request.ImageRepository,
		"--set-string", "image.digest="+request.ImageDigest,
		"--set-string", "api.baseURL="+request.APIBaseURL,
		"--set-string", "cell.key="+request.CellKey,
		"--set-string", "spec.existingConfigMap.name="+request.SpecConfigMapName,
		"--set-string", "spec.existingConfigMap.key="+request.SpecConfigMapKey,
		"--set-string", "token.existingSecret.name="+request.TokenSecretName,
		"--set-string", "token.existingSecret.key="+request.TokenSecretKey,
		"--set-string", "lkg.existingClaim.name="+request.LKGClaimName,
	)
	manifest, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render backup observer chart: %v\n%s", err, manifest)
	}
	return manifest
}

func decodeDeployment(t *testing.T, manifest []byte) appsv1.Deployment {
	t.Helper()
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	object := &unstructured.Unstructured{}
	if err := decoder.Decode(object); err != nil {
		t.Fatalf("decode rendered deployment: %v", err)
	}
	encoded, err := json.Marshal(object.Object)
	if err != nil {
		t.Fatalf("marshal rendered deployment: %v", err)
	}
	var deployment appsv1.Deployment
	if err := json.Unmarshal(encoded, &deployment); err != nil {
		t.Fatalf("unmarshal rendered deployment: %v", err)
	}
	return deployment
}

func setEnv(deployment *appsv1.Deployment, name, value string) {
	for index := range deployment.Spec.Template.Spec.Containers[0].Env {
		if deployment.Spec.Template.Spec.Containers[0].Env[index].Name == name {
			deployment.Spec.Template.Spec.Containers[0].Env[index].Value = value
			return
		}
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
