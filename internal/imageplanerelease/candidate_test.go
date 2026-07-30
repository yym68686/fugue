package imageplanerelease

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/componentmanifest"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/releasecontrol"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testBaseCommit   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testTargetCommit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testImageDigest  = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	testChartDigest  = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
)

func TestBuildCandidateBindsCellArtifactManifestAndObservedFence(t *testing.T) {
	request := validCandidateRequest(t)
	manifest := validCandidateManifest(t, request)
	first, err := BuildCandidate(request, manifest)
	if err != nil {
		t.Fatalf("build candidate: %v", err)
	}
	second, err := BuildCandidate(request, append([]byte("\n"), manifest...))
	if err != nil {
		t.Fatalf("rebuild candidate: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("candidate is not deterministic:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if err := VerifyCandidate(first); err != nil {
		t.Fatalf("verify candidate: %v", err)
	}
	if first.CellLockKey != "lane/image-plane/cell/canary-a" ||
		first.SourceCommit != testTargetCommit || first.ImageDigest != testImageDigest || first.ChartDigest != testChartDigest ||
		first.ComponentPlanFence != request.ComponentPlanStatus.FencingToken ||
		first.ComponentPlanStatusDigest != request.ComponentPlanStatus.Digest ||
		first.ExecutionAllowed || first.ProductionMutationAllowed || !first.ObservationOnly || !first.RollbackRequired {
		t.Fatalf("candidate boundary drifted: %+v", first)
	}
	if !strings.HasPrefix(first.IdempotencyKey, "image-plane-shadow/canary-a/") ||
		first.Digest != DigestCandidate(first) {
		t.Fatalf("candidate identity is not digest-bound: %+v", first)
	}
}

func TestBuildCandidateAcceptsExactRealHelmRender(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}
	request := validCandidateRequest(t)
	command := exec.Command(
		"helm", "template", request.ReleaseName, "../../deploy/helm/fugue-image-plane",
		"--namespace", request.ReleaseNamespace,
		"--set", "enabled=true",
		"--set-string", "image.repository="+request.ImageRepository,
		"--set-string", "image.digest="+request.ImageDigest,
		"--set-string", "api.baseURL="+request.APIBaseURL,
		"--set-string", "runtime.port=5001",
		"--set-string", "cell.id="+request.CellID,
	)
	manifest, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render real image-plane chart: %v\n%s", err, manifest)
	}
	candidate, err := BuildCandidate(request, manifest)
	if err != nil {
		t.Fatalf("build candidate from real Helm render: %v\n%s", err, manifest)
	}
	if candidate.WorkloadName != "image-plane-canary-a-fugue-image-plane" || candidate.ReleaseNamespace != "fugue-system" {
		t.Fatalf("real render identity drifted: %+v", candidate)
	}
}

func TestCandidateRejectsRequestBindingDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*CandidateRequest)
	}{
		{name: "source", mutate: func(r *CandidateRequest) { r.SourceCommit = testBaseCommit }},
		{name: "cell", mutate: func(r *CandidateRequest) { r.CellID = "Canary-A" }},
		{name: "release", mutate: func(r *CandidateRequest) { r.ReleaseName = "image_plane" }},
		{name: "other release", mutate: func(r *CandidateRequest) { r.ReleaseName = "image-plane-other" }},
		{name: "namespace", mutate: func(r *CandidateRequest) { r.ReleaseNamespace = "Fugue-System" }},
		{name: "legacy image", mutate: func(r *CandidateRequest) { r.ImageRepository = "registry.example.test/fugue/image-cache" }},
		{name: "tag", mutate: func(r *CandidateRequest) { r.ImageRepository += ":latest" }},
		{name: "digest", mutate: func(r *CandidateRequest) { r.ImageDigest = strings.ToUpper(testImageDigest) }},
		{name: "plaintext API", mutate: func(r *CandidateRequest) { r.APIBaseURL = "http://api.example.test" }},
		{name: "credential API", mutate: func(r *CandidateRequest) { r.APIBaseURL = "https://user@api.example.test" }},
		{name: "privileged port", mutate: func(r *CandidateRequest) { r.RuntimePort = 443 }},
		{name: "chart", mutate: func(r *CandidateRequest) { r.ChartDigest = "sha256:short" }},
		{name: "status", mutate: func(r *CandidateRequest) { r.ComponentPlanStatus.FencingToken++ }},
		{name: "plan digest", mutate: func(r *CandidateRequest) {
			r.ComponentPlanStatus.PlanDigest = testChartDigest
			r.ComponentPlanStatus.Digest = DigestComponentPlanStatusV1(r.ComponentPlanStatus)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validCandidateRequest(t)
			test.mutate(&request)
			if _, err := BuildCandidate(request, validCandidateManifest(t, validCandidateRequest(t))); err == nil {
				t.Fatal("drifted request produced a candidate")
			}
		})
	}
}

func TestCandidateRejectsRenderedBoundaryDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*appsv1.DaemonSet)
	}{
		{name: "namespace", mutate: func(ds *appsv1.DaemonSet) { ds.Namespace = "other" }},
		{name: "cell label", mutate: func(ds *appsv1.DaemonSet) { ds.Labels["fugue.io/cell-id"] = "other" }},
		{name: "cell selector", mutate: func(ds *appsv1.DaemonSet) { ds.Spec.Template.Spec.NodeSelector["fugue.io/image-plane-cell"] = "other" }},
		{name: "selector widening", mutate: func(ds *appsv1.DaemonSet) { ds.Spec.Selector.MatchLabels["extra"] = "value" }},
		{name: "rolling update", mutate: func(ds *appsv1.DaemonSet) { ds.Spec.UpdateStrategy.Type = appsv1.RollingUpdateDaemonSetStrategyType }},
		{name: "image", mutate: func(ds *appsv1.DaemonSet) {
			ds.Spec.Template.Spec.Containers[0].Image = "registry.example.test/fugue/fugue-image-plane-agent@" + testChartDigest
		}},
		{name: "published port", mutate: func(ds *appsv1.DaemonSet) {
			ds.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 5001}}
		}},
		{name: "service account", mutate: func(ds *appsv1.DaemonSet) { ds.Spec.Template.Spec.ServiceAccountName = "broad" }},
		{name: "broad listen", mutate: func(ds *appsv1.DaemonSet) {
			ds.Spec.Template.Spec.Containers[0].Env[1].Value = "0.0.0.0:5001"
		}},
		{name: "host root", mutate: func(ds *appsv1.DaemonSet) { ds.Spec.Template.Spec.Volumes[0].HostPath.Path = "/" }},
		{name: "writable identity", mutate: func(ds *appsv1.DaemonSet) { ds.Spec.Template.Spec.Containers[0].VolumeMounts[1].ReadOnly = false }},
		{name: "privileged", mutate: func(ds *appsv1.DaemonSet) { *ds.Spec.Template.Spec.Containers[0].SecurityContext.Privileged = true }},
		{name: "external probe", mutate: func(ds *appsv1.DaemonSet) {
			ds.Spec.Template.Spec.Containers[0].ReadinessProbe.Exec.Command[7] = "http://api.example.test/readyz"
		}},
		{name: "broad credential", mutate: func(ds *appsv1.DaemonSet) {
			ds.Spec.Template.Spec.Containers[0].Env = append(ds.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "FUGUE_API_KEY", Value: "secret"})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validCandidateRequest(t)
			daemonSet := validCandidateDaemonSet(request)
			test.mutate(&daemonSet)
			manifest, err := json.Marshal(daemonSet)
			if err != nil {
				t.Fatalf("marshal drifted DaemonSet: %v", err)
			}
			if _, err := BuildCandidate(request, manifest); err == nil {
				t.Fatal("drifted render produced a candidate")
			}
		})
	}

	request := validCandidateRequest(t)
	twoObjects := append(validCandidateManifest(t, request), validCandidateManifest(t, request)...)
	if _, err := BuildCandidate(request, twoObjects); err == nil {
		t.Fatal("multi-object render produced a candidate")
	}
}

func TestVerifyCandidateRejectsRecomputedUnsafeState(t *testing.T) {
	request := validCandidateRequest(t)
	candidate, err := BuildCandidate(request, validCandidateManifest(t, request))
	if err != nil {
		t.Fatalf("build candidate: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*Candidate)
	}{
		{name: "execution", mutate: func(c *Candidate) { c.ExecutionAllowed = true }},
		{name: "production", mutate: func(c *Candidate) { c.ProductionMutationAllowed = true }},
		{name: "rollback", mutate: func(c *Candidate) { c.RollbackRequired = false }},
		{name: "lock", mutate: func(c *Candidate) { c.CellLockKey = "lane/control-plane" }},
		{name: "idempotency", mutate: func(c *Candidate) { c.IdempotencyKey += "-other" }},
		{name: "repository", mutate: func(c *Candidate) { c.ImageRepository = "registry.example.test/fugue/image-cache" }},
		{name: "API", mutate: func(c *Candidate) { c.APIBaseURL = "http://api.example.test" }},
		{name: "scope", mutate: func(c *Candidate) {
			c.ComponentPlanScopeKey = "component-release-plan:" + testBaseCommit + ".." + testBaseCommit
		}},
		{name: "workload", mutate: func(c *Candidate) { c.WorkloadName = "other" }},
		{name: "blocker", mutate: func(c *Candidate) {
			c.Blockers = []string{"candidate is observation-only and cannot authorize a cluster mutation"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := candidate
			mutated.Blockers = append([]string(nil), candidate.Blockers...)
			test.mutate(&mutated)
			mutated.Digest = DigestCandidate(mutated)
			if err := VerifyCandidate(mutated); err == nil {
				t.Fatal("re-digested unsafe candidate verified")
			}
		})
	}
}

func validCandidateRequest(t *testing.T) CandidateRequest {
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
	plan, err := componentmanifest.PlanChanges(manifest, []string{"cmd/fugue-image-cache/shadow_agent.go"})
	if err != nil {
		t.Fatalf("plan image-plane change: %v", err)
	}
	coordination, err := componentmanifest.BuildShadowCoordinationPlan(plan)
	if err != nil {
		t.Fatalf("build coordination plan: %v", err)
	}
	envelope, err := componentmanifest.BuildShadowArtifactEnvelope(manifest, plan, coordination, testBaseCommit, testTargetCommit)
	if err != nil {
		t.Fatalf("build component envelope: %v", err)
	}
	identity, err := envelope.ArtifactIdentity()
	if err != nil {
		t.Fatalf("component identity: %v", err)
	}
	contentHash, err := componentPlanContentHash(envelope)
	if err != nil {
		t.Fatalf("component content hash: %v", err)
	}
	releaseStatus := releasecontrol.ComponentPlanStatus{
		APIVersion:         releasecontrol.ComponentPlanStatusAPIVersion,
		Kind:               releasecontrol.ComponentPlanStatusKind,
		Policy:             releasecontrol.ComponentPlanStatusPolicy,
		State:              releasecontrol.ComponentPlanStateObserved,
		ArtifactID:         "component-plan-image-plane-test",
		ContentHash:        contentHash,
		ScopeKey:           identity.ScopeKey,
		Generation:         identity.Generation,
		PlanDigest:         plan.PlanDigest,
		CoordinationDigest: coordination.CoordinationDigest,
		ReleaseID:          "component-plan-release-test",
		LaneKey: platformsafety.ReleaseLaneKey(
			model.PlatformArtifactKindComponentReleasePlan,
			identity.ScopeKey,
			model.PlatformArtifactReleaseChannelShadow,
		),
		FencingToken:              7,
		LaneVersion:               3,
		IdempotencyKey:            coordination.IdempotencyKey,
		ObservationOnly:           true,
		ProductionMutationAllowed: false,
	}
	releaseStatus.Digest = releasecontrol.DigestComponentPlanStatus(releaseStatus)
	encodedStatus, err := json.Marshal(releaseStatus)
	if err != nil {
		t.Fatalf("marshal release-control status: %v", err)
	}
	var status ComponentPlanStatusV1
	if err := json.Unmarshal(encodedStatus, &status); err != nil {
		t.Fatalf("decode image-plane status contract: %v", err)
	}
	if err := VerifyComponentPlanStatusV1(status); err != nil {
		t.Fatalf("verify image-plane status contract: %v", err)
	}
	return CandidateRequest{
		APIVersion:            CandidateRequestAPIVersion,
		Kind:                  CandidateRequestKind,
		SourceCommit:          testTargetCommit,
		CellID:                "canary-a",
		ReleaseName:           "image-plane-canary-a",
		ReleaseNamespace:      "fugue-system",
		ImageRepository:       "registry.example.test/fugue/fugue-image-plane-agent",
		ImageDigest:           testImageDigest,
		APIBaseURL:            "https://api.fugue-system.svc.cluster.local:8443",
		RuntimePort:           5001,
		ChartDigest:           testChartDigest,
		ComponentPlanEnvelope: envelope,
		ComponentPlanStatus:   status,
	}
}

func validCandidateManifest(t *testing.T, request CandidateRequest) []byte {
	t.Helper()
	manifest, err := json.Marshal(validCandidateDaemonSet(request))
	if err != nil {
		t.Fatalf("marshal valid DaemonSet: %v", err)
	}
	return manifest
}

func validCandidateDaemonSet(request CandidateRequest) appsv1.DaemonSet {
	labels := map[string]string{
		"app.kubernetes.io/name":       "fugue-image-plane",
		"app.kubernetes.io/instance":   request.ReleaseName,
		"app.kubernetes.io/component":  "image-plane-shadow",
		"fugue.io/cell-id":             request.CellID,
		"fugue.io/release-lane":        "image-plane",
		"fugue.io/ownership-mode":      "shadow",
		"fugue.io/production-mutation": "forbidden",
	}
	selector := map[string]string{
		"app.kubernetes.io/name":      "fugue-image-plane",
		"app.kubernetes.io/instance":  request.ReleaseName,
		"app.kubernetes.io/component": "image-plane-shadow",
		"fugue.io/cell-id":            request.CellID,
	}
	automount := false
	runAsNonRoot := true
	runAsUser := int64(65532)
	runAsGroup := int64(65532)
	fsGroup := int64(65532)
	allowPrivilegeEscalation := false
	privileged := false
	readOnlyRoot := true
	revisionHistory := int32(2)
	directory := corev1.HostPathDirectory
	probe := func(path string) *corev1.Probe {
		return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{
			"/usr/bin/wget", "-q", "-T", "2", "-Y", "off", "--spider",
			"http://127.0.0.1:5001" + path,
		}}}}
	}
	return appsv1.DaemonSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DaemonSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "image-plane-canary-a-fugue-image-plane", Namespace: request.ReleaseNamespace, Labels: labels,
		},
		Spec: appsv1.DaemonSetSpec{
			RevisionHistoryLimit: &revisionHistory,
			UpdateStrategy:       appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
			Selector:             &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: &automount,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &runAsNonRoot,
						RunAsUser:    &runAsUser,
						RunAsGroup:   &runAsGroup,
						FSGroup:      &fsGroup,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					NodeSelector: map[string]string{
						"fugue.io/image-plane-shadow": "true",
						"fugue.io/image-plane-cell":   request.CellID,
					},
					Containers: []corev1.Container{{
						Name:            "image-plane-shadow",
						Image:           request.ImageRepository + "@" + request.ImageDigest,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env: []corev1.EnvVar{
							{Name: "FUGUE_API_BASE", Value: request.APIBaseURL},
							{Name: "FUGUE_IMAGE_CACHE_LISTEN_ADDR", Value: "127.0.0.1:5001"},
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &allowPrivilegeEscalation,
							Privileged:               &privileged,
							ReadOnlyRootFilesystem:   &readOnlyRoot,
							RunAsNonRoot:             &runAsNonRoot,
							RunAsUser:                &runAsUser,
							RunAsGroup:               &runAsGroup,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "shadow-state", MountPath: "/var/lib/fugue/image-cache"},
							{Name: "component-identity", MountPath: "/run/fugue/image-cache", ReadOnly: true},
						},
						StartupProbe:   probe("/healthz"),
						LivenessProbe:  probe("/healthz"),
						ReadinessProbe: probe("/fugue/cache/v1/platform-plan/readyz"),
					}},
					Volumes: []corev1.Volume{
						{
							Name: "shadow-state",
							VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
								Path: "/var/lib/fugue/image-plane-shadow", Type: &directory,
							}},
						},
						{
							Name: "component-identity",
							VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
								Path: "/run/fugue/image-cache", Type: &directory,
							}},
						},
					},
				},
			},
		},
	}
}

func TestCandidateJSONIsCanonicalAndCredentialFree(t *testing.T) {
	request := validCandidateRequest(t)
	candidate, err := BuildCandidate(request, validCandidateManifest(t, request))
	if err != nil {
		t.Fatalf("build candidate: %v", err)
	}
	encoded, err := json.Marshal(candidate)
	if err != nil {
		t.Fatalf("marshal candidate: %v", err)
	}
	if bytes.Contains(bytes.ToLower(encoded), []byte("token")) || bytes.Contains(encoded, []byte("secret")) {
		t.Fatalf("candidate contains credential-shaped material: %s", encoded)
	}
}
