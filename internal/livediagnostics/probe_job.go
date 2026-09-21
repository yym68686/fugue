package livediagnostics

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProbeRequest is the stable wire protocol passed to an independently released
// collector. Credentials are projected by Kubernetes, never embedded here.
type ProbeRequest struct {
	ProbeImage                 string            `json:"probe_image"`
	Protocol                   string            `json:"protocol"`
	SessionID                  string            `json:"session_id"`
	ProbeID                    string            `json:"probe_id"`
	ProbeDigest                string            `json:"probe_digest"`
	CatalogDigest              string            `json:"catalog_digest"`
	Target                     Target            `json:"target"`
	ContainerID                string            `json:"container_id,omitempty"`
	DurationSeconds            int               `json:"duration_seconds"`
	SampleIntervalMilliseconds int               `json:"sample_interval_milliseconds"`
	MaxOutputBytes             int               `json:"max_output_bytes"`
	Parameters                 map[string]string `json:"parameters"`
	Config                     json.RawMessage   `json:"config"`
}

type EvidenceQuality struct {
	Status    string   `json:"status"`
	Gaps      []string `json:"gaps"`
	Truncated bool     `json:"truncated"`
}

type ProbeReport struct {
	ProbeImage    string          `json:"probe_image"`
	Schema        string          `json:"schema"`
	SessionID     string          `json:"session_id"`
	ProbeID       string          `json:"probe_id"`
	ProbeDigest   string          `json:"probe_digest"`
	CatalogDigest string          `json:"catalog_digest"`
	Target        Target          `json:"target"`
	StartedAt     time.Time       `json:"started_at"`
	FinishedAt    time.Time       `json:"finished_at"`
	Quality       EvidenceQuality `json:"quality"`
	Evidence      []Evidence      `json:"evidence"`
}

type Evidence struct {
	Name       string          `json:"name"`
	Source     string          `json:"source"`
	ObservedAt time.Time       `json:"observed_at"`
	Status     string          `json:"status"`
	Error      string          `json:"error,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
}

func BuildProbeJob(c VerifiedCatalog, p Probe, target Target, sessionID, namespace, controlPath string, request StartRequest, parameters map[string]string) (batchv1.Job, error) {
	if err := c.Validate(); err != nil {
		return batchv1.Job{}, err
	}
	resolved, parameters, err := c.Resolve(p.ID+"@"+p.Digest(), parameters)
	if err != nil {
		return batchv1.Job{}, err
	}
	p = resolved
	if err := target.ValidateResolved(); err != nil {
		return batchv1.Job{}, err
	}
	allowed := false
	for _, t := range p.TargetTypes {
		if t == target.Type {
			allowed = true
		}
	}
	if !allowed {
		return batchv1.Job{}, errors.New("probe does not support target type")
	}
	if target.Namespace != "" && !c.AllowsNamespace(target.Namespace) {
		return batchv1.Job{}, errors.New("target namespace is not authorized by diagnostic catalog")
	}
	request.Kind = ProbeRegistered
	if err := request.Normalize(); err != nil {
		return batchv1.Job{}, err
	}
	if request.DurationSeconds > p.MaxDurationSeconds {
		return batchv1.Job{}, fmt.Errorf("probe duration exceeds %d seconds", p.MaxDurationSeconds)
	}
	if !probeIDPattern.MatchString(sessionID) || !probeIDPattern.MatchString(namespace) {
		return batchv1.Job{}, errors.New("invalid diagnostic job identity")
	}
	input := ProbeRequest{ProbeImage: p.Image, Protocol: CatalogProtocol, SessionID: sessionID, ProbeID: p.ID, ProbeDigest: p.Digest(), CatalogDigest: c.Digest, Target: target, ContainerID: target.ContainerID, DurationSeconds: request.DurationSeconds, SampleIntervalMilliseconds: request.SampleIntervalMilliseconds, MaxOutputBytes: 4 << 20, Parameters: parameters, Config: p.Config}
	encoded, err := json.Marshal(input)
	if err != nil {
		return batchv1.Job{}, err
	}
	if len(encoded) > 64<<10 {
		return batchv1.Job{}, errors.New("probe request exceeds 64 KiB")
	}
	zero := int32(0)
	deadline := int64(request.DurationSeconds + 45)
	readOnly, no, automount := true, false, false
	uid := int64(65532)
	dir := corev1.HostPathDirectory
	labels := map[string]string{ManagedByLabel: ManagedByValue, SessionIDLabel: sessionID, KindLabel: string(ProbeRegistered), TargetTypeLabel: string(target.Type), ControlPathLabel: controlPath}
	annotations := map[string]string{ProbeRefAnnotation: p.ID, ProbeDigestAnnotation: p.Digest(), CatalogDigestAnnotation: c.Digest, TargetPodAnnotation: target.Pod, TargetPodUIDAnnotation: target.PodUID, TargetContainerAnnotation: target.Container, TargetContainerIDAnnotation: target.ContainerID, TargetNodeAnnotation: target.Node, TargetNamespaceAnnotation: target.Namespace, TargetComponentAnnotation: target.Component, TargetProcessAnnotation: target.ProcessName, TargetImageAnnotation: target.ImageDigest, DurationAnnotation: fmt.Sprint(request.DurationSeconds), FrequencyAnnotation: fmt.Sprint(request.FrequencyHz), SampleIntervalAnnotation: fmt.Sprint(request.SampleIntervalMilliseconds)}
	job := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: sessionID, Namespace: namespace, Labels: labels, Annotations: annotations}, Spec: batchv1.JobSpec{BackoffLimit: &zero, ActiveDeadlineSeconds: &deadline, TTLSecondsAfterFinished: int32Ptr(RetentionSeconds), Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations}, Spec: corev1.PodSpec{NodeName: target.Node, AutomountServiceAccountToken: &automount, EnableServiceLinks: &no, RestartPolicy: corev1.RestartPolicyNever, TerminationGracePeriodSeconds: int64Ptr(5), Containers: []corev1.Container{{Name: DiagnosticAgentContainer, Image: p.Image, Command: []string{ProbeBinary}, Env: []corev1.EnvVar{{Name: ProbeRequestEnv, Value: string(encoded)}}, SecurityContext: &corev1.SecurityContext{Privileged: &no, ReadOnlyRootFilesystem: &readOnly, AllowPrivilegeEscalation: &no, RunAsUser: &uid, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("64Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("256Mi")}}, VolumeMounts: []corev1.VolumeMount{{Name: "scratch", MountPath: "/tmp"}}}}, Volumes: []corev1.Volume{{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: quantityPtr("32Mi")}}}}}}}}
	pod := &job.Spec.Template.Spec
	container := &pod.Containers[0]
	switch p.Profile {
	case "cluster-read":
		pod.ServiceAccountName = c.Policy.ServiceAccount
		// Explicit short-lived read-only identity; token automount remains disabled.
		expiration := int64(600)
		pod.Volumes = append(pod.Volumes, corev1.Volume{
			Name: "kube-access",
			VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				DefaultMode: int32Ptr(0444),
				Sources: []corev1.VolumeProjection{
					{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}},
					{ConfigMap: &corev1.ConfigMapProjection{
						LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
						Items:                []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
					}},
				},
			}},
		})
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "kube-access", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true})
	case "host-read", "process-profile", "kernel-profile":
		pod.HostPID = true
		root := int64(0)
		container.SecurityContext.RunAsUser = &root
		if p.Profile == "process-profile" || p.Profile == "kernel-profile" {
			container.SecurityContext.Capabilities.Add = []corev1.Capability{"SYS_PTRACE", "PERFMON", "SYSLOG"}
			// The runtime's default AppArmor policy restricts cross-profile proc
			// reads even with SYS_PTRACE. This administrator-only capability class
			// explicitly opts the temporary, signed probe out of that peer policy.
			// It remains non-privileged, read-only, bounded, and without SYS_ADMIN.
			container.SecurityContext.AppArmorProfile = &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined}
		}
		if p.Profile == "kernel-profile" {
			// Debian's additional paranoid>2 check requires SYS_ADMIN even when
			// PERFMON is present. Keep this opt-in separate from proc/journal reads.
			container.SecurityContext.Capabilities.Add = append(container.SecurityContext.Capabilities.Add, "SYS_ADMIN")
		}
		for _, m := range []struct{ name, path, mount string }{{"host-proc", "/proc", "/host/proc"}, {"host-cgroup", "/sys/fs/cgroup", "/sys/fs/cgroup"}} {
			pod.Volumes = append(pod.Volumes, corev1.Volume{Name: m.name, VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: m.path, Type: &dir}}})
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: m.name, MountPath: m.mount, ReadOnly: true})
		}
	default:
		return batchv1.Job{}, errors.New("unknown diagnostic execution profile")
	}
	return job, nil
}

func JobRunnerImage(job batchv1.Job) string {
	for _, container := range job.Spec.Template.Spec.Containers {
		if container.Name == DiagnosticAgentContainer {
			return container.Image
		}
	}
	return ""
}
