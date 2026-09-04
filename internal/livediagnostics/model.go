package livediagnostics

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"fugue/internal/model"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ManagedByLabel                    = "fugue.pro/live-diagnostics"
	ManagedByValue                    = "true"
	AppIDLabel                        = "fugue.pro/diagnostic-app-id"
	SessionIDLabel                    = "fugue.pro/diagnostic-session-id"
	KindLabel                         = "fugue.pro/diagnostic-kind"
	TargetTypeLabel                   = "fugue.pro/diagnostic-target-type"
	ControlPathLabel                  = "fugue.pro/diagnostic-control-path"
	TargetPodAnnotation               = "fugue.pro/diagnostic-target-pod"
	TargetPodUIDAnnotation            = "fugue.pro/diagnostic-target-pod-uid"
	TargetContainerAnnotation         = "fugue.pro/diagnostic-target-container"
	TargetContainerIDAnnotation       = "fugue.pro/diagnostic-target-container-id"
	TargetNodeAnnotation              = "fugue.pro/diagnostic-target-node"
	TargetNamespaceAnnotation         = "fugue.pro/diagnostic-target-namespace"
	TargetComponentAnnotation         = "fugue.pro/diagnostic-target-component"
	TargetProcessAnnotation           = "fugue.pro/diagnostic-target-process"
	TargetImageAnnotation             = "fugue.pro/diagnostic-target-image"
	DurationAnnotation                = "fugue.pro/diagnostic-duration-seconds"
	FrequencyAnnotation               = "fugue.pro/diagnostic-frequency-hz"
	SampleIntervalAnnotation          = "fugue.pro/diagnostic-sample-interval-ms"
	DiagnosticAgentContainer          = "diagnostic-agent"
	DiagnosticAgentBinary             = "/usr/local/bin/fugue-diagnostic-agent"
	RetentionSeconds            int32 = 3600
	MaxActiveGlobal                   = 4
)

type TargetType string

const (
	TargetApp               TargetType = "app"
	TargetPlatformComponent TargetType = "platform_component"
	TargetNodeProcess       TargetType = "node_process"
)

type ProbeKind string

const (
	ProbeCPUProfile    ProbeKind = "cpu-profile"
	ProbeMemoryProfile ProbeKind = "memory-profile"
	ProbeProcessSample ProbeKind = "process-snapshot"
)

type Target struct {
	Type        TargetType `json:"type"`
	AppID       string     `json:"app_id,omitempty"`
	Component   string     `json:"component,omitempty"`
	Namespace   string     `json:"namespace,omitempty"`
	Pod         string     `json:"pod,omitempty"`
	PodUID      string     `json:"pod_uid,omitempty"`
	Container   string     `json:"container,omitempty"`
	ContainerID string     `json:"-"`
	Node        string     `json:"node,omitempty"`
	ProcessName string     `json:"process_name,omitempty"`
	ImageDigest string     `json:"image_digest,omitempty"`
}

var allowedNodeProcesses = map[string]struct{}{
	"fugue-agent":               {},
	"fugue-dns":                 {},
	"fugue-drain-agent":         {},
	"fugue-edge":                {},
	"fugue-edge-control":        {},
	"fugue-edge-front":          {},
	"fugue-image-cache":         {},
	"fugue-mesh-agent":          {},
	"fugue-mesh-recovery":       {},
	"fugue-observability-pilot": {},
	"fugue-ssh-front":           {},
	"fugue-telemetry-agent":     {},
	"fugue-watchdog":            {},
	"k3s":                       {},
	"k3s-agent":                 {},
}

func NormalizeNodeProcessName(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if _, ok := allowedNodeProcesses[value]; !ok {
		return "", fmt.Errorf("node process %q is not in the diagnostic allowlist", value)
	}
	return value, nil
}

type StartRequest struct {
	Kind                       ProbeKind `json:"kind"`
	DurationSeconds            int       `json:"duration_seconds"`
	FrequencyHz                int       `json:"frequency_hz"`
	SampleIntervalMilliseconds int       `json:"sample_interval_milliseconds,omitempty"`
}

func (r *StartRequest) Normalize() error {
	r.Kind = ProbeKind(strings.ToLower(strings.TrimSpace(string(r.Kind))))
	if r.Kind == "" {
		r.Kind = ProbeCPUProfile
	}
	switch r.Kind {
	case ProbeCPUProfile, ProbeMemoryProfile, ProbeProcessSample:
	default:
		return fmt.Errorf("unsupported diagnostic kind %q", r.Kind)
	}
	if r.DurationSeconds == 0 {
		r.DurationSeconds = 60
	}
	maxDuration := 120
	if r.Kind == ProbeMemoryProfile || r.Kind == ProbeProcessSample {
		maxDuration = 360
	}
	if r.DurationSeconds < 5 || r.DurationSeconds > maxDuration {
		return fmt.Errorf("duration_seconds must be between 5 and %d for %s", maxDuration, r.Kind)
	}
	if r.FrequencyHz == 0 {
		r.FrequencyHz = 19
	}
	if r.FrequencyHz < 1 || r.FrequencyHz > 99 {
		return errors.New("frequency_hz must be between 1 and 99")
	}
	if r.SampleIntervalMilliseconds == 0 {
		r.SampleIntervalMilliseconds = 1000
	}
	if r.SampleIntervalMilliseconds < 250 || r.SampleIntervalMilliseconds > 10000 {
		return errors.New("sample_interval_milliseconds must be between 250 and 10000")
	}
	return nil
}

func (t *Target) Normalize() {
	t.Type = TargetType(strings.ToLower(strings.TrimSpace(string(t.Type))))
	t.AppID = strings.TrimSpace(t.AppID)
	t.Component = strings.TrimSpace(t.Component)
	t.Namespace = strings.TrimSpace(t.Namespace)
	t.Pod = strings.TrimSpace(t.Pod)
	t.PodUID = strings.TrimSpace(t.PodUID)
	t.Container = strings.TrimSpace(t.Container)
	t.ContainerID = strings.TrimSpace(t.ContainerID)
	t.Node = strings.TrimSpace(t.Node)
	t.ProcessName = strings.TrimSpace(t.ProcessName)
	t.ImageDigest = strings.TrimSpace(t.ImageDigest)
}

func (t Target) ValidateResolved() error {
	t.Normalize()
	switch t.Type {
	case TargetApp:
		if t.AppID == "" {
			return errors.New("resolved app diagnostic target lacks app_id")
		}
		fallthrough
	case TargetPlatformComponent:
		if t.Namespace == "" || t.Pod == "" || t.PodUID == "" || t.Container == "" || t.ContainerID == "" || t.Node == "" {
			return errors.New("resolved container diagnostic target is incomplete")
		}
	case TargetNodeProcess:
		if t.Node == "" || t.ProcessName == "" {
			return errors.New("resolved node process diagnostic target is incomplete")
		}
		if _, err := NormalizeNodeProcessName(t.ProcessName); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported diagnostic target type %q", t.Type)
	}
	return nil
}

func BuildJob(target Target, sessionID, sessionNamespace, runnerImage, controlPath string, probe StartRequest) (batchv1.Job, error) {
	target.Normalize()
	if err := target.ValidateResolved(); err != nil {
		return batchv1.Job{}, err
	}
	if err := probe.Normalize(); err != nil {
		return batchv1.Job{}, err
	}
	sessionID = strings.TrimSpace(sessionID)
	sessionNamespace = strings.TrimSpace(sessionNamespace)
	runnerImage = strings.TrimSpace(runnerImage)
	controlPath = strings.TrimSpace(controlPath)
	if sessionID == "" || sessionNamespace == "" || !strings.Contains(runnerImage, "@sha256:") {
		return batchv1.Job{}, errors.New("diagnostic session requires an id, namespace, and digest-addressed runner image")
	}
	if controlPath == "" {
		controlPath = "api"
	}
	labels := map[string]string{
		ManagedByLabel:   ManagedByValue,
		SessionIDLabel:   sessionID,
		KindLabel:        string(probe.Kind),
		TargetTypeLabel:  string(target.Type),
		ControlPathLabel: model.DNS1035Label(controlPath, "api"),
	}
	if target.AppID != "" {
		labels[AppIDLabel] = target.AppID
	}
	annotations := map[string]string{
		TargetPodAnnotation:         target.Pod,
		TargetPodUIDAnnotation:      target.PodUID,
		TargetContainerAnnotation:   target.Container,
		TargetContainerIDAnnotation: target.ContainerID,
		TargetNodeAnnotation:        target.Node,
		TargetNamespaceAnnotation:   target.Namespace,
		TargetComponentAnnotation:   target.Component,
		TargetProcessAnnotation:     target.ProcessName,
		TargetImageAnnotation:       target.ImageDigest,
		DurationAnnotation:          strconv.Itoa(probe.DurationSeconds),
		FrequencyAnnotation:         strconv.Itoa(probe.FrequencyHz),
		SampleIntervalAnnotation:    strconv.Itoa(probe.SampleIntervalMilliseconds),
	}
	args := []string{
		"--kind", string(probe.Kind),
		"--duration", strconv.Itoa(probe.DurationSeconds),
		"--frequency", strconv.Itoa(probe.FrequencyHz),
		"--sample-interval-ms", strconv.Itoa(probe.SampleIntervalMilliseconds),
	}
	if target.Type == TargetNodeProcess {
		args = append(args, "--process-name", target.ProcessName)
	} else {
		args = append(args, "--container-id", target.ContainerID)
	}
	zero := int32(0)
	analysisAllowanceSeconds := 45
	if probe.Kind == ProbeMemoryProfile {
		analysisAllowanceSeconds = 240
	}
	activeDeadline := int64(probe.DurationSeconds + analysisAllowanceSeconds)
	privileged := false
	readOnly := true
	allowPrivilegeEscalation := false
	runAsUser := int64(0)
	automount := false
	hostPathDirectory := corev1.HostPathDirectory
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: sessionID, Namespace: sessionNamespace, Labels: labels, Annotations: annotations},
		Spec: batchv1.JobSpec{
			BackoffLimit: &zero, ActiveDeadlineSeconds: &activeDeadline, TTLSecondsAfterFinished: int32Ptr(RetentionSeconds),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec: corev1.PodSpec{
					NodeName: target.Node, HostPID: true, AutomountServiceAccountToken: &automount, EnableServiceLinks: &automount,
					RestartPolicy: corev1.RestartPolicyNever, TerminationGracePeriodSeconds: int64Ptr(5),
					Containers: []corev1.Container{{
						Name: DiagnosticAgentContainer, Image: runnerImage, ImagePullPolicy: corev1.PullIfNotPresent,
						Command: []string{DiagnosticAgentBinary}, Args: args,
						SecurityContext: &corev1.SecurityContext{
							Privileged: &privileged, RunAsUser: &runAsUser, ReadOnlyRootFilesystem: &readOnly, AllowPrivilegeEscalation: &allowPrivilegeEscalation,
							Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"PERFMON", "SYS_PTRACE", "SYS_ADMIN", "SYSLOG"}},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "host-proc", MountPath: "/host/proc", ReadOnly: true},
							{Name: "host-cgroup", MountPath: "/sys/fs/cgroup", ReadOnly: true},
							{Name: "scratch", MountPath: "/tmp"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "host-proc", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/proc", Type: &hostPathDirectory}}},
						{Name: "host-cgroup", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/sys/fs/cgroup", Type: &hostPathDirectory}}},
						{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: quantityPtr("32Mi")}}},
					},
				},
			},
		},
	}
	return job, nil
}

func int32Ptr(value int32) *int32 { return &value }
func int64Ptr(value int64) *int64 { return &value }

func quantityPtr(value string) *resource.Quantity {
	result := resource.MustParse(value)
	return &result
}
