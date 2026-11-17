package model

import (
	"fmt"
	"path"
	"strings"
	"time"
)

type AppRecoveryStartRequest struct {
	AuthorizeOverlap bool              `json:"authorize_overlap"`
	StorageStrategy  string            `json:"storage_strategy"`
	RetentionSeconds *int64            `json:"retention_seconds"`
	TimeoutSeconds   int               `json:"timeout_seconds,omitempty"`
	EphemeralPaths   []string          `json:"ephemeral_paths,omitempty"`
	Probes           []AppReleaseProbe `json:"probes"`
}

type AppRecoveryContainer struct {
	Name         string `json:"name"`
	ID           string `json:"id"`
	RestartCount int    `json:"restart_count"`
}

type AppRecoveryPod struct {
	Name       string                 `json:"name"`
	UID        string                 `json:"uid"`
	Node       string                 `json:"node"`
	Containers []AppRecoveryContainer `json:"containers"`
}

// AppRecovery is a durable intent and evidence record. Private execution input
// is stored separately from the public view; never serialize it in API responses.
type AppRecovery struct {
	ID                    string                  `json:"id"`
	AppID                 string                  `json:"app_id"`
	Phase                 string                  `json:"phase"`
	Active                bool                    `json:"active"`
	Message               string                  `json:"message,omitempty"`
	ImageRef              string                  `json:"image_ref,omitempty"`
	Namespace             string                  `json:"namespace,omitempty"`
	SourceDeployment      string                  `json:"source_deployment,omitempty"`
	ReplacementDeployment string                  `json:"replacement_deployment,omitempty"`
	SourcePVC             string                  `json:"source_pvc,omitempty"`
	ReplacementPVC        string                  `json:"replacement_pvc,omitempty"`
	EvidenceStatus        string                  `json:"evidence_status,omitempty"`
	SourcePods            []AppRecoveryPod        `json:"source_pods,omitempty"`
	ProbeResults          []AppReleaseProbeResult `json:"probe_results,omitempty"`
	CutoverAt             *time.Time              `json:"cutover_at,omitempty"`
	RetentionUntil        *time.Time              `json:"retention_until,omitempty"`
	CreatedAt             time.Time               `json:"created_at"`
	UpdatedAt             time.Time               `json:"updated_at"`
}

type AppRecoveryRecord struct {
	AppRecovery
	Version                  int64                   `json:"version"`
	TenantID                 string                  `json:"tenant_id"`
	Request                  AppRecoveryStartRequest `json:"request"`
	SourceSpec               AppSpec                 `json:"source_spec"`
	ReplacementSpec          *AppSpec                `json:"replacement_spec,omitempty"`
	SourceDeploymentUID      string                  `json:"source_deployment_uid,omitempty"`
	ReplacementDeploymentUID string                  `json:"replacement_deployment_uid,omitempty"`
	ReplacementPodUID        string                  `json:"replacement_pod_uid,omitempty"`
	SourceService            string                  `json:"source_service,omitempty"`
	ReplacementService       string                  `json:"replacement_service,omitempty"`
	SourceServiceUID         string                  `json:"source_service_uid,omitempty"`
	SourceReplicaSets        map[string]string       `json:"source_replica_sets,omitempty"`
	SourceTraffic            *AppTrafficPolicy       `json:"source_traffic,omitempty"`
	SourceReleaseID          string                  `json:"source_release_id,omitempty"`
	ReleaseRequested         bool                    `json:"release_requested,omitempty"`
	Deadline                 time.Time               `json:"deadline"`
}

func RecoveryEphemeralPaths(spec AppSpec, additional []string) []string {
	out := append([]string(nil), spec.EphemeralPaths...)
	for _, candidate := range additional {
		found := false
		for _, existing := range out {
			if candidate == existing {
				found = true
				break
			}
		}
		if !found {
			out = append(out, candidate)
		}
	}
	return out
}

func ValidateRecoveryPaths(paths []string, spec AppSpec) error {
	if len(paths) > 16 {
		return fmt.Errorf("at most 16 ephemeral paths are allowed")
	}
	protected := []string{"/", "/proc", "/sys", "/dev", "/etc", "/bin", "/sbin", "/usr", "/lib", "/lib64", "/var/run"}
	if spec.Workspace != nil {
		mount := spec.Workspace.MountPath
		if mount == "" {
			mount = DefaultAppWorkspaceMountPath
		}
		protected = append(protected, mount)
	}
	for _, f := range spec.Files {
		protected = append(protected, f.Path)
	}
	if spec.PersistentStorage != nil {
		for _, m := range spec.PersistentStorage.Mounts {
			protected = append(protected, m.Path)
		}
	}
	for i, p := range paths {
		if !strings.HasPrefix(p, "/") || path.Clean(p) != p || strings.ContainsAny(p, "\x00\r\n") {
			return fmt.Errorf("ephemeral path must be a clean absolute path")
		}
		for _, q := range append(append([]string{}, protected...), paths[:i]...) {
			if q == "" {
				continue
			}
			// Root is forbidden itself; other protected paths cannot be masked.
			if p == q || q != "/" && (strings.HasPrefix(p, q+"/") || strings.HasPrefix(q, p+"/")) {
				return fmt.Errorf("ephemeral path %q overlaps protected or persistent path %q", p, q)
			}
		}
	}
	return nil
}

func (r *AppRecoveryStartRequest) Validate(spec AppSpec) error {
	if spec.Replicas != 1 || AppSpecVolumeReplicationEnabled(spec) || AppPersistentStorageSpecUsesSharedProjectRWX(spec.PersistentStorage) {
		return fmt.Errorf("recovery requires one replica and independent storage; shared project or replicated volumes require an explicit storage migration")
	}
	if !r.AuthorizeOverlap || r.StorageStrategy != "fresh" || r.RetentionSeconds == nil || *r.RetentionSeconds < 0 || *r.RetentionSeconds > 2592000 {
		return fmt.Errorf("explicit overlap authorization, fresh storage and retention_seconds (0..2592000) are required")
	}
	if r.TimeoutSeconds == 0 {
		r.TimeoutSeconds = 300
	}
	if r.TimeoutSeconds < 10 || r.TimeoutSeconds > 900 {
		return fmt.Errorf("timeout_seconds must be 10..900")
	}
	if len(r.Probes) < 1 || len(r.Probes) > 8 {
		return fmt.Errorf("one to eight explicit business probes are required")
	}
	for _, p := range r.Probes {
		if p.ExpectedStatus != 0 && (p.ExpectedStatus < 200 || p.ExpectedStatus >= 300) || p.ExpectStreamEventBeforeMillis != 0 || len(p.ExpectedBodyContains) > 4096 {
			return fmt.Errorf("business probes require a successful HTTP response and a body assertion within the first 4 KiB")
		}
		if p.Kind != "" && p.Kind != AppReleaseProbeKindHTTP {
			return fmt.Errorf("recovery probes currently require kind http")
		}
		if !strings.HasPrefix(p.Path, "/") || strings.HasPrefix(p.Path, "//") || strings.ContainsAny(p.Path, "\r\n?#") || p.ExpectedBodyContains == "" {
			return fmt.Errorf("each business probe requires a relative absolute path and expected_body_contains")
		}
		if p.TimeoutMilliseconds < 0 || p.TimeoutMilliseconds > 60000 || len(p.Body) > 65536 {
			return fmt.Errorf("probe timeout must be 0..60000 ms and body at most 64 KiB")
		}
		for _, v := range p.Headers {
			if len(v) > 8192 || strings.ContainsAny(v, "\r\n") {
				return fmt.Errorf("invalid probe header")
			}
		}
	}
	if err := ValidateRecoveryPaths(r.EphemeralPaths, spec); err != nil {
		return err
	}
	return ValidateRecoveryPaths(RecoveryEphemeralPaths(spec, r.EphemeralPaths), spec)
}
