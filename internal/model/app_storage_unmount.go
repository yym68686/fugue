package model

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
)

const OperationTypeAppStorageUnmount = "app_storage_unmount"

type AppStorageUnmountPlan struct {
	AppID          string   `json:"app_id"`
	Path           string   `json:"path"`
	SpecSHA256     string   `json:"spec_sha256"`
	Allowed        bool     `json:"allowed"`
	Reason         string   `json:"reason"`
	RetainedMounts []string `json:"retained_mounts"`
	ReusePVC       bool     `json:"reuse_pvc"`
	ClaimName      string   `json:"claim_name,omitempty"`
}

type AppStorageUnmountRequest struct {
	Path             string                 `json:"path"`
	AuthorizeOverlap bool                   `json:"authorize_overlap"`
	Probe            AppStorageUnmountProbe `json:"probe"`
}

type AppStorageUnmountProbe struct {
	Path                 string `json:"path"`
	Method               string `json:"method,omitempty"`
	Body                 string `json:"body,omitempty"`
	BearerEnv            string `json:"bearer_env,omitempty"`
	ExpectedBodyContains string `json:"expected_body_contains"`
}

var storageUnmountEnvName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

func (p AppStorageUnmountProbe) Validate() error {
	u, err := url.ParseRequestURI(p.Path)
	if err != nil || !strings.HasPrefix(p.Path, "/") || strings.HasPrefix(p.Path, "//") ||
		u.IsAbs() || u.Host != "" || u.Opaque != "" || u.Path == "" || strings.HasPrefix(u.Path, "//") ||
		strings.ContainsAny(p.Path, "%\\?#\r\n\x00") || len(p.Path) > 2048 ||
		p.Path != "/" && path.Clean(p.Path) != strings.TrimSuffix(p.Path, "/") {
		return fmt.Errorf("probe path must be a clean absolute path on the application, without encoding, query or fragment")
	}
	switch p.Method {
	case "", "GET":
		if p.Body != "" {
			return fmt.Errorf("GET probe must not have a body")
		}
	case "POST":
		if len(p.Body) > 8192 || p.Body != "" && !json.Valid([]byte(p.Body)) {
			return fmt.Errorf("POST probe body must be valid JSON and at most 8192 bytes")
		}
	default:
		return fmt.Errorf("probe method must be GET or POST")
	}
	if p.ExpectedBodyContains == "" || len(p.ExpectedBodyContains) > 1024 {
		return fmt.Errorf("probe requires expected_body_contains of 1..1024 bytes")
	}
	if p.BearerEnv != "" && !storageUnmountEnvName.MatchString(p.BearerEnv) {
		return fmt.Errorf("bearer_env must name an application environment variable")
	}
	return nil
}

func (r AppStorageUnmountRequest) Validate(spec AppSpec) error {
	if !r.AuthorizeOverlap {
		return fmt.Errorf("authorize_overlap must confirm a temporary replica and safe concurrent access to the retained mounts")
	}
	if err := r.Probe.Validate(); err != nil {
		return err
	}
	if r.Probe.BearerEnv != "" && strings.TrimSpace(spec.Env[r.Probe.BearerEnv]) == "" {
		return fmt.Errorf("bearer_env must reference an existing application environment variable")
	}
	return nil
}

// AppSpecWithoutPersistentMount deliberately does not normalize, resize,
// disable, or replace storage. A caller must separately verify live placement,
// storage identity, candidate admission, and application readiness.
func AppSpecWithoutPersistentMount(spec AppSpec, mountPath string) (AppSpec, []string, error) {
	retained := []string{}
	if !strings.HasPrefix(mountPath, "/") || path.Clean(mountPath) != mountPath || mountPath == "/" ||
		strings.ContainsAny(mountPath, "\x00\r\n\\") {
		return AppSpec{}, retained, fmt.Errorf("mount path must be exact, absolute and clean")
	}
	storage := spec.PersistentStorage
	if storage == nil {
		return AppSpec{}, retained, fmt.Errorf("app has no persistent storage mounts")
	}
	next := *storage
	next.Mounts = make([]AppPersistentStorageMount, 0, len(storage.Mounts))
	matches := 0
	for _, mount := range storage.Mounts {
		if mount.Path == mountPath {
			matches++
		} else {
			next.Mounts = append(next.Mounts, mount)
			retained = append(retained, mount.Path)
		}
	}
	if matches != 1 {
		return AppSpec{}, retained, fmt.Errorf("path must match exactly one persistent mount")
	}
	if len(next.Mounts) == 0 {
		return AppSpec{}, retained, fmt.Errorf("the last mount cannot be removed with online unmount")
	}
	if spec.Replicas != 1 || !AppHasClusterService(spec) || len(spec.Ports) == 0 || spec.Ports[0] <= 0 {
		return AppSpec{}, retained, fmt.Errorf("online unmount requires an app with one replica and an HTTP cluster-service port")
	}
	if !AppPersistentStorageSpecSupportsSameNodeOnlineRollout(storage) || spec.Workspace != nil || AppSpecVolumeReplicationEnabled(spec) {
		return AppSpec{}, retained, fmt.Errorf("only an unreplicated, dedicated same-node filesystem RWO claim is supported")
	}
	if storage.ResetToken != "" {
		return AppSpec{}, retained, fmt.Errorf("storage with a reset token is not eligible for online unmount")
	}
	if AppSafeZeroDowntimeRolloutEnabled(spec) {
		return AppSpec{}, retained, fmt.Errorf("advanced traffic-managed rollout is not supported for this unmount")
	}
	if spec.NetworkPolicy != nil && spec.NetworkPolicy.Ingress != nil && NormalizeAppNetworkPolicyMode(spec.NetworkPolicy.Ingress.Mode) == AppNetworkPolicyModeRestricted {
		return AppSpec{}, retained, fmt.Errorf("controller-to-candidate business probes are not supported with restrictive app ingress")
	}
	spec.PersistentStorage = &next
	return spec, retained, nil
}
