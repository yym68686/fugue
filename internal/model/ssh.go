package model

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultAppSSHUser               = "root"
	DefaultAppSSHPort               = 22
	DefaultAppSSHAuthorizedKeysPath = "/root/.ssh/authorized_keys"
	DefaultAppSSHHostKeysPath       = "/etc/ssh/fugue-host-keys"

	DefaultAppSSHPublicPortStart = 22000
	DefaultAppSSHPublicPortEnd   = 32000
)

const (
	AppSSHEndpointStatusPending     = "pending"
	AppSSHEndpointStatusReady       = "ready"
	AppSSHEndpointStatusUnsupported = "unsupported"
	AppSSHEndpointStatusUnavailable = "unavailable"
	AppSSHEndpointStatusDisabled    = "disabled"
	AppSSHEndpointStatusReleased    = "released"
)

const (
	SSHKeyStatusActive = "active"
)

type AppSSHSpec struct {
	Enabled            bool     `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	TargetPort         int      `json:"target_port,omitempty" yaml:"target_port,omitempty"`
	User               string   `json:"user,omitempty" yaml:"user,omitempty"`
	AuthorizedKeyIDs   []string `json:"authorized_key_ids,omitempty" yaml:"authorized_key_ids,omitempty"`
	AuthorizedKeys     []string `json:"authorized_keys,omitempty" yaml:"authorized_keys,omitempty"`
	AuthorizedKeysPath string   `json:"authorized_keys_path,omitempty" yaml:"authorized_keys_path,omitempty"`
	HostKeysPath       string   `json:"host_keys_path,omitempty" yaml:"host_keys_path,omitempty"`
	AllowTCPForwarding bool     `json:"allow_tcp_forwarding,omitempty" yaml:"allow_tcp_forwarding,omitempty"`
}

type AppSSHStatus struct {
	Supported          bool   `json:"supported"`
	Ready              bool   `json:"ready"`
	Hostname           string `json:"hostname,omitempty"`
	PublicPort         int    `json:"public_port,omitempty"`
	TargetPort         int    `json:"target_port,omitempty"`
	User               string `json:"user,omitempty"`
	HostKeyFingerprint string `json:"host_key_fingerprint,omitempty"`
	Message            string `json:"message,omitempty"`
}

type AppSSHEndpoint struct {
	ID                 string     `json:"id"`
	TenantID           string     `json:"tenant_id"`
	ProjectID          string     `json:"project_id,omitempty"`
	AppID              string     `json:"app_id"`
	RuntimeID          string     `json:"runtime_id,omitempty"`
	RuntimeType        string     `json:"runtime_type,omitempty"`
	EdgeGroupID        string     `json:"edge_group_id,omitempty"`
	Hostname           string     `json:"hostname"`
	PublicPort         int        `json:"public_port"`
	TargetNamespace    string     `json:"target_namespace,omitempty"`
	TargetService      string     `json:"target_service,omitempty"`
	TargetHost         string     `json:"target_host,omitempty"`
	TargetPort         int        `json:"target_port"`
	User               string     `json:"user"`
	Status             string     `json:"status"`
	StatusReason       string     `json:"status_reason,omitempty"`
	HostKeyFingerprint string     `json:"host_key_fingerprint,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	ReleasedAt         *time.Time `json:"released_at,omitempty"`
}

type SSHKey struct {
	ID          string     `json:"id"`
	TenantID    string     `json:"tenant_id"`
	Label       string     `json:"label"`
	PublicKey   string     `json:"public_key,omitempty"`
	Fingerprint string     `json:"fingerprint"`
	Status      string     `json:"status"`
	Comment     string     `json:"comment,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
}

type EdgeSSHRouteBundle struct {
	SchemaVersion string            `json:"schema_version,omitempty"`
	Version       string            `json:"version"`
	GeneratedAt   time.Time         `json:"generated_at"`
	ValidUntil    time.Time         `json:"valid_until,omitempty"`
	Issuer        string            `json:"issuer,omitempty"`
	KeyID         string            `json:"key_id,omitempty"`
	Signature     string            `json:"signature,omitempty"`
	Signatures    []BundleSignature `json:"signatures,omitempty"`
	EdgeID        string            `json:"edge_id,omitempty"`
	EdgeGroupID   string            `json:"edge_group_id,omitempty"`
	Routes        []EdgeSSHRoute    `json:"routes"`
}

type EdgeSSHRoute struct {
	AppID           string `json:"app_id"`
	TenantID        string `json:"tenant_id"`
	RuntimeID       string `json:"runtime_id,omitempty"`
	RuntimeType     string `json:"runtime_type,omitempty"`
	EdgeGroupID     string `json:"edge_group_id,omitempty"`
	Hostname        string `json:"hostname"`
	PublicPort      int    `json:"public_port"`
	TargetHost      string `json:"target_host"`
	TargetPort      int    `json:"target_port"`
	User            string `json:"user"`
	Status          string `json:"status"`
	StatusReason    string `json:"status_reason,omitempty"`
	RouteGeneration string `json:"route_generation,omitempty"`
}

func NormalizeAppSSHSpec(spec *AppSSHSpec) *AppSSHSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	if !out.Enabled {
		return nil
	}
	if out.TargetPort <= 0 || out.TargetPort > 65535 {
		out.TargetPort = DefaultAppSSHPort
	}
	out.User = strings.TrimSpace(out.User)
	if out.User == "" {
		out.User = DefaultAppSSHUser
	}
	out.AuthorizedKeysPath = strings.TrimSpace(out.AuthorizedKeysPath)
	if out.AuthorizedKeysPath == "" {
		out.AuthorizedKeysPath = DefaultAppSSHAuthorizedKeysPath
	}
	out.HostKeysPath = strings.TrimSpace(out.HostKeysPath)
	if out.HostKeysPath == "" {
		out.HostKeysPath = DefaultAppSSHHostKeysPath
	}
	out.AuthorizedKeyIDs = normalizeStringList(out.AuthorizedKeyIDs)
	out.AuthorizedKeys = NormalizeSSHPublicKeys(out.AuthorizedKeys)
	return &out
}

func AppSSHEnabled(spec AppSpec) bool {
	return spec.SSH != nil && spec.SSH.Enabled
}

func AppSSHTargetPort(spec AppSpec) int {
	ssh := NormalizeAppSSHSpec(spec.SSH)
	if ssh == nil {
		return 0
	}
	return ssh.TargetPort
}

func AppSSHUser(spec AppSpec) string {
	ssh := NormalizeAppSSHSpec(spec.SSH)
	if ssh == nil {
		return ""
	}
	return ssh.User
}

func AppSSHAuthorizedKeysPath(spec AppSpec) string {
	ssh := NormalizeAppSSHSpec(spec.SSH)
	if ssh == nil {
		return ""
	}
	return ssh.AuthorizedKeysPath
}

func AppSSHHostKeysPath(spec AppSpec) string {
	ssh := NormalizeAppSSHSpec(spec.SSH)
	if ssh == nil {
		return ""
	}
	return ssh.HostKeysPath
}

func NormalizeAppSSHEndpointStatus(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case AppSSHEndpointStatusPending:
		return AppSSHEndpointStatusPending
	case AppSSHEndpointStatusReady:
		return AppSSHEndpointStatusReady
	case AppSSHEndpointStatusUnsupported:
		return AppSSHEndpointStatusUnsupported
	case AppSSHEndpointStatusUnavailable:
		return AppSSHEndpointStatusUnavailable
	case AppSSHEndpointStatusDisabled:
		return AppSSHEndpointStatusDisabled
	case AppSSHEndpointStatusReleased:
		return AppSSHEndpointStatusReleased
	default:
		return ""
	}
}

func NormalizeSSHPublicKey(raw string) (string, error) {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) < 2 {
		return "", fmt.Errorf("ssh public key must include key type and key data")
	}
	switch fields[0] {
	case "ssh-ed25519", "ssh-rsa", "ecdsa-sha2-nistp256", "ecdsa-sha2-nistp384", "ecdsa-sha2-nistp521":
	default:
		return "", fmt.Errorf("unsupported ssh public key type %q", fields[0])
	}
	if strings.Contains(fields[1], "PRIVATE") {
		return "", fmt.Errorf("private keys are not accepted")
	}
	if _, err := base64.StdEncoding.DecodeString(fields[1]); err != nil {
		return "", fmt.Errorf("invalid ssh public key data")
	}
	return strings.Join(fields, " "), nil
}

func NormalizeSSHPublicKeys(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	out := make([]string, 0, len(keys))
	seen := map[string]struct{}{}
	for _, key := range keys {
		normalized, err := NormalizeSSHPublicKey(key)
		if err != nil {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func SSHPublicKeyFingerprint(publicKey string) (string, error) {
	normalized, err := NormalizeSSHPublicKey(publicKey)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(normalized)
	if len(fields) < 2 {
		return "", fmt.Errorf("ssh public key must include key type and key data")
	}
	keyBytes, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return "", fmt.Errorf("invalid ssh public key data")
	}
	sum := sha256.Sum256(keyBytes)
	return "SHA256:" + base64.RawStdEncoding.EncodeToString(sum[:]), nil
}

func normalizeStringList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
