package model

import "time"

type ManagedSSHForwardRule struct {
	Host    string `json:"host"`
	Port    uint32 `json:"port"`
	EndPort uint32 `json:"end_port,omitempty"`
}

type ManagedSSHIntentSpec struct {
	Enabled             bool                    `json:"enabled"`
	IdentityID          string                  `json:"identity_id"`
	Container           string                  `json:"container"`
	User                string                  `json:"user"`
	WorkingDirectory    string                  `json:"working_directory"`
	AuthorizedKeyIDs    []string                `json:"authorized_key_ids"`
	AllowPTY            bool                    `json:"allow_pty"`
	AllowSFTP           bool                    `json:"allow_sftp"`
	ForwardRules        []ManagedSSHForwardRule `json:"forward_rules,omitempty"`
	AllowedEnvironment  []string                `json:"allowed_environment,omitempty"`
	MaxConnections      int                     `json:"max_connections"`
	MaxConnectionsPerIP int                     `json:"max_connections_per_ip"`
	MaxChannels         int                     `json:"max_channels"`
}

type ManagedSSHIntent struct {
	TenantID   string               `json:"tenant_id"`
	AppID      string               `json:"app_id"`
	Generation uint64               `json:"generation"`
	Spec       ManagedSSHIntentSpec `json:"spec"`
	CreatedAt  time.Time            `json:"created_at"`
	UpdatedAt  time.Time            `json:"updated_at"`
}

type ManagedSSHPublicBinding struct {
	ID                string    `json:"id"`
	TenantID          string    `json:"tenant_id"`
	AppID             string    `json:"app_id"`
	IdentityID        string    `json:"identity_id"`
	Hostname          string    `json:"hostname"`
	PublicPort        int       `json:"public_port"`
	RuntimeID         string    `json:"runtime_id"`
	RuntimeType       string    `json:"runtime_type"`
	EdgeGroupID       string    `json:"edge_group_id,omitempty"`
	LegacyRouteDigest string    `json:"legacy_route_digest"`
	CreatedAt         time.Time `json:"created_at"`
}
