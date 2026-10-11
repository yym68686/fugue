package managedssh

import (
	"context"
	"io"
	"net"
	"time"

	"fugue/internal/model"
)

type Target struct {
	TenantID         string `json:"tenant_id"`
	AppID            string `json:"app_id"`
	Namespace        string `json:"namespace"`
	PodName          string `json:"pod_name"`
	PodUID           string `json:"pod_uid"`
	Container        string `json:"container"`
	ContainerID      string `json:"container_id"`
	User             string `json:"user"`
	WorkingDirectory string `json:"working_directory"`
}

type KeyGrant struct {
	ID        string    `json:"id"`
	PublicKey string    `json:"public_key"`
	ExpiresAt time.Time `json:"expires_at"`
}

type ForwardRule = model.ManagedSSHForwardRule

type Configuration struct {
	Schema              string               `json:"schema"`
	Generation          uint64               `json:"generation"`
	Identity            model.AppSSHIdentity `json:"identity"`
	Target              Target               `json:"target"`
	Keys                []KeyGrant           `json:"keys"`
	ValidUntil          time.Time            `json:"valid_until"`
	AllowPTY            bool                 `json:"allow_pty"`
	AllowSFTP           bool                 `json:"allow_sftp"`
	ForwardRules        []ForwardRule        `json:"forward_rules,omitempty"`
	AllowedEnvironment  []string             `json:"allowed_environment,omitempty"`
	MaxConnections      int                  `json:"max_connections"`
	MaxConnectionsPerIP int                  `json:"max_connections_per_ip"`
	MaxChannels         int                  `json:"max_channels"`
	Draining            bool                 `json:"draining,omitempty"`
}

type Terminal struct {
	Name    string
	Columns uint32
	Rows    uint32
	Modes   map[uint8]uint32
}

type Resize struct {
	Columns uint32
	Rows    uint32
}

type SessionRequest struct {
	ID          string
	Kind        string
	Command     string
	Terminal    *Terminal
	Environment map[string]string
	Resize      <-chan Resize
	Signals     <-chan string
}

type SessionIO struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

type Exit struct {
	Code   uint32
	Signal string
}

type Capabilities struct {
	Exec    bool
	Shell   bool
	PTY     bool
	SFTP    bool
	Signals bool
	Forward bool
}

type Executor interface {
	Validate(context.Context, Target) (Capabilities, error)
	Run(context.Context, Target, SessionRequest, SessionIO) (Exit, error)
	Forward(context.Context, Target, string, uint32) (net.Conn, error)
}

type AuditEvent struct {
	Type              string    `json:"type"`
	TenantID          string    `json:"tenant_id"`
	AppID             string    `json:"app_id"`
	IdentityID        string    `json:"identity_id"`
	Generation        uint64    `json:"generation"`
	ConnectionID      string    `json:"connection_id,omitempty"`
	SessionID         string    `json:"session_id,omitempty"`
	KeyFingerprint    string    `json:"key_fingerprint,omitempty"`
	TargetPodUID      string    `json:"target_pod_uid"`
	TargetContainerID string    `json:"target_container_id"`
	Reason            string    `json:"reason,omitempty"`
	ExitCode          uint32    `json:"exit_code,omitempty"`
	OccurredAt        time.Time `json:"occurred_at"`
}

type AuditSink interface {
	Record(context.Context, AuditEvent) error
}
