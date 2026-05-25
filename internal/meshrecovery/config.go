package meshrecovery

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type RecoveryConfig struct {
	ListenAddr         string
	StatePath          string
	SeedPath           string
	TLSCertFile        string
	TLSKeyFile         string
	Generation         string
	PreviousGeneration string
	Mode               string
	LoginServer        string
	Message            string
	Issuer             string
	SigningKey         string
	SigningKeyID       string
	Token              string
	RejoinAuthKey      string
	DirectoryValidFor  time.Duration
	ManifestValidFor   time.Duration
	NodeTTL            time.Duration
}

type MeshAgentConfig struct {
	Endpoints             []string
	Token                 string
	SigningKey            string
	SigningKeyID          string
	CACertFile            string
	TLSInsecureSkipVerify bool
	StatePath             string
	DirectoryPath         string
	GenerationPath        string
	PollInterval          time.Duration
	HTTPTimeout           time.Duration
	RejoinEnabled         bool
	TailscaleBin          string
	TailscaleArgs         []string
	LoginServer           string
	Node                  MeshNode
}

func RecoveryFromEnv() RecoveryConfig {
	return RecoveryConfig{
		ListenAddr:         envString("FUGUE_MESH_RECOVERY_LISTEN_ADDR", "127.0.0.1:7840"),
		StatePath:          envString("FUGUE_MESH_RECOVERY_STATE_PATH", "/var/lib/fugue/mesh-recovery/state.json"),
		SeedPath:           strings.TrimSpace(os.Getenv("FUGUE_MESH_RECOVERY_SEED_PATH")),
		TLSCertFile:        strings.TrimSpace(os.Getenv("FUGUE_MESH_RECOVERY_TLS_CERT_FILE")),
		TLSKeyFile:         strings.TrimSpace(os.Getenv("FUGUE_MESH_RECOVERY_TLS_KEY_FILE")),
		Generation:         envString("FUGUE_MESH_RECOVERY_GENERATION", "meshgen-initial"),
		PreviousGeneration: strings.TrimSpace(os.Getenv("FUGUE_MESH_RECOVERY_PREVIOUS_GENERATION")),
		Mode:               envString("FUGUE_MESH_RECOVERY_MODE", GenerationModeNormal),
		LoginServer:        strings.TrimSpace(os.Getenv("FUGUE_MESH_RECOVERY_LOGIN_SERVER")),
		Message:            strings.TrimSpace(os.Getenv("FUGUE_MESH_RECOVERY_MESSAGE")),
		Issuer:             envString("FUGUE_MESH_RECOVERY_ISSUER", DefaultIssuer),
		SigningKey:         strings.TrimSpace(os.Getenv("FUGUE_MESH_RECOVERY_SIGNING_KEY")),
		SigningKeyID:       envString("FUGUE_MESH_RECOVERY_SIGNING_KEY_ID", "mesh-recovery"),
		Token:              strings.TrimSpace(os.Getenv("FUGUE_MESH_RECOVERY_TOKEN")),
		RejoinAuthKey:      strings.TrimSpace(os.Getenv("FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY")),
		DirectoryValidFor:  envDuration("FUGUE_MESH_RECOVERY_DIRECTORY_VALID_FOR", 2*time.Minute),
		ManifestValidFor:   envDuration("FUGUE_MESH_RECOVERY_MANIFEST_VALID_FOR", 2*time.Minute),
		NodeTTL:            envDuration("FUGUE_MESH_RECOVERY_NODE_TTL", 2*time.Minute),
	}
}

func MeshAgentFromEnv() MeshAgentConfig {
	nodeID := envString("FUGUE_MESH_AGENT_NODE_ID", hostname())
	return MeshAgentConfig{
		Endpoints:             envList("FUGUE_MESH_AGENT_ENDPOINTS"),
		Token:                 envFirst("FUGUE_MESH_AGENT_TOKEN", "FUGUE_MESH_RECOVERY_TOKEN"),
		SigningKey:            envFirst("FUGUE_MESH_AGENT_SIGNING_KEY", "FUGUE_MESH_RECOVERY_SIGNING_KEY"),
		SigningKeyID:          envString("FUGUE_MESH_AGENT_SIGNING_KEY_ID", envString("FUGUE_MESH_RECOVERY_SIGNING_KEY_ID", "mesh-recovery")),
		CACertFile:            strings.TrimSpace(os.Getenv("FUGUE_MESH_AGENT_CA_CERT_FILE")),
		TLSInsecureSkipVerify: envBool("FUGUE_MESH_AGENT_TLS_INSECURE_SKIP_VERIFY", false),
		StatePath:             envString("FUGUE_MESH_AGENT_STATE_PATH", "/var/lib/fugue/mesh-agent/state.json"),
		DirectoryPath:         envString("FUGUE_MESH_AGENT_DIRECTORY_PATH", "/var/lib/fugue/mesh-agent/peer-directory.json"),
		GenerationPath:        envString("FUGUE_MESH_AGENT_GENERATION_PATH", "/var/lib/fugue/mesh-agent/generation.json"),
		PollInterval:          envDuration("FUGUE_MESH_AGENT_POLL_INTERVAL", 15*time.Second),
		HTTPTimeout:           envDuration("FUGUE_MESH_AGENT_HTTP_TIMEOUT", 10*time.Second),
		RejoinEnabled:         envBool("FUGUE_MESH_AGENT_REJOIN_ENABLED", false),
		TailscaleBin:          envString("FUGUE_MESH_AGENT_TAILSCALE_BIN", "tailscale"),
		TailscaleArgs:         envList("FUGUE_MESH_AGENT_TAILSCALE_ARGS"),
		LoginServer:           strings.TrimSpace(os.Getenv("FUGUE_MESH_AGENT_LOGIN_SERVER")),
		Node: MeshNode{
			NodeID:            nodeID,
			Hostname:          envString("FUGUE_MESH_AGENT_HOSTNAME", hostname()),
			Roles:             envList("FUGUE_MESH_AGENT_ROLES"),
			Region:            strings.TrimSpace(os.Getenv("FUGUE_MESH_AGENT_REGION")),
			Country:           strings.TrimSpace(os.Getenv("FUGUE_MESH_AGENT_COUNTRY")),
			PublicIPv4:        strings.TrimSpace(os.Getenv("FUGUE_MESH_AGENT_PUBLIC_IPV4")),
			PublicIPv6:        strings.TrimSpace(os.Getenv("FUGUE_MESH_AGENT_PUBLIC_IPV6")),
			PrivateIPv4:       strings.TrimSpace(os.Getenv("FUGUE_MESH_AGENT_PRIVATE_IPV4")),
			MeshIP:            strings.TrimSpace(os.Getenv("FUGUE_MESH_AGENT_MESH_IP")),
			APIEndpoints:      envList("FUGUE_MESH_AGENT_API_ENDPOINTS"),
			RecoveryEndpoints: envList("FUGUE_MESH_AGENT_RECOVERY_ENDPOINTS"),
			EdgeEndpoints:     envList("FUGUE_MESH_AGENT_EDGE_ENDPOINTS"),
		},
	}
}

func envString(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envFirst(keys ...string) string {
	for _, key := range keys {
		value := strings.TrimSpace(os.Getenv(key))
		if value != "" {
			return value
		}
	}
	return ""
}

func envList(key string) []string {
	value := os.Getenv(key)
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	})
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		log.Printf("invalid boolean in %s=%q, using fallback %v", key, value, fallback)
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("invalid duration in %s=%q, using fallback %s", key, value, fallback)
		return fallback
	}
	return parsed
}

func hostname() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "fugue-node"
	}
	return strings.TrimSpace(host)
}
