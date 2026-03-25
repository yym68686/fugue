package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type APIConfig struct {
	BindAddr                    string
	StorePath                   string
	DatabaseURL                 string
	BootstrapAdminKey           string
	AppBaseDomain               string
	APIPublicDomain             string
	RegistryPushBase            string
	RegistryPullBase            string
	ClusterJoinRegistryEndpoint string
	ClusterJoinServer           string
	ClusterJoinToken            string
	ClusterJoinMeshProvider     string
	ClusterJoinMeshLoginServer  string
	ClusterJoinMeshAuthKey      string
	ImportWorkDir               string
	ShutdownDrainDelay          time.Duration
	ShutdownTimeout             time.Duration
}

type ControllerConfig struct {
	StorePath                     string
	DatabaseURL                   string
	RegistryPushBase              string
	RegistryPullBase              string
	SourceUploadBaseURL           string
	ImportWorkDir                 string
	PollInterval                  time.Duration
	FallbackPollInterval          time.Duration
	RuntimeOfflineAfter           time.Duration
	RenderDir                     string
	KubectlApply                  bool
	KubectlNamespace              string
	LeaderElectionEnabled         bool
	LeaderElectionLeaseName       string
	LeaderElectionLeaseNamespace  string
	LeaderElectionLeaseDuration   time.Duration
	LeaderElectionRenewDeadline   time.Duration
	LeaderElectionRetryPeriod     time.Duration
	LeaderElectionIdentity        string
	LegacyControllerLabelSelector string
	LegacyControllerContainerName string
	LegacyControllerCheckInterval time.Duration
}

type AgentConfig struct {
	ServerURL          string
	NodeKey            string
	EnrollToken        string
	RuntimeKey         string
	RuntimeID          string
	RuntimeName        string
	MachineName        string
	MachineFingerprint string
	RuntimeEndpoint    string
	WorkDir            string
	PollInterval       time.Duration
	HeartbeatEvery     time.Duration
	StateFile          string
	ApplyWithKubectl   bool
}

func APIFromEnv() APIConfig {
	cfg := APIConfig{
		BindAddr:                    getenv("FUGUE_BIND_ADDR", ":8080"),
		StorePath:                   getenv("FUGUE_STORE_PATH", "./data/store.json"),
		DatabaseURL:                 getenv("FUGUE_DATABASE_URL", ""),
		BootstrapAdminKey:           getenv("FUGUE_BOOTSTRAP_ADMIN_KEY", "fugue_bootstrap_admin_change_me"),
		AppBaseDomain:               getenv("FUGUE_APP_BASE_DOMAIN", "fugue.pro"),
		APIPublicDomain:             getenv("FUGUE_API_PUBLIC_DOMAIN", "api.fugue.pro"),
		RegistryPushBase:            getenv("FUGUE_REGISTRY_PUSH_BASE", "127.0.0.1:30500"),
		RegistryPullBase:            strings.TrimSpace(os.Getenv("FUGUE_REGISTRY_PULL_BASE")),
		ClusterJoinRegistryEndpoint: strings.TrimSpace(os.Getenv("FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT")),
		ClusterJoinServer:           getenv("FUGUE_CLUSTER_JOIN_SERVER", ""),
		ClusterJoinToken:            getenv("FUGUE_CLUSTER_JOIN_TOKEN", ""),
		ClusterJoinMeshProvider:     getenv("FUGUE_CLUSTER_JOIN_MESH_PROVIDER", ""),
		ClusterJoinMeshLoginServer:  getenv("FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER", ""),
		ClusterJoinMeshAuthKey:      getenv("FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY", ""),
		ImportWorkDir:               getenv("FUGUE_IMPORT_WORK_DIR", "./data/import"),
		ShutdownDrainDelay:          getenvDuration("FUGUE_API_SHUTDOWN_DRAIN_DELAY", 5*time.Second),
		ShutdownTimeout:             getenvDuration("FUGUE_API_SHUTDOWN_TIMEOUT", 25*time.Second),
	}
	if cfg.RegistryPullBase == "" {
		cfg.RegistryPullBase = cfg.RegistryPushBase
	}
	if cfg.ClusterJoinRegistryEndpoint == "" {
		cfg.ClusterJoinRegistryEndpoint = cfg.RegistryPullBase
	}
	return cfg
}

func ControllerFromEnv() ControllerConfig {
	cfg := ControllerConfig{
		StorePath:                     getenv("FUGUE_STORE_PATH", "./data/store.json"),
		DatabaseURL:                   getenv("FUGUE_DATABASE_URL", ""),
		RegistryPushBase:              getenv("FUGUE_REGISTRY_PUSH_BASE", "127.0.0.1:30500"),
		RegistryPullBase:              strings.TrimSpace(os.Getenv("FUGUE_REGISTRY_PULL_BASE")),
		SourceUploadBaseURL:           getenv("FUGUE_SOURCE_UPLOAD_BASE_URL", "http://127.0.0.1:8080"),
		ImportWorkDir:                 getenv("FUGUE_IMPORT_WORK_DIR", "./data/import"),
		PollInterval:                  getenvDuration("FUGUE_CONTROLLER_POLL_INTERVAL", 5*time.Second),
		FallbackPollInterval:          getenvDuration("FUGUE_CONTROLLER_FALLBACK_POLL_INTERVAL", 30*time.Second),
		RuntimeOfflineAfter:           getenvDuration("FUGUE_RUNTIME_OFFLINE_AFTER", 90*time.Second),
		RenderDir:                     getenv("FUGUE_RENDER_DIR", "./data/rendered"),
		KubectlApply:                  getenvBool("FUGUE_CONTROLLER_KUBECTL_APPLY", false),
		KubectlNamespace:              os.Getenv("FUGUE_CONTROLLER_KUBECTL_NAMESPACE"),
		LeaderElectionEnabled:         getenvBool("FUGUE_CONTROLLER_LEADER_ELECTION_ENABLED", false),
		LeaderElectionLeaseName:       getenv("FUGUE_CONTROLLER_LEADER_ELECTION_LEASE_NAME", "fugue-controller"),
		LeaderElectionLeaseNamespace:  os.Getenv("FUGUE_CONTROLLER_LEADER_ELECTION_LEASE_NAMESPACE"),
		LeaderElectionLeaseDuration:   getenvDuration("FUGUE_CONTROLLER_LEADER_ELECTION_LEASE_DURATION", 15*time.Second),
		LeaderElectionRenewDeadline:   getenvDuration("FUGUE_CONTROLLER_LEADER_ELECTION_RENEW_DEADLINE", 10*time.Second),
		LeaderElectionRetryPeriod:     getenvDuration("FUGUE_CONTROLLER_LEADER_ELECTION_RETRY_PERIOD", 2*time.Second),
		LeaderElectionIdentity:        getenv("FUGUE_CONTROLLER_LEADER_ELECTION_IDENTITY", hostnameFallback()),
		LegacyControllerLabelSelector: getenv("FUGUE_CONTROLLER_LEGACY_CONTROLLER_LABEL_SELECTOR", ""),
		LegacyControllerContainerName: getenv("FUGUE_CONTROLLER_LEGACY_CONTROLLER_CONTAINER_NAME", "controller"),
		LegacyControllerCheckInterval: getenvDuration("FUGUE_CONTROLLER_LEGACY_CONTROLLER_CHECK_INTERVAL", 2*time.Second),
	}
	if cfg.RegistryPullBase == "" {
		cfg.RegistryPullBase = cfg.RegistryPushBase
	}
	return cfg
}

func AgentFromEnv() AgentConfig {
	return AgentConfig{
		ServerURL:          getenv("FUGUE_AGENT_SERVER", "http://127.0.0.1:8080"),
		NodeKey:            os.Getenv("FUGUE_AGENT_NODE_KEY"),
		EnrollToken:        os.Getenv("FUGUE_AGENT_ENROLL_TOKEN"),
		RuntimeKey:         os.Getenv("FUGUE_AGENT_RUNTIME_KEY"),
		RuntimeID:          os.Getenv("FUGUE_AGENT_RUNTIME_ID"),
		RuntimeName:        getenv("FUGUE_AGENT_RUNTIME_NAME", hostnameFallback()),
		MachineName:        getenv("FUGUE_AGENT_MACHINE_NAME", hostnameFallback()),
		MachineFingerprint: getenv("FUGUE_AGENT_MACHINE_FINGERPRINT", machineFingerprintFallback()),
		RuntimeEndpoint:    getenv("FUGUE_AGENT_RUNTIME_ENDPOINT", ""),
		WorkDir:            getenv("FUGUE_AGENT_WORK_DIR", "./data/agent"),
		PollInterval:       getenvDuration("FUGUE_AGENT_POLL_INTERVAL", 10*time.Second),
		HeartbeatEvery:     getenvDuration("FUGUE_AGENT_HEARTBEAT_EVERY", 15*time.Second),
		StateFile:          getenv("FUGUE_AGENT_STATE_FILE", "./data/agent/state.json"),
		ApplyWithKubectl:   getenvBool("FUGUE_AGENT_APPLY_WITH_KUBECTL", false),
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	value := os.Getenv(key)
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

func getenvDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
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

func hostnameFallback() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "fugue-runtime"
	}
	return host
}

func machineFingerprintFallback() string {
	candidates := []string{
		"/etc/machine-id",
		"/var/lib/dbus/machine-id",
		"/sys/class/dmi/id/product_uuid",
	}
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		value := string(data)
		if trimmed := getenvTrimmed(value); trimmed != "" {
			return trimmed
		}
	}
	return hostnameFallback()
}

func getenvTrimmed(value string) string {
	return strings.TrimSpace(value)
}
