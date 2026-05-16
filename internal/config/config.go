package config

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type APIConfig struct {
	BindAddr                     string
	StorePath                    string
	DatabaseURL                  string
	BootstrapAdminKey            string
	WorkloadIdentitySigningKey   string
	ControlPlaneNamespace        string
	ControlPlaneReleaseInstance  string
	ControlPlaneGitHubRepository string
	ControlPlaneGitHubWorkflow   string
	ControlPlaneGitHubAPIURL     string
	ControlPlaneGitHubToken      string
	AppBaseDomain                string
	APIPublicDomain              string
	DNSStaticRecordsJSON         string
	PlatformRoutesJSON           string
	EdgeTLSAskToken              string
	AllowLegacyEdgeToken         bool
	RegistryPushBase             string
	RegistryPullBase             string
	ClusterJoinRegistryEndpoint  string
	ClusterJoinServer            string
	ClusterJoinServerFallbacks   string
	ClusterJoinCAHash            string
	ClusterJoinBootstrapTokenTTL time.Duration
	ClusterJoinMeshProvider      string
	ClusterJoinMeshLoginServer   string
	ClusterJoinMeshAuthKey       string
	BundleSigningKey             string
	BundleSigningKeyID           string
	BundleSigningPreviousKey     string
	BundleSigningPreviousKeyID   string
	BundleRevokedKeyIDs          []string
	BundleValidFor               time.Duration
	ImportWorkDir                string
	ShutdownDrainDelay           time.Duration
	ShutdownTimeout              time.Duration
}

type ControllerConfig struct {
	StorePath                     string
	DatabaseURL                   string
	APIPublicDomain               string
	WorkloadIdentitySigningKey    string
	RegistryPushBase              string
	RegistryPullBase              string
	SourceUploadBaseURL           string
	ImportWorkDir                 string
	ForegroundImportWorkers       int
	ForegroundActivateWorkers     int
	GitHubSyncImportWorkers       int
	GitHubSyncActivateWorkers     int
	GitHubSyncInterval            time.Duration
	GitHubSyncTimeout             time.Duration
	GitHubSyncRetryBaseDelay      time.Duration
	GitHubSyncRetryMaxDelay       time.Duration
	ManagedAppRolloutTimeout      time.Duration
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
	CellStorePath      string
	CellListenAddr     string
	CellPeerProbe      bool
	CellPeerProbePort  int
	PollInterval       time.Duration
	HeartbeatEvery     time.Duration
	StateFile          string
	ApplyWithKubectl   bool
}

type EdgeConfig struct {
	APIURL                     string
	EdgeToken                  string
	EdgeID                     string
	EdgeGroupID                string
	Region                     string
	Country                    string
	PublicHostname             string
	PublicIPv4                 string
	PublicIPv6                 string
	MeshIP                     string
	Draining                   bool
	CachePath                  string
	CacheArchiveLimit          int
	MaxStale                   time.Duration
	PeerFallbackEnabled        bool
	ListenAddr                 string
	SyncInterval               time.Duration
	HeartbeatInterval          time.Duration
	HTTPTimeout                time.Duration
	CaddyEnabled               bool
	CaddyAdminURL              string
	CaddyListenAddr            string
	CaddyTLSMode               string
	CaddyTLSAskURL             string
	CaddyProxyListenAddr       string
	CaddyStaticTLSCertFile     string
	CaddyStaticTLSKeyFile      string
	BundleSigningKey           string
	BundleSigningKeyID         string
	BundleSigningPreviousKey   string
	BundleSigningPreviousKeyID string
	BundleRevokedKeyIDs        []string
}

type DNSConfig struct {
	APIURL                     string
	EdgeToken                  string
	DNSNodeID                  string
	EdgeGroupID                string
	PublicHostname             string
	PublicIPv4                 string
	PublicIPv6                 string
	MeshIP                     string
	Zone                       string
	AnswerIPs                  []string
	RouteAAnswerIPs            []string
	CachePath                  string
	CacheArchiveLimit          int
	MaxStale                   time.Duration
	EdgeHealthProbeEnabled     bool
	EdgeHealthProbePort        int
	EdgeHealthProbeTimeout     time.Duration
	ListenAddr                 string
	UDPAddr                    string
	TCPAddr                    string
	SyncInterval               time.Duration
	HeartbeatInterval          time.Duration
	HTTPTimeout                time.Duration
	TTL                        int
	Nameservers                []string
	BundleSigningKey           string
	BundleSigningKeyID         string
	BundleSigningPreviousKey   string
	BundleSigningPreviousKeyID string
	BundleRevokedKeyIDs        []string
}

func APIFromEnv() APIConfig {
	cfg := APIConfig{
		BindAddr:                     getenv("FUGUE_BIND_ADDR", ":8080"),
		StorePath:                    getenv("FUGUE_STORE_PATH", "./data/store.json"),
		DatabaseURL:                  getenv("FUGUE_DATABASE_URL", ""),
		BootstrapAdminKey:            getenv("FUGUE_BOOTSTRAP_ADMIN_KEY", "fugue_bootstrap_admin_change_me"),
		WorkloadIdentitySigningKey:   strings.TrimSpace(os.Getenv("FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY")),
		ControlPlaneNamespace:        getenv("FUGUE_CONTROL_PLANE_NAMESPACE", ""),
		ControlPlaneReleaseInstance:  getenv("FUGUE_CONTROL_PLANE_RELEASE_INSTANCE", ""),
		ControlPlaneGitHubRepository: strings.TrimSpace(os.Getenv("FUGUE_CONTROL_PLANE_GITHUB_REPOSITORY")),
		ControlPlaneGitHubWorkflow:   getenv("FUGUE_CONTROL_PLANE_GITHUB_WORKFLOW", "deploy-control-plane.yml"),
		ControlPlaneGitHubAPIURL:     getenv("FUGUE_CONTROL_PLANE_GITHUB_API_URL", "https://api.github.com"),
		ControlPlaneGitHubToken:      strings.TrimSpace(os.Getenv("FUGUE_CONTROL_PLANE_GITHUB_TOKEN")),
		AppBaseDomain:                getenv("FUGUE_APP_BASE_DOMAIN", ""),
		APIPublicDomain:              getenv("FUGUE_API_PUBLIC_DOMAIN", ""),
		DNSStaticRecordsJSON:         strings.TrimSpace(os.Getenv("FUGUE_DNS_STATIC_RECORDS_JSON")),
		PlatformRoutesJSON:           strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_ROUTES_JSON")),
		EdgeTLSAskToken:              strings.TrimSpace(os.Getenv("FUGUE_EDGE_TLS_ASK_TOKEN")),
		AllowLegacyEdgeToken:         getenvBool("FUGUE_ALLOW_LEGACY_EDGE_TOKEN", false),
		RegistryPushBase:             getenv("FUGUE_REGISTRY_PUSH_BASE", ""),
		RegistryPullBase:             strings.TrimSpace(os.Getenv("FUGUE_REGISTRY_PULL_BASE")),
		ClusterJoinRegistryEndpoint:  strings.TrimSpace(os.Getenv("FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT")),
		ClusterJoinServer:            getenv("FUGUE_CLUSTER_JOIN_SERVER", ""),
		ClusterJoinServerFallbacks:   strings.TrimSpace(os.Getenv("FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS")),
		ClusterJoinCAHash:            strings.TrimSpace(os.Getenv("FUGUE_CLUSTER_JOIN_CA_HASH")),
		ClusterJoinBootstrapTokenTTL: getenvDuration("FUGUE_CLUSTER_JOIN_BOOTSTRAP_TOKEN_TTL", 15*time.Minute),
		ClusterJoinMeshProvider:      getenv("FUGUE_CLUSTER_JOIN_MESH_PROVIDER", ""),
		ClusterJoinMeshLoginServer:   getenv("FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER", ""),
		ClusterJoinMeshAuthKey:       getenv("FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY", ""),
		BundleSigningKey:             strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_KEY")),
		BundleSigningKeyID:           getenv("FUGUE_BUNDLE_SIGNING_KEY_ID", "control-plane"),
		BundleSigningPreviousKey:     strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY")),
		BundleSigningPreviousKeyID:   strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID")),
		BundleRevokedKeyIDs:          getenvList("FUGUE_BUNDLE_REVOKED_KEY_IDS"),
		BundleValidFor:               getenvDuration("FUGUE_BUNDLE_VALID_FOR", 15*time.Minute),
		ImportWorkDir:                getenv("FUGUE_IMPORT_WORK_DIR", "./data/import"),
		ShutdownDrainDelay:           getenvDuration("FUGUE_API_SHUTDOWN_DRAIN_DELAY", 5*time.Second),
		ShutdownTimeout:              getenvDuration("FUGUE_API_SHUTDOWN_TIMEOUT", 25*time.Second),
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
		APIPublicDomain:               getenv("FUGUE_API_PUBLIC_DOMAIN", ""),
		WorkloadIdentitySigningKey:    strings.TrimSpace(os.Getenv("FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY")),
		RegistryPushBase:              getenv("FUGUE_REGISTRY_PUSH_BASE", "127.0.0.1:30500"),
		RegistryPullBase:              strings.TrimSpace(os.Getenv("FUGUE_REGISTRY_PULL_BASE")),
		SourceUploadBaseURL:           getenv("FUGUE_SOURCE_UPLOAD_BASE_URL", "http://127.0.0.1:8080"),
		ImportWorkDir:                 getenv("FUGUE_IMPORT_WORK_DIR", "./data/import"),
		ForegroundImportWorkers:       getenvInt("FUGUE_CONTROLLER_FOREGROUND_IMPORT_WORKERS", 0),
		ForegroundActivateWorkers:     getenvInt("FUGUE_CONTROLLER_FOREGROUND_ACTIVATE_WORKERS", 4),
		GitHubSyncImportWorkers:       getenvInt("FUGUE_CONTROLLER_GITHUB_SYNC_IMPORT_WORKERS", 0),
		GitHubSyncActivateWorkers:     getenvInt("FUGUE_CONTROLLER_GITHUB_SYNC_ACTIVATE_WORKERS", 4),
		GitHubSyncInterval:            getenvDuration("FUGUE_CONTROLLER_GITHUB_SYNC_INTERVAL", time.Minute),
		GitHubSyncTimeout:             getenvDuration("FUGUE_CONTROLLER_GITHUB_SYNC_TIMEOUT", 20*time.Second),
		GitHubSyncRetryBaseDelay:      getenvDuration("FUGUE_CONTROLLER_GITHUB_SYNC_RETRY_BASE_DELAY", 5*time.Minute),
		GitHubSyncRetryMaxDelay:       getenvDuration("FUGUE_CONTROLLER_GITHUB_SYNC_RETRY_MAX_DELAY", time.Hour),
		ManagedAppRolloutTimeout:      getenvDuration("FUGUE_CONTROLLER_MANAGED_APP_ROLLOUT_TIMEOUT", 10*time.Minute),
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
	workDir := getenv("FUGUE_AGENT_WORK_DIR", "./data/agent")
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
		WorkDir:            workDir,
		CellStorePath:      getenv("FUGUE_AGENT_CELL_STORE_PATH", filepath.Join(workDir, "cell-store.json")),
		CellListenAddr:     getenv("FUGUE_AGENT_CELL_LISTEN_ADDR", ":7831"),
		CellPeerProbe:      getenvBool("FUGUE_AGENT_CELL_PEER_PROBE", true),
		CellPeerProbePort:  getenvInt("FUGUE_AGENT_CELL_PEER_PROBE_PORT", 7831),
		PollInterval:       getenvDuration("FUGUE_AGENT_POLL_INTERVAL", 10*time.Second),
		HeartbeatEvery:     getenvDuration("FUGUE_AGENT_HEARTBEAT_EVERY", 15*time.Second),
		StateFile:          getenv("FUGUE_AGENT_STATE_FILE", "./data/agent/state.json"),
		ApplyWithKubectl:   getenvBool("FUGUE_AGENT_APPLY_WITH_KUBECTL", false),
	}
}

func EdgeFromEnv() EdgeConfig {
	return EdgeConfig{
		APIURL:                     getenv("FUGUE_API_URL", ""),
		EdgeToken:                  strings.TrimSpace(os.Getenv("FUGUE_EDGE_TOKEN")),
		EdgeID:                     strings.TrimSpace(os.Getenv("FUGUE_EDGE_ID")),
		EdgeGroupID:                strings.TrimSpace(os.Getenv("FUGUE_EDGE_GROUP_ID")),
		Region:                     strings.TrimSpace(os.Getenv("FUGUE_EDGE_REGION")),
		Country:                    strings.TrimSpace(os.Getenv("FUGUE_EDGE_COUNTRY")),
		PublicHostname:             strings.TrimSpace(os.Getenv("FUGUE_EDGE_PUBLIC_HOSTNAME")),
		PublicIPv4:                 strings.TrimSpace(os.Getenv("FUGUE_EDGE_PUBLIC_IPV4")),
		PublicIPv6:                 strings.TrimSpace(os.Getenv("FUGUE_EDGE_PUBLIC_IPV6")),
		MeshIP:                     strings.TrimSpace(os.Getenv("FUGUE_EDGE_MESH_IP")),
		Draining:                   getenvBool("FUGUE_EDGE_DRAINING", false),
		CachePath:                  getenv("FUGUE_EDGE_ROUTES_CACHE_PATH", "/var/lib/fugue/edge/routes-cache.json"),
		CacheArchiveLimit:          getenvInt("FUGUE_EDGE_CACHE_ARCHIVE_LIMIT", 5),
		MaxStale:                   getenvDuration("FUGUE_EDGE_MAX_STALE", 24*time.Hour),
		PeerFallbackEnabled:        getenvBool("FUGUE_EDGE_PEER_FALLBACK_ENABLED", true),
		ListenAddr:                 getenv("FUGUE_EDGE_LISTEN_ADDR", "127.0.0.1:7832"),
		SyncInterval:               getenvDuration("FUGUE_EDGE_SYNC_INTERVAL", 15*time.Second),
		HeartbeatInterval:          getenvDuration("FUGUE_EDGE_HEARTBEAT_INTERVAL", 30*time.Second),
		HTTPTimeout:                getenvDuration("FUGUE_EDGE_HTTP_TIMEOUT", 10*time.Second),
		CaddyEnabled:               getenvBool("FUGUE_EDGE_CADDY_ENABLED", false),
		CaddyAdminURL:              getenv("FUGUE_EDGE_CADDY_ADMIN_URL", "http://127.0.0.1:2019"),
		CaddyListenAddr:            getenv("FUGUE_EDGE_CADDY_LISTEN_ADDR", "127.0.0.1:18080"),
		CaddyTLSMode:               getenv("FUGUE_EDGE_CADDY_TLS_MODE", "off"),
		CaddyTLSAskURL:             strings.TrimSpace(os.Getenv("FUGUE_EDGE_CADDY_TLS_ASK_URL")),
		CaddyProxyListenAddr:       getenv("FUGUE_EDGE_PROXY_LISTEN_ADDR", "127.0.0.1:7833"),
		CaddyStaticTLSCertFile:     strings.TrimSpace(os.Getenv("FUGUE_EDGE_CADDY_STATIC_TLS_CERT_FILE")),
		CaddyStaticTLSKeyFile:      strings.TrimSpace(os.Getenv("FUGUE_EDGE_CADDY_STATIC_TLS_KEY_FILE")),
		BundleSigningKey:           strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_KEY")),
		BundleSigningKeyID:         getenv("FUGUE_BUNDLE_SIGNING_KEY_ID", "control-plane"),
		BundleSigningPreviousKey:   strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY")),
		BundleSigningPreviousKeyID: strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID")),
		BundleRevokedKeyIDs:        getenvList("FUGUE_BUNDLE_REVOKED_KEY_IDS"),
	}
}

func DNSFromEnv() DNSConfig {
	zone := getenv("FUGUE_DNS_ZONE", "")
	return DNSConfig{
		APIURL:                     getenv("FUGUE_API_URL", ""),
		EdgeToken:                  strings.TrimSpace(os.Getenv("FUGUE_DNS_TOKEN")),
		DNSNodeID:                  strings.TrimSpace(os.Getenv("FUGUE_DNS_NODE_ID")),
		EdgeGroupID:                strings.TrimSpace(os.Getenv("FUGUE_EDGE_GROUP_ID")),
		PublicHostname:             strings.TrimSpace(os.Getenv("FUGUE_DNS_PUBLIC_HOSTNAME")),
		PublicIPv4:                 strings.TrimSpace(os.Getenv("FUGUE_DNS_PUBLIC_IPV4")),
		PublicIPv6:                 strings.TrimSpace(os.Getenv("FUGUE_DNS_PUBLIC_IPV6")),
		MeshIP:                     strings.TrimSpace(os.Getenv("FUGUE_DNS_MESH_IP")),
		Zone:                       zone,
		AnswerIPs:                  getenvList("FUGUE_DNS_ANSWER_IPS"),
		RouteAAnswerIPs:            getenvList("FUGUE_DNS_ROUTE_A_ANSWER_IPS"),
		CachePath:                  getenv("FUGUE_DNS_CACHE_PATH", "/var/lib/fugue/dns/dns-cache.json"),
		CacheArchiveLimit:          getenvInt("FUGUE_DNS_CACHE_ARCHIVE_LIMIT", 5),
		MaxStale:                   getenvDuration("FUGUE_DNS_MAX_STALE", 24*time.Hour),
		EdgeHealthProbeEnabled:     getenvBool("FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED", false),
		EdgeHealthProbePort:        getenvInt("FUGUE_DNS_EDGE_HEALTH_PROBE_PORT", 443),
		EdgeHealthProbeTimeout:     getenvDuration("FUGUE_DNS_EDGE_HEALTH_PROBE_TIMEOUT", 250*time.Millisecond),
		ListenAddr:                 getenv("FUGUE_DNS_LISTEN_ADDR", "127.0.0.1:7834"),
		UDPAddr:                    getenv("FUGUE_DNS_UDP_ADDR", "127.0.0.1:5353"),
		TCPAddr:                    getenv("FUGUE_DNS_TCP_ADDR", "127.0.0.1:5353"),
		SyncInterval:               getenvDuration("FUGUE_DNS_SYNC_INTERVAL", 15*time.Second),
		HeartbeatInterval:          getenvDuration("FUGUE_DNS_HEARTBEAT_INTERVAL", 30*time.Second),
		HTTPTimeout:                getenvDuration("FUGUE_DNS_HTTP_TIMEOUT", 10*time.Second),
		TTL:                        getenvInt("FUGUE_DNS_TTL", 60),
		Nameservers:                getenvList("FUGUE_DNS_NAMESERVERS"),
		BundleSigningKey:           strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_KEY")),
		BundleSigningKeyID:         getenv("FUGUE_BUNDLE_SIGNING_KEY_ID", "control-plane"),
		BundleSigningPreviousKey:   strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY")),
		BundleSigningPreviousKeyID: strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID")),
		BundleRevokedKeyIDs:        getenvList("FUGUE_BUNDLE_REVOKED_KEY_IDS"),
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvList(key string) []string {
	value := os.Getenv(key)
	if value == "" {
		return nil
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\n' || r == '\t'
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

func getenvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("invalid integer in %s=%q, using fallback %d", key, value, fallback)
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
