package config

import (
	"log"
	"os"
	"strconv"
	"time"
)

type APIConfig struct {
	BindAddr          string
	StorePath         string
	DatabaseURL       string
	BootstrapAdminKey string
	AppBaseDomain     string
	APIPublicDomain   string
	RegistryPushBase  string
	ImportWorkDir     string
}

type ControllerConfig struct {
	StorePath            string
	DatabaseURL          string
	PollInterval         time.Duration
	FallbackPollInterval time.Duration
	RuntimeOfflineAfter  time.Duration
	RenderDir            string
	KubectlApply         bool
	KubectlNamespace     string
}

type AgentConfig struct {
	ServerURL        string
	NodeKey          string
	EnrollToken      string
	RuntimeKey       string
	RuntimeID        string
	RuntimeName      string
	RuntimeEndpoint  string
	WorkDir          string
	PollInterval     time.Duration
	HeartbeatEvery   time.Duration
	StateFile        string
	ApplyWithKubectl bool
}

func APIFromEnv() APIConfig {
	return APIConfig{
		BindAddr:          getenv("FUGUE_BIND_ADDR", ":8080"),
		StorePath:         getenv("FUGUE_STORE_PATH", "./data/store.json"),
		DatabaseURL:       getenv("FUGUE_DATABASE_URL", ""),
		BootstrapAdminKey: getenv("FUGUE_BOOTSTRAP_ADMIN_KEY", "fugue_bootstrap_admin_change_me"),
		AppBaseDomain:     getenv("FUGUE_APP_BASE_DOMAIN", "fugue.pro"),
		APIPublicDomain:   getenv("FUGUE_API_PUBLIC_DOMAIN", "api.fugue.pro"),
		RegistryPushBase:  getenv("FUGUE_REGISTRY_PUSH_BASE", "127.0.0.1:30500"),
		ImportWorkDir:     getenv("FUGUE_IMPORT_WORK_DIR", "./data/import"),
	}
}

func ControllerFromEnv() ControllerConfig {
	return ControllerConfig{
		StorePath:            getenv("FUGUE_STORE_PATH", "./data/store.json"),
		DatabaseURL:          getenv("FUGUE_DATABASE_URL", ""),
		PollInterval:         getenvDuration("FUGUE_CONTROLLER_POLL_INTERVAL", 5*time.Second),
		FallbackPollInterval: getenvDuration("FUGUE_CONTROLLER_FALLBACK_POLL_INTERVAL", 30*time.Second),
		RuntimeOfflineAfter:  getenvDuration("FUGUE_RUNTIME_OFFLINE_AFTER", 90*time.Second),
		RenderDir:            getenv("FUGUE_RENDER_DIR", "./data/rendered"),
		KubectlApply:         getenvBool("FUGUE_CONTROLLER_KUBECTL_APPLY", false),
		KubectlNamespace:     getenv("FUGUE_CONTROLLER_KUBECTL_NAMESPACE", "default"),
	}
}

func AgentFromEnv() AgentConfig {
	return AgentConfig{
		ServerURL:        getenv("FUGUE_AGENT_SERVER", "http://127.0.0.1:8080"),
		NodeKey:          os.Getenv("FUGUE_AGENT_NODE_KEY"),
		EnrollToken:      os.Getenv("FUGUE_AGENT_ENROLL_TOKEN"),
		RuntimeKey:       os.Getenv("FUGUE_AGENT_RUNTIME_KEY"),
		RuntimeID:        os.Getenv("FUGUE_AGENT_RUNTIME_ID"),
		RuntimeName:      getenv("FUGUE_AGENT_RUNTIME_NAME", hostnameFallback()),
		RuntimeEndpoint:  getenv("FUGUE_AGENT_RUNTIME_ENDPOINT", ""),
		WorkDir:          getenv("FUGUE_AGENT_WORK_DIR", "./data/agent"),
		PollInterval:     getenvDuration("FUGUE_AGENT_POLL_INTERVAL", 10*time.Second),
		HeartbeatEvery:   getenvDuration("FUGUE_AGENT_HEARTBEAT_EVERY", 15*time.Second),
		StateFile:        getenv("FUGUE_AGENT_STATE_FILE", "./data/agent/state.json"),
		ApplyWithKubectl: getenvBool("FUGUE_AGENT_APPLY_WITH_KUBECTL", false),
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
