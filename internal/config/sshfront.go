package config

import (
	"os"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/sshfront"
)

type SSHFrontConfig = sshfront.Config

func SSHFrontFromEnv() SSHFrontConfig {
	return SSHFrontConfig{
		APIURL:                              getenv("FUGUE_SSH_FRONT_API_URL", getenv("FUGUE_API_URL", "")),
		EdgeToken:                           getenv("FUGUE_SSH_FRONT_EDGE_TOKEN", getenv("FUGUE_EDGE_TOKEN", "")),
		EdgeID:                              strings.TrimSpace(getenv("FUGUE_SSH_FRONT_EDGE_ID", os.Getenv("FUGUE_EDGE_ID"))),
		EdgeGroupID:                         strings.TrimSpace(getenv("FUGUE_SSH_FRONT_EDGE_GROUP_ID", os.Getenv("FUGUE_EDGE_GROUP_ID"))),
		ListenHost:                          getenv("FUGUE_SSH_FRONT_LISTEN_HOST", "0.0.0.0"),
		HealthAddr:                          getenv("FUGUE_SSH_FRONT_HEALTH_LISTEN_ADDR", ":7836"),
		CachePath:                           getenv("FUGUE_SSH_FRONT_ROUTES_CACHE_PATH", "/var/lib/fugue/edge/ssh-routes-cache.json"),
		PublicPortStart:                     getenvInt("FUGUE_SSH_PUBLIC_PORT_START", model.DefaultAppSSHPublicPortStart),
		PublicPortEnd:                       getenvInt("FUGUE_SSH_PUBLIC_PORT_END", model.DefaultAppSSHPublicPortEnd),
		SyncInterval:                        getenvDuration("FUGUE_SSH_FRONT_SYNC_INTERVAL", 15*time.Second),
		HTTPTimeout:                         getenvDuration("FUGUE_SSH_FRONT_HTTP_TIMEOUT", 10*time.Second),
		DialTimeout:                         getenvDuration("FUGUE_SSH_FRONT_DIAL_TIMEOUT", 10*time.Second),
		IdleTimeout:                         getenvDuration("FUGUE_SSH_FRONT_IDLE_TIMEOUT", 0),
		ShutdownTimeout:                     getenvDuration("FUGUE_SSH_FRONT_SHUTDOWN_TIMEOUT", 10*time.Second),
		MaxConnectionsPerIP:                 getenvInt("FUGUE_SSH_FRONT_MAX_CONNECTIONS_PER_IP", 0),
		MaxConnectionAttemptsPerIPPerMinute: getenvInt("FUGUE_SSH_FRONT_MAX_CONNECTION_ATTEMPTS_PER_IP_PER_MINUTE", 0),
		MaxConnectionsPerApp:                getenvInt("FUGUE_SSH_FRONT_MAX_CONNECTIONS_PER_APP", 0),
		MaxConnectionsPerTenant:             getenvInt("FUGUE_SSH_FRONT_MAX_CONNECTIONS_PER_TENANT", 0),
		BundleSigningKey:                    strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_KEY")),
		BundleSigningKeyID:                  getenv("FUGUE_BUNDLE_SIGNING_KEY_ID", "control-plane"),
		BundleSigningPreviousKey:            strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY")),
		BundleSigningPreviousKeyID:          strings.TrimSpace(os.Getenv("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID")),
		BundleRevokedKeyIDs:                 getenvList("FUGUE_BUNDLE_REVOKED_KEY_IDS"),
	}
}
