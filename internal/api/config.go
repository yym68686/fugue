package api

import (
	"os"
	"strconv"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/observability"
)

func imageStoreMinimumReplicasFromEnv() int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv("FUGUE_IMAGE_STORE_MIN_REPLICAS")))
	if err != nil || value <= 0 {
		return 1
	}
	return value
}

type ServerConfig struct {
	DatabaseURL                      string
	ControlPlaneNamespace            string
	ControlPlaneReleaseInstance      string
	BackupCoordination               BackupCoordinationConfig
	ControlPlaneCNPGBackupEnabled    bool
	ControlPlaneCNPGBackupName       string
	RegistryGCLeaseName              string
	ControlPlaneGitHubRepository     string
	ControlPlaneGitHubWorkflow       string
	ControlPlaneGitHubAPIURL         string
	ControlPlaneGitHubToken          string
	AppBaseDomain                    string
	APIPublicDomain                  string
	SSHPublicHost                    string
	SSHPublicPortStart               int
	SSHPublicPortEnd                 int
	DNSStaticRecordsJSON             string
	DNSNameservers                   []string
	DNSRouteAAnswerIPs               []string
	DNSBundleTTL                     int
	PlatformRoutesJSON               string
	EdgeQualityRankingMode           string
	AppSafeZeroDowntimePublicEnabled bool
	EdgeTLSAskToken                  string
	AllowLegacyEdgeToken             bool
	ImageStoreMode                   string
	ImageStoreMinReplicas            int
	RegistryPushBase                 string
	RegistryPullBase                 string
	ClusterJoinRegistryEndpoint      string
	MovableRWOStorageClass           string
	ManagedPostgresStorageClass      string
	ClusterJoinServer                string
	ClusterJoinServerFallbacks       string
	ClusterJoinCAHash                string
	ClusterJoinBootstrapTokenTTL     time.Duration
	ClusterJoinK3SVersion            string
	ClusterJoinMeshProvider          string
	ClusterJoinMeshLoginServer       string
	ClusterJoinMeshAuthKey           string
	BundleSigningKey                 string
	BundleSigningKeyID               string
	BundleSigningPreviousKey         string
	BundleSigningPreviousKeyID       string
	BundleRevokedKeyIDs              []string
	HeartbeatAuditKeyring            bundleauth.Keyring
	BundleValidFor                   time.Duration
	ImportWorkDir                    string
	Observability                    observability.Config
}

type BackupCoordinationConfig struct {
	LeaseName      string
	LeaseNamespace string
	LeaseDuration  time.Duration
	RenewPeriod    time.Duration
}
