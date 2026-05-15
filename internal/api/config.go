package api

import "time"

type ServerConfig struct {
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
}
