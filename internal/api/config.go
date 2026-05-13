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
	EdgeTLSAskToken              string
	RegistryPushBase             string
	RegistryPullBase             string
	ClusterJoinRegistryEndpoint  string
	ClusterJoinServer            string
	ClusterJoinCAHash            string
	ClusterJoinBootstrapTokenTTL time.Duration
	ClusterJoinMeshProvider      string
	ClusterJoinMeshLoginServer   string
	ClusterJoinMeshAuthKey       string
	ImportWorkDir                string
}
