package api

type ServerConfig struct {
	AppBaseDomain     string
	APIPublicDomain   string
	RegistryPushBase  string
	ClusterJoinServer string
	ClusterJoinToken  string
	ImportWorkDir     string
}
