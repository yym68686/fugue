package api

type ServerConfig struct {
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
}
