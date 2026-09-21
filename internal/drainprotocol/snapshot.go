package drainprotocol

const Version = "fugue.drain-observation/v1"

// Snapshot is a fresh socket observation, never a prestop or cached receipt.
// Kubernetes authenticates transport and supplies the Pod UID binding.
type Snapshot struct {
	APIVersion        string `json:"api_version"`
	Nonce             string `json:"nonce"`
	Pod               string `json:"pod"`
	Namespace         string `json:"namespace"`
	AppPorts          []int  `json:"app_ports"`
	ActiveConnections *int   `json:"active_connections"`
}
