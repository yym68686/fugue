package model

type ServicePersistentStorageOverride struct {
	StorageSize string `json:"storage_size,omitempty"`
}

type OperationDiagnosis struct {
	Category        string                      `json:"category"`
	Summary         string                      `json:"summary"`
	Hint            string                      `json:"hint,omitempty"`
	AppName         string                      `json:"app_name,omitempty"`
	Service         string                      `json:"service,omitempty"`
	DependencyChain []string                    `json:"dependency_chain,omitempty"`
	BlockedBy       []OperationDiagnosisBlocker `json:"blocked_by,omitempty"`
}

type OperationDiagnosisBlocker struct {
	OperationID string `json:"operation_id"`
	AppID       string `json:"app_id,omitempty"`
	AppName     string `json:"app_name,omitempty"`
	Service     string `json:"service,omitempty"`
	Type        string `json:"type,omitempty"`
	Status      string `json:"status,omitempty"`
}
