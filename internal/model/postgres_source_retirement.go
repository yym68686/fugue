package model

type PostgresSourceRetirementPod struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
	Node string `json:"node"`
}

type PostgresSourceRetirementVolume struct {
	PVC         string `json:"pvc"`
	PVCUID      string `json:"pvc_uid"`
	PV          string `json:"pv"`
	PVUID       string `json:"pv_uid"`
	PVCSpecHash string `json:"pvc_spec_hash"`
	PVSpecHash  string `json:"pv_spec_hash"`
}

// This credential-free witness pins physical identities, not object names alone.
type PostgresSourceRetirementPlan struct {
	ServiceID        string                           `json:"service_id"`
	AppID            string                           `json:"app_id"`
	Namespace        string                           `json:"namespace"`
	SourceCluster    string                           `json:"source_cluster"`
	SourceUID        string                           `json:"source_uid"`
	SourceSpecHash   string                           `json:"source_spec_hash"`
	MigrationUID     string                           `json:"migration_uid"`
	MigrationHash    string                           `json:"migration_hash"`
	TargetCluster    string                           `json:"target_cluster"`
	TargetUID        string                           `json:"target_uid"`
	TargetPod        string                           `json:"target_pod"`
	TargetPodUID     string                           `json:"target_pod_uid"`
	TargetNode       string                           `json:"target_node"`
	TargetSpecHash   string                           `json:"target_spec_hash"`
	SystemID         string                           `json:"system_id"`
	Endpoint         string                           `json:"endpoint"`
	EndpointUID      string                           `json:"endpoint_uid"`
	EndpointSpecHash string                           `json:"endpoint_spec_hash"`
	ConfigHash       string                           `json:"config_hash"`
	PlanHash         string                           `json:"plan_hash"`
	PreserveStorage  bool                             `json:"preserve_storage"`
	Pods             []PostgresSourceRetirementPod    `json:"pods"`
	Volumes          []PostgresSourceRetirementVolume `json:"volumes"`
}
