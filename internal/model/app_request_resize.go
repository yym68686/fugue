package model

func IsAppRequestResize(operationType string) bool {
	return operationType == OperationTypeAppCPUResize || operationType == OperationTypeAppMemoryResize
}

// PreserveVerifiedAppMemoryRequest prevents older release observations from
// reverting a verified runtime request. Normal releases consume the marker.
func PreserveVerifiedAppMemoryRequest(observed *AppSpec, stored AppSpec) {
	if stored.MemoryRequestTemplateMebibytes == nil || stored.Resources == nil {
		return
	}
	if observed.Resources == nil {
		observed.Resources = CloneResourceSpec(stored.Resources)
	} else {
		observed.Resources = CloneResourceSpec(observed.Resources)
	}
	observed.Resources.MemoryMebibytes = stored.Resources.MemoryMebibytes
	template := *stored.MemoryRequestTemplateMebibytes
	observed.MemoryRequestTemplateMebibytes = &template
}
