package model

// PreserveVerifiedAppCPURequest keeps observation-based baseline recovery from
// replacing a verified runtime target with an older immutable workload snapshot.
// Normal configuration operations fold/reset this marker separately.
func PreserveVerifiedAppCPURequest(observed *AppSpec, stored AppSpec) {
	if stored.CPURequestTemplateMilliCores == nil || stored.Resources == nil {
		return
	}
	if observed.Resources == nil {
		observed.Resources = CloneResourceSpec(stored.Resources)
	} else {
		observed.Resources = CloneResourceSpec(observed.Resources)
	}
	observed.Resources.CPUMilliCores = stored.Resources.CPUMilliCores
	template := *stored.CPURequestTemplateMilliCores
	observed.CPURequestTemplateMilliCores = &template
}
