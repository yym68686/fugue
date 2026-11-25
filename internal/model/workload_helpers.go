package model

import "strings"

// HasServiceBinding reports whether an app binds the nonempty service ID.
// Callers are responsible for checking access to the app and service.
func (app App) HasServiceBinding(serviceID string) bool {
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return false
	}
	for _, binding := range app.Bindings {
		if strings.TrimSpace(binding.ServiceID) == serviceID {
			return true
		}
	}
	return false
}

// MinObservedReplicaCount returns the smallest observed count, clamped at zero.
// An empty observation set does not prove any ready replicas.
func MinObservedReplicaCount(values ...int) int {
	if len(values) == 0 {
		return 0
	}
	minimum := values[0]
	for _, value := range values[1:] {
		minimum = min(minimum, value)
	}
	return max(0, minimum)
}
