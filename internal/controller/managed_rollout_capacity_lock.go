package controller

import (
	"sort"
	"strings"
	"sync"

	"fugue/internal/runtime"
)

func (s *Service) lockManagedRolloutCapacityDomain(scheduling runtime.SchedulingConstraints) func() {
	key := managedRolloutCapacityDomainKey(scheduling)
	value, _ := s.managedRolloutCapacityLocks.LoadOrStore(key, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

func managedRolloutCapacityDomainKey(scheduling runtime.SchedulingConstraints) string {
	parts := make([]string, 0, len(scheduling.NodeSelector)+len(scheduling.Tolerations))
	for key, value := range scheduling.NodeSelector {
		parts = append(parts, "selector:"+strings.TrimSpace(key)+"="+strings.TrimSpace(value))
	}
	for _, toleration := range scheduling.Tolerations {
		parts = append(parts, "toleration:"+strings.Join([]string{
			strings.TrimSpace(toleration.Key),
			strings.TrimSpace(toleration.Operator),
			strings.TrimSpace(toleration.Value),
			strings.TrimSpace(toleration.Effect),
		}, "="))
	}
	if len(parts) == 0 {
		return "cluster-default"
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x00")
}
