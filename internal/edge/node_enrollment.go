package edge

import (
	"time"

	"fugue/internal/model"
)

func (s *Service) currentEdgeEnrollment() *model.EdgeNodeEnrollment {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.edgeEnrollment == nil {
		return nil
	}
	copy := *s.edgeEnrollment
	return &copy
}

func (s *Service) routeReaderToken(path string) (string, error) {
	// A managed new node has a projected group signing identity. Derive a
	// short-lived node-bound read credential; never reuse another node's token.
	if s.Config.WorkloadMode == model.EdgeWorkloadModeDynamic && s.InventoryProducer.IdentityKeyringFile != "" {
		return issueBoundEdgeComponentIdentity(s.InventoryProducer.IdentityKeyringFile, s.Config, time.Now().UTC(), "edge-route-reader")
	}
	return loadEdgeRouteReaderToken(path)
}
