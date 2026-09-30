package api

import "fugue/internal/platformconfig"

// Replaying DNS compilation verifies every embedded signature and exact
// selected publication in a coherent durable snapshot. Publication repeats the
// same reference and fence checks inside its authority transaction.
func (s *Server) validateCellDNSCompilation(compiled platformconfig.CompileResult) error {
	if err := s.store.ValidateDNSPublicationReferences(compiled.DNSArtifact); err != nil {
		return &platformConfigReferenceError{err.Error()}
	}
	return nil
}
