package celldns

import "fugue/internal/platformconfig"

// RetainGlobalOnly removes replacement routing authorities from a synthetic
// transition. Its original signed global inputs and DNS members remain intact.
func RetainGlobalOnly(req *platformconfig.CompileRequest) {
	req.Intent.RouteAuthorityTransition.ReferenceMode = platformconfig.DNSRetainedGlobalReferenceV1
	req.Intent.CellRoutePublications = nil
	req.CellRoutePublications = nil
	approved := req.Intent.DNSRouteSources[:0]
	for _, source := range req.Intent.DNSRouteSources {
		if source.ScopeKey == platformconfig.GlobalScopeKey {
			approved = append(approved, source)
		}
	}
	req.Intent.DNSRouteSources = approved
}
