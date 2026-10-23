package platformconfig_test

import (
	"reflect"
	"testing"

	"fugue/internal/cellpublication"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func retainedDNSRequest(t *testing.T) platformconfig.CompileRequest {
	t.Helper()
	r := celldns.TransitionRequest(t)
	r.Intent.RouteAuthorityTransition.ReferenceMode = platformconfig.DNSRetainedGlobalReferenceV1
	r.Intent.CellRoutePublications = nil
	r.CellRoutePublications = nil
	return r
}

func TestDNSRetainedRoutesCompileWithoutWorkerPublications(t *testing.T) {
	r := retainedDNSRequest(t)
	before, _ := platformconfig.Digest(r.PreviousTrafficPublication)
	c := celldns.Compile(t, r)
	if !reflect.DeepEqual(c.ReleaseSet.ArtifactKinds, []string{model.PlatformArtifactKindDNSAnswerBundle}) || len(c.ReleaseSet.ConsumerTopology.EdgeNodeIDs) != 0 {
		t.Fatal("DNS-only configuration acquired Worker membership")
	}
	if _, err := cellpublication.VerifyDNSArtifact(c.DNSArtifact, celldns.Keys()); err != nil {
		t.Fatal(err)
	}
	if err := platformconfig.ValidateDNSAuthorityHandoff(r.PreviousTrafficPublication.DNS, c.DNSArtifact, "dns-a"); err != nil {
		t.Fatal("independent publication changed existing DNS behavior", err)
	}
	after, _ := platformconfig.Digest(r.PreviousTrafficPublication)
	if before != after {
		t.Fatal("compilation mutated retained route/TLS authority")
	}
	deps, err := platformconfig.DNSRouteDependencies(c.DNSArtifact)
	if err != nil || len(deps) != 2 {
		t.Fatal("missing exact original authority dependencies", deps, err)
	}
	for _, d := range deps {
		if d.Parent.ID != r.PreviousTrafficPublication.Parent.ID || !d.RequireVerified || d.FencingToken != r.PreviousTrafficPublication.Reference.FencingToken {
			t.Fatal("DNS detached from verified routing authority")
		}
	}
}

func TestDNSRetainedRoutesRejectAuthorityOrConstraintExpansion(t *testing.T) {
	for name, mutate := range map[string]func(*platformconfig.CompileRequest){
		"unknown reference version": func(r *platformconfig.CompileRequest) { r.Intent.RouteAuthorityTransition.ReferenceMode = "future" },
		"missing retained source":   func(r *platformconfig.CompileRequest) { r.PreviousTrafficPublication = nil },
		"mixed routing authorities": func(r *platformconfig.CompileRequest) {
			other := celldns.TransitionRequest(t)
			r.CellRoutePublications = other.CellRoutePublications
			r.Intent.CellRoutePublications = other.Intent.CellRoutePublications
		},
		"foreign physical endpoint": func(r *platformconfig.CompileRequest) {
			r.RuntimeSnapshot.DNSEdgeEndpoints[0].A = []string{"93.184.216.99"}
		},
		"weakened evidence expiry": func(r *platformconfig.CompileRequest) { r.Policy.DNSReadiness.FactFreshnessSeconds += 30 },
		"source tampering": func(r *platformconfig.CompileRequest) {
			r.PreviousTrafficPublication.Route.ContentHash = r.PreviousTrafficPublication.TLS.ContentHash
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := retainedDNSRequest(t)
			mutate(&r)
			if _, err := platformconfig.Compile(r); err == nil {
				t.Fatal("invalid independent DNS authority accepted")
			}
		})
	}
}
