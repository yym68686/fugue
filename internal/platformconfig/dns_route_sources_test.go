package platformconfig_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"fugue/internal/cellpublication"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestDNSRouteSourcesPreserveExactBaselineAndCanonicalApproval(t *testing.T) {
	for _, transition := range []bool{false, true} {
		r, _, _ := celldns.SourceAuthorizedRequest(t, transition)
		c := celldns.Compile(t, r)
		if _, err := cellpublication.VerifyDNSArtifact(c.DNSArtifact, celldns.Keys()); err != nil {
			t.Fatal(err)
		}
		want := platformconfig.NormalizePlatformIntent(r.Intent).DNSRouteSources
		for _, a := range []model.PlatformArtifact{c.DNSArtifact, cloneDNSArtifact(t, c.DNSArtifact)} {
			got, err := platformconfig.DNSRouteSourceAuthorizations(a)
			if err != nil || !reflect.DeepEqual(want, got) {
				t.Fatal("source approval lost after artifact persistence", got, err)
			}
		}
		slices.Reverse(r.Intent.DNSRouteSources)
		next := celldns.Compile(t, r)
		if next.DNSArtifact.ContentHash != c.DNSArtifact.ContentHash {
			t.Fatal("source approval enumeration changed signed content")
		}
		// A future immutable policy may be approved before its activation is
		// observed. It does not alter baseline route proofs or their release pins.
		future := r.Intent.DNSRouteSources[0]
		future.PolicyArtifactID, future.PolicyDigest = "future-policy", "sha256:"+strings.Repeat("c", 64)
		r.Intent.DNSRouteSources = append(r.Intent.DNSRouteSources, future)
		futureCompiled := celldns.Compile(t, r)
		before, _ := platformconfig.Digest(c.DNSArtifact.Content["readiness_plan"])
		after, _ := platformconfig.Digest(futureCompiled.DNSArtifact.Content["readiness_plan"])
		if before != after {
			t.Fatal("configuration approval granted a new observed route proof")
		}
	}
}

func cloneDNSArtifact(t *testing.T, a model.PlatformArtifact) model.PlatformArtifact {
	t.Helper()
	raw, _ := json.Marshal(a)
	var out model.PlatformArtifact
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDNSRouteSourcesRejectCrossAuthorityAndUnboundInputs(t *testing.T) {
	for name, mutate := range map[string]func(*platformconfig.CompileRequest){
		"foreign scope": func(r *platformconfig.CompileRequest) {
			r.Intent.DNSRouteSources[0].ScopeKey = "authority-cell:cell-foreign"
		},
		"self scope":   func(r *platformconfig.CompileRequest) { r.Intent.DNSRouteSources[0].ScopeKey = r.Intent.Scope },
		"missing cell": func(r *platformconfig.CompileRequest) { r.Intent.DNSRouteSources = r.Intent.DNSRouteSources[1:] },
		"duplicate": func(r *platformconfig.CompileRequest) {
			r.Intent.DNSRouteSources = append(r.Intent.DNSRouteSources, r.Intent.DNSRouteSources[0])
		},
		"policy digest": func(r *platformconfig.CompileRequest) {
			r.CellRoutePublications[0].ProducerPolicy.ContentHash = "sha256:" + strings.Repeat("f", 64)
		},
		"missing binding": func(r *platformconfig.CompileRequest) { r.CellRoutePublications[0].ProducerPolicy = nil },
		"activation": func(r *platformconfig.CompileRequest) {
			r.CellRoutePublications[0].ProducerPolicy.ReleaseID = "unbound-policy-release"
		},
		"fence":                    func(r *platformconfig.CompileRequest) { r.CellRoutePublications[0].ProducerPolicy.FencingToken = 0 },
		"undeclared producer":      func(r *platformconfig.CompileRequest) { r.Intent.DNSRouteSources = nil },
		"missing previous binding": func(r *platformconfig.CompileRequest) { r.PreviousTrafficPublication.ProducerPolicy = nil },
		"previous policy substitution": func(r *platformconfig.CompileRequest) {
			r.PreviousTrafficPublication.ProducerPolicy.ArtifactID = "foreign-policy"
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, _, _ := celldns.SourceAuthorizedRequest(t, true)
			mutate(&r)
			if _, err := platformconfig.Compile(r); err == nil {
				t.Fatal("invalid source approval compiled")
			}
		})
	}
}
