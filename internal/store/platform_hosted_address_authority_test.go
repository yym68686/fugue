package store

import (
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/testfixture/celldns"
)

func TestHostedAddressServingUsesLockedSignedBaseline(t *testing.T) {
	req, _ := celldns.TransitionRequestWithSource(t)
	p := req.PreviousTrafficPublication
	keys := []string{platformproducer.PolicyReleaseMetadata, platformproducer.DNSPolicyIDMetadata, platformproducer.DNSPolicyDigestMetadata, platformproducer.StaticIntentIDMetadata, platformproducer.StaticIntentDigestMetadata}
	for _, k := range keys {
		p.Parent.Metadata[k] = "bound-" + k
	}
	p.Parent = celldns.Sign(t, p.Parent, p.Parent.ID, 1)
	p.Reference.ReleaseSetDigest = p.Parent.ContentHash
	fixref := func(ref pc.PreviousTrafficPublicationReference, a *model.PlatformArtifact) {
		raw, err := json.Marshal(ref)
		if err != nil {
			t.Fatal(err)
		}
		a.Metadata[pc.HostedAddressSourceMetadata] = string(raw)
		a.Metadata[pc.HostedAddressReservationsDigestMetadata] = "sha256:" + strings.Repeat("b", 64)
	}
	now := time.Now().UTC()
	for _, scenario := range []string{"valid", "move full", "move LKG", "omit TLS", "expired", "revoked DNS", "unsigned DNS", "cross policy", "missing parent marker", "noncanonical source", "different source member", "foreign parent", "different digest"} {
		t.Run(scenario, func(t *testing.T) {
			ref := p.Reference
			previous := p.Parent
			dns := p.DNS
			parent := model.PlatformArtifact{ID: "successor", ScopeKey: "global", ArtifactKind: model.PlatformArtifactKindReleaseSet, Metadata: maps.Clone(p.Parent.Metadata)}
			child := model.PlatformArtifact{ID: "dns-successor", ScopeKey: "global", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, Metadata: map[string]string{}}
			r := model.PlatformArtifactRelease{ID: ref.ReleaseID, ArtifactID: ref.ReleaseSetID, FencingToken: ref.FencingToken}
			lkg := model.PlatformLKGSnapshot{ArtifactID: ref.ReleaseSetID, ContentHash: ref.ReleaseSetDigest, VerifiedByReleaseID: r.ID, ExpiresAt: now.Add(time.Hour)}
			fixref(ref, &parent)
			fixref(ref, &child)
			state := model.State{PlatformArtifacts: []model.PlatformArtifact{p.Route, p.TLS}}
			switch scenario {
			case "move full":
				r.FencingToken++
			case "move LKG":
				lkg.VerifiedByReleaseID = "other"
			case "omit TLS":
				state.PlatformArtifacts = state.PlatformArtifacts[:1]
			case "expired":
				lkg.ExpiresAt = now
			case "revoked DNS":
				dns.Status = model.PlatformArtifactStatusRejected
			case "unsigned DNS":
				dns.Metadata = maps.Clone(dns.Metadata)
				dns.Metadata["unexpected"] = "changed"
			case "cross policy":
				parent.Metadata[keys[0]] = "other-policy"
			case "missing parent marker":
				delete(parent.Metadata, pc.HostedAddressSourceMetadata)
			case "noncanonical source":
				child.Metadata[pc.HostedAddressSourceMetadata] += " "
			case "different source member":
				ref.DNSArtifactID = "foreign-dns"
				fixref(ref, &child)
				fixref(ref, &parent)
			case "foreign parent":
				previous.ScopeKey = "different"
			case "different digest":
				parent.Metadata[pc.HostedAddressReservationsDigestMetadata] = "sha256:" + strings.Repeat("c", 64)
			}
			state.PlatformArtifacts = append(state.PlatformArtifacts, dns, previous)
			e := validateHostedAddressBaseline(&state, parent, child, r, lkg, now, celldns.Keys())
			if scenario == "valid" {
				if e != nil {
					t.Fatal(e)
				}
				ids, e := retainedTrafficSourceRouteIDs(child)
				if e != nil || len(ids) != 4 || !slices.Contains(ids, ref.TLSArtifactID) || !slices.Contains(ids, ref.ReleaseSetID) || !slices.Contains(ids, ref.DNSArtifactID) || !slices.Contains(ids, ref.RouteArtifactID) {
					t.Fatal("incomplete postgres transaction inputs", ids, e)
				}
				if rejectUnboundHostedPublication(parent, "gray") == nil || rejectUnboundHostedPublication(parent, "full") == nil {
					t.Fatal("unfenced serving publication admitted")
				}
			} else if !errors.Is(e, ErrConflict) {
				t.Fatal("source substitution accepted", e)
			}
		})
	}
}
