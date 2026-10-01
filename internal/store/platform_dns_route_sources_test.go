package store

import (
	"errors"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/testfixture/celldns"
)

func TestDNSRouteSourcesValidateImmutablePolicyAndExactActivationLedger(t *testing.T) {
	for _, transition := range []bool{false, true} {
		for _, scenario := range []string{"valid", "superseded activation", "missing policy", "policy signature", "policy digest", "paused policy", "wrong target", "single publication", "missing activation", "wrong activation fence", "wrong activation artifact", "wrong activation scope", "rolled back activation"} {
			t.Run(scenario+"/transition="+map[bool]string{true: "true", false: "false"}[transition], func(t *testing.T) {
				req, artifacts, releases := celldns.SourceAuthorizedRequest(t, transition)
				c := celldns.Compile(t, req)
				state := &model.State{PlatformArtifacts: artifacts, PlatformArtifactReleases: releases}
				switch scenario {
				case "superseded activation":
					state.PlatformArtifactReleases[0].Status = model.PlatformArtifactReleaseStatusSuperseded
				case "missing policy":
					state.PlatformArtifacts = artifacts[1:]
				case "policy signature":
					state.PlatformArtifacts[0].Provenance.Signature = "forged"
				case "policy digest":
					state.PlatformArtifacts[0].ContentHash = "sha256:" + strings.Repeat("f", 64)
				case "paused policy", "wrong target", "single publication":
					a := &state.PlatformArtifacts[0]
					switch scenario {
					case "paused policy":
						a.Content["mode"] = "paused"
					case "wrong target":
						a.Content["target_scope"] = "authority-cell:cell-foreign"
					case "single publication":
						a.Content["serving"].(map[string]any)["single_publication"] = true
					}
					*a = celldns.Sign(t, *a, a.ID, a.GenerationSequence)
				case "missing activation":
					state.PlatformArtifactReleases = releases[1:]
				case "wrong activation fence":
					state.PlatformArtifactReleases[0].FencingToken++
				case "wrong activation artifact":
					state.PlatformArtifactReleases[0].ArtifactID = artifacts[1].ID
				case "wrong activation scope":
					state.PlatformArtifactReleases[0].ScopeKey = "platform-config-producer:cell-foreign"
				case "rolled back activation":
					state.PlatformArtifactReleases[0].Status = model.PlatformArtifactReleaseStatusRolledBack
				}
				if scenario == "paused policy" || scenario == "wrong target" || scenario == "single publication" {
					// Approve the modified immutable content explicitly so the store
					// must reject its semantics, not merely a changed digest.
					req.Intent.DNSRouteSources[0].PolicyDigest = state.PlatformArtifacts[0].ContentHash
					req.CellRoutePublications[0].ProducerPolicy.ContentHash = state.PlatformArtifacts[0].ContentHash
					c = celldns.Compile(t, req)
				}
				err := validateDNSRouteProducerBindings(state, c.DNSArtifact, celldns.Keys())
				if scenario == "valid" || scenario == "superseded activation" {
					if err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, ErrConflict) {
					t.Fatal("invalid producer binding accepted", err)
				}
			})
		}
	}
}
