// Diagnostic only. Receives previously fetched artifact representations from
// stdin; cannot contact a cluster, sign an artifact or authorize a release.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	pp "fugue/internal/platformproducer"
)

type packet struct {
	Host                             string
	Prior, Candidate, Parent, Policy model.PlatformArtifact
	ProjectionOnly                   bool
}

func run(r io.Reader, w io.Writer) error {
	bad := errors.New("incomplete or mismatched diagnostic artifact chain")
	raw, e := io.ReadAll(io.LimitReader(r, (32<<20)+1))
	if e != nil || len(raw) > 32<<20 {
		return bad
	}
	var v packet
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return fmt.Errorf("diagnostic envelope not understood: %w", err)
	}
	if dec.Decode(new(any)) != io.EOF || !v.ProjectionOnly {
		return errors.Join(bad, errors.New("not one bounded diagnostic packet"))
	}
	if v.Prior.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || v.Candidate.ArtifactKind != v.Prior.ArtifactKind || v.Parent.ArtifactKind != model.PlatformArtifactKindReleaseSet ||
		v.Parent.ScopeKey != pc.GlobalScopeKey || v.Candidate.ScopeKey != v.Parent.ScopeKey || v.Prior.ScopeKey != v.Parent.ScopeKey ||
		v.Parent.Metadata[pp.DNSPolicyIDMetadata] != v.Policy.ID || v.Parent.Metadata[pp.DNSPolicyDigestMetadata] != v.Policy.ContentHash {
		return errors.Join(bad, errors.New("kind/scope/policy mismatch"))
	}
	for _, a := range []model.PlatformArtifact{v.Prior, v.Candidate, v.Parent, v.Policy} {
		if a.ID == "" || a.ContentHash == "" || a.Status != model.PlatformArtifactStatusValidated {
			return errors.Join(bad, errors.New("artifact ID, digest or validation status missing"))
		}
	}
	// The diagnostics API masks the fence inside the JSON string. Never
	// reconstruct it or pass this projection to the serving validator.
	marker := v.Parent.Metadata[pc.HostedAddressSourceMetadata]
	digest := v.Parent.Metadata[pc.HostedAddressReservationsDigestMetadata]
	var pins struct {
		ID      string `json:"dns_artifact_id"`
		Digest  string `json:"dns_artifact_digest"`
		Release string `json:"release_id"`
	}
	if marker == "" || len(marker) > 4096 || v.Candidate.Metadata[pc.HostedAddressSourceMetadata] != marker || digest == "" ||
		v.Candidate.Metadata[pc.HostedAddressReservationsDigestMetadata] != digest || json.Unmarshal([]byte(marker), &pins) != nil ||
		pins.ID != v.Prior.ID || pins.Digest != v.Prior.ContentHash || pins.Release == "" {
		return errors.Join(bad, errors.New("visible diagnostic predecessor pins differ"))
	}
	decode := func(src any, out any) bool {
		data, e := json.Marshal(src)
		return e == nil && json.Unmarshal(data, out) == nil
	}
	var release pc.ReleaseSet
	if !decode(v.Parent.Content, &release) || len(release.ArtifactIDs) != len(release.ArtifactKinds) {
		return errors.Join(bad, errors.New("release set membership unreadable"))
	}
	n := 0
	for i, k := range release.ArtifactKinds {
		if k == v.Candidate.ArtifactKind {
			if release.ArtifactIDs[i] != v.Candidate.ID {
				return bad
			}
			n++
		}
	}
	var declared []pp.OfflineEdge
	if n != 1 || !decode(v.Policy.Content["offline_edges"], &declared) || pp.ValidateOfflineEdges(declared) != nil {
		return errors.Join(bad, errors.New("signed policy shape or DNS child membership cannot be established"))
	}
	effective, e := pp.PublicationOfflineHolds(v.Parent, declared)
	if e != nil {
		return e
	}
	var holds []pc.HostedAddressHold
	for _, h := range effective {
		holds = append(holds, pc.HostedAddressHold{EdgeID: h.EdgeID, EdgeGroupID: h.EdgeGroupID, Address: h.PublicIPv4})
	}
	var a, b pc.DNSArtifactPayload
	if !decode(v.Prior.Content, &a) || !decode(v.Candidate.Content, &b) {
		return bad
	}
	scope, e := pc.SelectHostedHoldRequirements(v.Host, a, b, holds)
	if e != nil {
		return e
	}
	before, after := 0, 0
	for _, change := range scope {
		before += len(change.Before.Targets)
		after += len(change.After.Targets)
	}
	return json.NewEncoder(w).Encode(map[string]any{"schema": "hosted-hold-scope-preview/v1", "base_release": pins.Release, "candidate_parent": v.Parent.ID, "affected_records": len(scope), "before_target_references": before, "after_target_references": after, "signature_reverified": false, "fence_verified": false, "authorizes_traffic": false})
}
func main() {
	if e := run(os.Stdin, os.Stdout); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
