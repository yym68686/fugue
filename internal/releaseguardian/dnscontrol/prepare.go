package dnscontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	kubejson "sigs.k8s.io/json"
)

// HoldChange describes only the exact one-entry delta. A nil before or
// after represents absence, not permission to infer a missing predecessor.
type HoldChange struct {
	EdgeID string                        `json:"edgeId"`
	Before *platformproducer.OfflineEdge `json:"before"`
	After  *platformproducer.OfflineEdge `json:"after"`
}

func DecodeHoldChange(raw []byte) (HoldChange, error) {
	var c HoldChange
	strict, err := kubejson.UnmarshalStrict(raw, &c)
	var fields map[string]json.RawMessage
	if len(raw) > 64<<10 || err != nil || len(strict) != 0 || json.Unmarshal(raw, &fields) != nil || len(fields) != 3 ||
		fields["before"] == nil || fields["after"] == nil || c.validate() != nil {
		return HoldChange{}, errors.New("DNS hold change requires a single exact before/after edge declaration")
	}
	return c, nil
}

func (c HoldChange) validate() error {
	if !validID(c.EdgeID) || reflect.DeepEqual(c.Before, c.After) {
		return errors.New("DNS hold change has no exact edge delta")
	}
	for _, h := range []*platformproducer.OfflineEdge{c.Before, c.After} {
		if h != nil && (h.EdgeID != c.EdgeID || platformproducer.ValidateOfflineEdges([]platformproducer.OfflineEdge{*h}) != nil) {
			return errors.New("DNS hold change contains invalid physical endpoint or recovery policy")
		}
	}
	return nil
}

func generation(prefix string, source model.PlatformArtifact, value any) (string, error) {
	d, err := platformconfig.Digest(struct {
		Previous ArtifactRef
		Change   any
	}{artifactRef(source), value})
	if err != nil {
		return "", err
	}
	return prefix + strings.TrimPrefix(d, "sha256:"), nil
}

func cloneContent(content map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(raw, &result)
	return result, err
}

// planNextDNS preserves the original artifact's JSON representation outside
// the two permitted fields. No topology or runtime configuration is inferred
// from a hostname, a Git lane or a default.
func planNextDNS(old model.PlatformArtifact, static model.PlatformArtifact, producer model.PlatformArtifact, c HoldChange) (model.PlatformArtifactCreateRequest, error) {
	var req model.PlatformArtifactCreateRequest
	if err := c.validate(); err != nil {
		return req, err
	}
	p, err := platformproducer.Decode(producer)
	if err != nil || p.Mode != "serving" || p.TargetScope != "global" || p.PublicationRole != "" || p.Serving == nil || p.Serving.SinglePublication ||
		old.ID != p.DNSPolicyArtifactID || old.ContentHash != p.DNSPolicyDigest || static.ID != p.StaticIntentArtifactID || static.ContentHash != p.StaticIntentDigest {
		return req, errors.New("DNS draft preparation requires current continuous global producer inputs")
	}
	intent, err := platformproducer.DecodeStaticIntent(static)
	if err != nil {
		return req, err
	}
	policy, err := platformproducer.DecodeProjectionPolicy(old, intent.Consumers, p.HostedZoneTemplates)
	if err != nil {
		return req, err
	}
	if policy.DNSPlacementMode != platformconfig.DNSPlacementConsumerReadiness || policy.DNSQueryPolicy == nil ||
		platformproducer.ValidatePinnedSources(p, intent, &policy) != nil {
		return req, errors.New("DNS producer does not have explicit global readiness policy")
	}
	holds := slices.Clone(policy.OfflineEdges)
	idx := slices.IndexFunc(holds, func(h platformproducer.OfflineEdge) bool { return h.EdgeID == c.EdgeID })
	if idx < 0 && c.Before != nil || idx >= 0 && (c.Before == nil || !reflect.DeepEqual(holds[idx], *c.Before)) {
		return req, errors.New("existing hold differs from the exact requested predecessor")
	}
	if idx >= 0 {
		holds = slices.Delete(holds, idx, idx+1)
	}
	if c.After != nil {
		holds = append(holds, *c.After)
	}
	slices.SortFunc(holds, func(a, b platformproducer.OfflineEdge) int { return strings.Compare(a.EdgeID, b.EdgeID) })
	if platformproducer.ValidateOfflineEdges(holds) != nil {
		return req, errors.New("successor hold inventory is invalid")
	}
	gen, err := generation("dns-native-", old, c)
	if err != nil {
		return req, err
	}
	content, err := cloneContent(old.Content)
	if err != nil {
		return req, err
	}
	content["generation"] = gen
	// Canonical storage turns typed hold structs into JSON values.
	if len(holds) == 0 {
		delete(content, "offline_edges")
	} else {
		content["offline_edges"] = holds
	}
	content, err = cloneContent(content)
	if err != nil {
		return req, err
	}
	req = model.PlatformArtifactCreateRequest{ArtifactKind: old.ArtifactKind, Scope: old.Scope, Generation: gen, Content: content, CompatibilityFloor: old.CompatibilityFloor}
	// Apply the same typed rule before making any API call. Real sequence and
	// artifact identity are assigned only by Fugue and checked again afterward.
	if old.GenerationSequence == math.MaxInt64 {
		return model.PlatformArtifactCreateRequest{}, errors.New("DNS artifact generation exhausted")
	}
	candidate := old
	candidate.ID, candidate.Generation, candidate.GenerationSequence, candidate.Content = "native-unpersisted-dns", gen, old.GenerationSequence+1, content
	candidate.ContentHash, err = platformconfig.Digest(content)
	if err != nil {
		return model.PlatformArtifactCreateRequest{}, err
	}
	next := p
	next.Generation, next.DNSPolicyArtifactID, next.DNSPolicyDigest = "unpersisted-next-producer", candidate.ID, candidate.ContentHash
	if err := platformproducer.ValidateOfflineEdgeUpdate(p, next, static, old, candidate); err != nil {
		return model.PlatformArtifactCreateRequest{}, err
	}
	return req, nil
}

func planNextProducer(old, nextDNS model.PlatformArtifact) (model.PlatformArtifactCreateRequest, error) {
	var req model.PlatformArtifactCreateRequest
	if err := validateArtifact(nextDNS, model.PlatformArtifactKindPolicySnapshot, "global"); err != nil {
		return req, err
	}
	gen, err := generation("producer-native-", old, artifactRef(nextDNS))
	if err != nil {
		return req, err
	}
	content, err := cloneContent(old.Content)
	if err != nil {
		return req, err
	}
	content["generation"], content["dns_policy_artifact_id"], content["dns_policy_digest"] = gen, nextDNS.ID, nextDNS.ContentHash
	return model.PlatformArtifactCreateRequest{ArtifactKind: old.ArtifactKind, Scope: old.Scope, Generation: gen, Content: content, CompatibilityFloor: old.CompatibilityFloor}, nil
}

// DraftPreparer is deliberately separate from Reader and from the eventual
// publication principal. It can create/validate immutable artifacts, never
// select a producer or alter a traffic lane. It is useful before cutover.
type DraftPreparer struct {
	reader *Reader
	token  string
	client *http.Client
}

func NewDraftPreparer(reader *Reader, draftToken string) (*DraftPreparer, error) {
	if reader == nil || draftToken == reader.token {
		return nil, errors.New("DNS preparation requires separate read and draft credentials")
	}
	client, err := NewReader(reader.origin, draftToken)
	if err != nil {
		return nil, err
	}
	return &DraftPreparer{reader, client.token, client.client}, nil
}

func (p *DraftPreparer) post(ctx context.Context, path string, value, out any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.reader.origin+path, bytes.NewReader(body))
	if err != nil {
		return errors.New("invalid DNS draft preparation request")
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return errors.New("DNS draft preparation result unknown; re-read immutable generation before retry")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("DNS draft preparation rejected (HTTP %d); no publication was requested", resp.StatusCode)
	}
	l := &io.LimitedReader{R: resp.Body, N: maxObservationBytes + 1}
	d := json.NewDecoder(l)
	if d.Decode(out) != nil || d.Decode(new(json.RawMessage)) != io.EOF || l.N <= 0 {
		return errors.New("DNS draft preparation response was not verified")
	}
	return nil
}

func matchingDraft(a model.PlatformArtifact, req model.PlatformArtifactCreateRequest) bool {
	hash, err := platformconfig.Digest(req.Content)
	computed, digestErr := platformconfig.Digest(a.Content)
	return err == nil && digestErr == nil && computed == hash && a.ContentHash == hash && a.Generation == req.Generation &&
		a.ScopeKey == req.Scope.Key && a.Scope == req.Scope && a.ArtifactKind == req.ArtifactKind && validID(a.ID) && a.CompatibilityFloor == req.CompatibilityFloor
}

func (p *DraftPreparer) readDraft(ctx context.Context, id string, req model.PlatformArtifactCreateRequest) (model.PlatformArtifact, error) {
	var response model.PlatformArtifactResponse
	if !validID(id) {
		return response.Artifact, errors.New("invalid draft identity")
	}
	if err := p.reader.get(ctx, "/v1/admin/artifacts/"+url.PathEscape(id), nil, &response); err != nil {
		return response.Artifact, err
	}
	if response.Artifact.ID != id || !matchingDraft(response.Artifact, req) {
		return model.PlatformArtifact{}, errors.New("existing generation does not match exact draft content")
	}
	return response.Artifact, nil
}

func (p *DraftPreparer) ensure(ctx context.Context, req model.PlatformArtifactCreateRequest) (model.PlatformArtifact, error) {
	var list model.PlatformArtifactListResponse
	if err := p.reader.get(ctx, "/v1/admin/artifacts", url.Values{"kind": {req.ArtifactKind}, "scope": {req.Scope.Key}, "generation": {req.Generation}, "limit": {"2"}}, &list); err != nil {
		return model.PlatformArtifact{}, err
	}
	var a model.PlatformArtifact
	switch len(list.Artifacts) {
	case 0:
		var created model.PlatformArtifactResponse
		if err := p.post(ctx, "/v1/admin/artifacts", req, &created); err != nil {
			return a, err
		}
		if !matchingDraft(created.Artifact, req) {
			return a, errors.New("new artifact does not match exact draft")
		}
		a = created.Artifact
	case 1:
		var err error
		a, err = p.readDraft(ctx, list.Artifacts[0].ID, req)
		if err != nil {
			return a, err
		}
	default:
		return a, errors.New("ambiguous immutable artifact generation")
	}
	if a.Status != model.PlatformArtifactStatusValidated {
		if a.Status != model.PlatformArtifactStatusDraft {
			return a, errors.New("existing immutable artifact is not a validated or pending draft")
		}
		var validated model.PlatformArtifactValidationResponse
		if err := p.post(ctx, "/v1/admin/artifacts/"+url.PathEscape(a.ID)+"/validate", model.PlatformArtifactValidateRequest{DryRun: false}, &validated); err != nil {
			return a, err
		}
		if !validated.Pass || validated.DryRun || validated.Artifact.ID != a.ID || !matchingDraft(validated.Artifact, req) {
			return a, errors.New("Fugue rejected exact DNS source validation")
		}
	}
	verified, err := p.reader.artifact(ctx, a.ID, req.ArtifactKind, req.Scope.Key)
	if err != nil || !matchingDraft(verified, req) {
		return model.PlatformArtifact{}, errors.New("independent artifact read did not return the exact validated draft")
	}
	return verified, nil
}

// Prepare persists only immutable draft inputs and calls Fugue's normal
// typed validation. The final GET-only shadow must still pass before review.
func (p *DraftPreparer) Prepare(ctx context.Context, change HoldChange, now func() time.Time) (Observation, error) {
	if p == nil || p.reader == nil || now == nil {
		return Observation{}, errors.New("DNS draft preparer is not configured")
	}
	old, full, lkg, err := p.reader.current(ctx)
	if err != nil {
		return Observation{}, err
	}
	if _, err := publicationRef(old, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow"); err != nil {
		return Observation{}, err
	}
	policy, err := platformproducer.Decode(old.Artifact)
	if err != nil {
		return Observation{}, err
	}
	if !lkg.ExpiresAt.After(now()) || lkg.ArtifactID != full.Artifact.ID || lkg.ContentHash != full.Artifact.ContentHash ||
		lkg.VerifiedByReleaseID != full.Release.ID || full.Release.VerificationState != model.PlatformArtifactVerificationStateVerified ||
		full.Artifact.Metadata[platformproducer.PolicyReleaseMetadata] != old.Release.ID {
		return Observation{}, errors.New("no verified full predecessor for draft preparation")
	}
	static, err := p.reader.artifact(ctx, policy.StaticIntentArtifactID, model.PlatformArtifactKindPlatformIntent, "global")
	if err != nil {
		return Observation{}, err
	}
	source, err := p.reader.artifact(ctx, policy.DNSPolicyArtifactID, model.PlatformArtifactKindPolicySnapshot, "global")
	if err != nil {
		return Observation{}, err
	}
	request, err := planNextDNS(source, static, old.Artifact, change)
	if err != nil {
		return Observation{}, err
	}
	nextDNS, err := p.ensure(ctx, request)
	if err != nil {
		return Observation{}, err
	}
	nextRequest, err := planNextProducer(old.Artifact, nextDNS)
	if err != nil {
		return Observation{}, err
	}
	next, err := p.ensure(ctx, nextRequest)
	if err != nil {
		return Observation{}, err
	}
	if _, _, err := ValidateSources(Sources{Producer: old, Full: full, LKG: lkg, Static: static, PreviousDNS: source, NextDNS: nextDNS, NextProducer: next}, now()); err != nil {
		return Observation{}, err
	}
	observed, err := p.reader.Observe(ctx, next.ID, now)
	if err != nil {
		return Observation{}, err
	}
	if !reflect.DeepEqual(observed.Sources.Producer, old) || !reflect.DeepEqual(observed.Sources.Full, full) || observed.Sources.LKG != lkg {
		return Observation{}, errors.New("selected predecessor changed during draft preparation")
	}
	return observed, nil
}
