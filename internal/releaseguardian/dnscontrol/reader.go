package dnscontrol

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/productioncontrol"
)

const maxObservationBytes = 128 << 20

// Reader is a strictly GET-only observer of the existing API. The origin
// and dedicated read credential come from runtime/operator configuration,
// not a Git declaration or an untrusted server response. No POST helper is
// available to shadow or draft commands.
type Reader struct {
	origin string
	token  string
	client *http.Client
}

func NewReader(origin, token string) (*Reader, error) {
	if !validOrigin(origin) || token == "" || len(token) > 4096 || strings.ContainsAny(token, " \t\r\n") {
		return nil, errors.New("DNS control observation requires an explicit HTTPS origin and dedicated read token")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	client := &http.Client{Transport: tr, Timeout: 90 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Reader{origin: origin, token: token, client: client}, nil
}

func (r *Reader) get(ctx context.Context, path string, query url.Values, out any) error {
	if r == nil || r.client == nil {
		return errors.New("DNS control observer unavailable")
	}
	target := r.origin + path
	if len(query) != 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return errors.New("invalid DNS control observation request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := r.client.Do(req)
	if err != nil {
		return errors.New("DNS control observation is unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength > maxObservationBytes {
		return errors.New("DNS control observation did not return a successful bounded response")
	}
	limited := &io.LimitedReader{R: resp.Body, N: maxObservationBytes + 1}
	decoder := json.NewDecoder(limited)
	if decoder.Decode(out) != nil || decoder.Decode(new(json.RawMessage)) != io.EOF || limited.N <= 0 {
		return errors.New("DNS control observation is incomplete, invalid or excessive")
	}
	return nil
}

func (r *Reader) selected(ctx context.Context, kind, scope, channel string) (model.PlatformStateArtifactResponse, error) {
	var response model.PlatformStateArtifactResponse
	err := r.get(ctx, "/v1/platform-state/artifacts/"+kind, url.Values{"scope_key": {scope}, "channel": {channel}}, &response)
	return response, err
}

func (r *Reader) artifact(ctx context.Context, id, kind, scope string) (model.PlatformArtifact, error) {
	var response model.PlatformArtifactResponse
	if !validID(id) {
		return response.Artifact, errors.New("invalid exact DNS control artifact identifier")
	}
	err := r.get(ctx, "/v1/admin/artifacts/"+url.PathEscape(id), nil, &response)
	a := response.Artifact
	if err != nil {
		return a, err
	}
	if a.ID != id || validateArtifact(a, kind, scope) != nil {
		return model.PlatformArtifact{}, errors.New("DNS control artifact was not the exact validated source")
	}
	return a, nil
}

func (r *Reader) current(ctx context.Context) (Publication, Publication, model.PlatformLKGSnapshot, error) {
	var producer, full Publication
	p, err := r.selected(ctx, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil {
		return producer, full, model.PlatformLKGSnapshot{}, err
	}
	f, err := r.selected(ctx, model.PlatformArtifactKindReleaseSet, "global", "full")
	if err != nil {
		return producer, full, model.PlatformLKGSnapshot{}, err
	}
	if p.Artifact == nil || p.Release == nil || f.Artifact == nil || f.Release == nil || f.LKG == nil ||
		p.Generation != p.Artifact.Generation || f.Generation != f.Artifact.Generation {
		return producer, full, model.PlatformLKGSnapshot{}, errors.New("no current exact producer and verified full selection")
	}
	producer, full = Publication{*p.Artifact, *p.Release}, Publication{*f.Artifact, *f.Release}
	return producer, full, *f.LKG, nil
}

func (r *Reader) preview(ctx context.Context, id string) (Preview, error) {
	var p Preview
	if !validID(id) {
		return p, errors.New("invalid producer preview identifier")
	}
	err := r.get(ctx, "/v1/admin/platform-config/routes/project", url.Values{"producer_policy_artifact_id": {id}, "view": {"runtime_summary"}}, &p)
	return p, err
}

type Observation struct {
	Sources Sources
	Before  Preview
	After   Preview
}

// Observe verifies the selected producer/full/LKG before and after the
// paired read. It never renews a selection behind the owner's back.
func (r *Reader) Observe(ctx context.Context, nextID string, now func() time.Time) (Observation, error) {
	var result Observation
	if now == nil {
		return result, errors.New("no DNS shadow clock")
	}
	s := &result.Sources
	var err error
	s.Producer, s.Full, s.LKG, err = r.current(ctx)
	if err != nil {
		return Observation{}, err
	}
	previous, err := platformproducer.Decode(s.Producer.Artifact)
	if err != nil {
		return Observation{}, err
	}
	s.NextProducer, err = r.artifact(ctx, nextID, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope)
	if err != nil {
		return Observation{}, err
	}
	next, err := platformproducer.Decode(s.NextProducer)
	if err != nil {
		return Observation{}, err
	}
	s.Static, err = r.artifact(ctx, previous.StaticIntentArtifactID, model.PlatformArtifactKindPlatformIntent, "global")
	if err != nil {
		return Observation{}, err
	}
	s.PreviousDNS, err = r.artifact(ctx, previous.DNSPolicyArtifactID, model.PlatformArtifactKindPolicySnapshot, "global")
	if err != nil {
		return Observation{}, err
	}
	s.NextDNS, err = r.artifact(ctx, next.DNSPolicyArtifactID, model.PlatformArtifactKindPolicySnapshot, "global")
	if err != nil {
		return Observation{}, err
	}
	if _, _, err := ValidateSources(*s, now()); err != nil {
		return Observation{}, err
	}
	result.Before, err = r.preview(ctx, s.Producer.Artifact.ID)
	if err != nil {
		return Observation{}, err
	}
	result.After, err = r.preview(ctx, s.NextProducer.ID)
	if err != nil {
		return Observation{}, err
	}
	producer, full, lkg, err := r.current(ctx)
	if err != nil {
		return Observation{}, err
	}
	if producer.Release.ID != s.Producer.Release.ID || full.Release.ID != s.Full.Release.ID ||
		producer.Release.FencingToken != s.Producer.Release.FencingToken || full.Release.FencingToken != s.Full.Release.FencingToken ||
		artifactRef(producer.Artifact) != artifactRef(s.Producer.Artifact) || artifactRef(full.Artifact) != artifactRef(s.Full.Artifact) ||
		lkg != s.LKG {
		return Observation{}, errors.New("producer, full or verified LKG selection changed during the DNS shadow")
	}
	s.Producer, s.Full, s.LKG = producer, full, lkg
	_, projection, err := ValidateSources(*s, now())
	if err != nil {
		return Observation{}, err
	}
	if _, err := ValidatePreview(result.Before, result.After, *s, projection, now()); err != nil {
		return Observation{}, err
	}
	return result, nil
}

func (r *Reader) Draft(ctx context.Context, j *productioncontrol.Journal, cluster, request, nextID string, now func() time.Time) (productioncontrol.Submission, error) {
	if j == nil {
		return productioncontrol.Submission{}, errors.New("DNS draft requires persisted production control")
	}
	head, err := j.ReadResource(ctx, Scope(cluster))
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	observed, err := r.Observe(ctx, nextID, now)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	check, err := j.ReadResource(ctx, Scope(cluster))
	if err != nil || check != head {
		return productioncontrol.Submission{}, errors.New("DNS producer journal head changed during read-only shadow")
	}
	return Draft(cluster, request, r.origin, head, observed.Sources, observed.Before, observed.After, now())
}
