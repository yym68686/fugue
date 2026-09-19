package platformconsumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
)

// Client provides assignment reads and runtime fact submission for consumers. It
// deliberately has no serving or business-database behavior.
type Client struct {
	BaseURL    string
	TokenFile  string
	HTTPClient *http.Client
}

var ErrNoServingAssignment = errors.New("no serving traffic assignment")

func (c Client) SyncServing(ctx context.Context, component, nodeID, scope, kind string) (Identity, model.PlatformConsumerAssignment, model.PlatformArtifact, model.PlatformArtifactRelease, error) {
	return c.syncChannel(ctx, component, nodeID, scope, kind, "serving")
}

type Identity struct {
	Token         string    `json:"token"`
	ExpiresAt     time.Time `json:"expires_at"`
	Component     string    `json:"component"`
	NodeID        string    `json:"node_id"`
	ScopeKey      string    `json:"scope_key"`
	ArtifactKinds []string  `json:"artifact_kinds"`
}

func (c Client) Sync(ctx context.Context, component, nodeID, scope, kind string) (Identity, model.PlatformConsumerAssignment, model.PlatformArtifact, model.PlatformArtifactRelease, error) {
	return c.SyncChannel(ctx, component, nodeID, scope, kind, model.PlatformArtifactReleaseChannelShadow)
}

// SyncChannel downloads exactly one declared lane. It never chooses a newer
// lane, falls back to another channel, applies the artifact or grants serving.
func (c Client) SyncChannel(ctx context.Context, component, nodeID, scope, kind, channel string) (Identity, model.PlatformConsumerAssignment, model.PlatformArtifact, model.PlatformArtifactRelease, error) {
	return c.syncChannel(ctx, component, nodeID, scope, kind, channel)
}
func (c Client) syncChannel(ctx context.Context, component, nodeID, scope, kind, channel string) (Identity, model.PlatformConsumerAssignment, model.PlatformArtifact, model.PlatformArtifactRelease, error) {
	switch channel {
	case "serving", model.PlatformArtifactReleaseChannelShadow, model.PlatformArtifactReleaseChannelGray, model.PlatformArtifactReleaseChannelFull:
	default:
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, errors.New("platform release channel is invalid")
	}
	base, err := url.Parse(strings.TrimSpace(c.BaseURL))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil {
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, errors.New("platform API endpoint is invalid")
	}
	base.RawQuery, base.Fragment = "", ""
	base.Path = strings.TrimRight(base.Path, "/")
	raw, err := ReadFile(c.TokenFile, 32768)
	if err != nil || len(raw) == 0 || len(raw) > 32768 {
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, errors.New("platform Pod credential unavailable")
	}
	var id Identity
	if err := c.json(ctx, base.String()+"/v1/platform-state/consumers/identity", strings.TrimSpace(string(raw)), http.MethodPost, nil, &id); err != nil {
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, err
	}
	if id.Token == "" || id.Component != component || id.NodeID != nodeID || id.ScopeKey != scope || !slices.Contains(id.ArtifactKinds, kind) || !id.ExpiresAt.After(time.Now().Add(10*time.Second)) {
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, errors.New("platform credential identity mismatch")
	}
	var assignments model.PlatformConsumerAssignmentResponse
	query := ""
	if channel == "serving" {
		query = "?serving_only=true"
	}
	if err := c.json(ctx, base.String()+"/v1/platform-state/consumers/assignment"+query, id.Token, http.MethodGet, nil, &assignments); err != nil {
		var status *responseStatusError
		if channel == "serving" && errors.As(err, &status) && status.Code == http.StatusNotFound {
			return id, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, ErrNoServingAssignment
		}
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, err
	}
	var chosen *model.PlatformConsumerAssignment
	for i := range assignments.Assignments {
		a := &assignments.Assignments[i]
		if a.ScopeKey == scope && a.ArtifactKind == kind && (a.ReleaseChannel == channel || (channel == "serving" && (a.ReleaseChannel == "gray" || a.ReleaseChannel == "full"))) {
			if chosen != nil {
				return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, errors.New("ambiguous platform release assignment")
			}
			chosen = a
		}
	}
	if chosen == nil {
		if channel == "serving" {
			return id, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, ErrNoServingAssignment
		}
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, errors.New("platform release assignment unavailable")
	}
	var envelope struct {
		Artifact   model.PlatformArtifact           `json:"artifact"`
		Assignment model.PlatformConsumerAssignment `json:"assignment"`
		Release    model.PlatformArtifactRelease    `json:"release"`
	}
	endpoint := base.String() + "/v1/platform-state/consumers/artifacts/" + url.PathEscape(chosen.ArtifactID) + "?expected_consumer_set_id=" + url.QueryEscape(chosen.ExpectedConsumerSetID)
	if err := c.json(ctx, endpoint, id.Token, http.MethodGet, nil, &envelope); err != nil {
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, err
	}
	if !reflect.DeepEqual(envelope.Assignment, *chosen) {
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, errors.New("platform artifact assignment mismatch")
	}
	return id, *chosen, envelope.Artifact, envelope.Release, nil
}

// CheckAssignment prevents a completed observation from being reported against
// a release or topology revision replaced while the consumer was working.
func (c Client) CheckAssignment(ctx context.Context, identity Identity, assignment model.PlatformConsumerAssignment) error {
	return c.checkAssignment(ctx, identity, assignment, false)
}
func (c Client) CheckServingAssignment(ctx context.Context, identity Identity, assignment model.PlatformConsumerAssignment) error {
	return c.checkAssignment(ctx, identity, assignment, true)
}
func (c Client) checkAssignment(ctx context.Context, identity Identity, assignment model.PlatformConsumerAssignment, serving bool) error {
	var current model.PlatformConsumerAssignmentResponse
	path := "/v1/platform-state/consumers/assignment"
	if serving {
		path += "?serving_only=true"
	}
	if err := c.requestJSON(ctx, path, identity.Token, http.MethodGet, nil, &current); err != nil {
		return err
	}
	matches := 0
	for _, item := range current.Assignments {
		if item.ScopeKey == assignment.ScopeKey && item.ArtifactKind == assignment.ArtifactKind && item.ReleaseChannel == assignment.ReleaseChannel {
			if !reflect.DeepEqual(item, assignment) {
				return errors.New("platform assignment changed during observation")
			}
			matches++
		}
	}
	if matches != 1 {
		return errors.New("platform assignment changed during observation")
	}
	return nil
}

func (c Client) PostJSON(ctx context.Context, path, token string, in, out any) error {
	return c.requestJSON(ctx, path, token, http.MethodPost, in, out)
}

func (c Client) requestJSON(ctx context.Context, path, token, method string, in, out any) error {
	base, err := url.Parse(strings.TrimSpace(c.BaseURL))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil {
		return errors.New("platform API endpoint is invalid")
	}
	base.RawQuery, base.Fragment = "", ""
	rel, err := url.Parse(path)
	if err != nil || rel.Host != "" || rel.Scheme != "" || rel.Fragment != "" {
		return errors.New("platform request path invalid")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/" + strings.TrimLeft(rel.Path, "/")
	base.RawQuery = rel.RawQuery
	return c.json(ctx, base.String(), token, method, in, out)
}

func (c Client) json(ctx context.Context, endpoint, token, method string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return errors.New("encode platform request failed")
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return errors.New("platform request invalid")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	} else {
		clone := *client
		clone.Timeout = 15 * time.Second
		client = &clone
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("platform request unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &responseStatusError{Code: resp.StatusCode}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil || len(raw) > 8<<20 {
		return errors.New("platform response exceeds limit or is incomplete")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if dec.Decode(out) != nil || dec.Decode(&struct{}{}) != io.EOF {
		return errors.New("platform response invalid")
	}
	return nil
}

type responseStatusError struct{ Code int }

func (e *responseStatusError) Error() string {
	return fmt.Sprintf("platform request rejected: HTTP %d", e.Code)
}

func ReadFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, errors.New("platform file exceeds limit or is incomplete")
	}
	return raw, nil
}

// ReleaseSet downloads the exact signed parent through an existing child
// assignment; this never broadens the caller's scope or artifact capability.
func (c Client) ReleaseSet(ctx context.Context, id Identity, a model.PlatformConsumerAssignment, r model.PlatformArtifactRelease) (model.PlatformArtifact, error) {
	if a.ReleaseSetID == "" || a.ExpectedConsumerSetID == "" || id.ScopeKey != a.ScopeKey || !slices.Contains(id.ArtifactKinds, a.ArtifactKind) {
		return model.PlatformArtifact{}, errors.New("parent assignment identity invalid")
	}
	base, err := url.Parse(strings.TrimSpace(c.BaseURL))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil {
		return model.PlatformArtifact{}, errors.New("platform API endpoint is invalid")
	}
	base.RawQuery, base.Fragment = "", ""
	endpoint := strings.TrimRight(base.String(), "/") + "/v1/platform-state/consumers/artifacts/" + url.PathEscape(a.ReleaseSetID) + "?expected_consumer_set_id=" + url.QueryEscape(a.ExpectedConsumerSetID)
	var reply struct {
		Artifact   model.PlatformArtifact           `json:"artifact"`
		Assignment model.PlatformConsumerAssignment `json:"assignment"`
		Release    model.PlatformArtifactRelease    `json:"release"`
	}
	if err := c.json(ctx, endpoint, id.Token, http.MethodGet, nil, &reply); err != nil {
		return model.PlatformArtifact{}, err
	}
	if !reflect.DeepEqual(reply.Assignment, a) || !reflect.DeepEqual(reply.Release, r) || reply.Artifact.ID != a.ReleaseSetID || reply.Artifact.ArtifactKind != model.PlatformArtifactKindReleaseSet || reply.Artifact.ScopeKey != a.ScopeKey {
		return model.PlatformArtifact{}, errors.New("parent release binding changed")
	}
	return reply.Artifact, nil
}
