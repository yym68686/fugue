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

type Identity struct {
	Token         string    `json:"token"`
	ExpiresAt     time.Time `json:"expires_at"`
	Component     string    `json:"component"`
	NodeID        string    `json:"node_id"`
	ScopeKey      string    `json:"scope_key"`
	ArtifactKinds []string  `json:"artifact_kinds"`
}

func (c Client) Sync(ctx context.Context, component, nodeID, scope, kind string) (Identity, model.PlatformConsumerAssignment, model.PlatformArtifact, model.PlatformArtifactRelease, error) {
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
	if err := c.json(ctx, base.String()+"/v1/platform-state/consumers/assignment", id.Token, http.MethodGet, nil, &assignments); err != nil {
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, err
	}
	var chosen *model.PlatformConsumerAssignment
	for i := range assignments.Assignments {
		a := &assignments.Assignments[i]
		if a.ScopeKey == scope && a.ArtifactKind == kind && a.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow {
			if chosen != nil {
				return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, errors.New("ambiguous platform shadow assignment")
			}
			chosen = a
		}
	}
	if chosen == nil {
		return Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifact{}, model.PlatformArtifactRelease{}, errors.New("platform shadow assignment unavailable")
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

func (c Client) PostJSON(ctx context.Context, path, token string, in, out any) error {
	base, err := url.Parse(strings.TrimSpace(c.BaseURL))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil {
		return errors.New("platform API endpoint is invalid")
	}
	base.RawQuery, base.Fragment = "", ""
	base.Path = strings.TrimRight(base.Path, "/") + "/" + strings.TrimLeft(path, "/")
	return c.json(ctx, base.String(), token, http.MethodPost, in, out)
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
		return fmt.Errorf("platform request rejected: HTTP %d", resp.StatusCode)
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
