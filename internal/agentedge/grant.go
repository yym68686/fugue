// Package agentedge implements authorization and selection for Runtime Agent
// control requests. A public discovery result is never an authorization input.
package agentedge

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/staticedgecontract"
)

const GrantSchema = "fugue.agent-edge-grant/v1"
const GrantPurpose = "agent-control"
const PolicyScope = "agent-edge-control"
const MaxGrantBytes = 256 << 10
const signatureDomain = "fugue.agent-edge-grant.ed25519/v1\n"

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)
var topologyIdentifier = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var hostnamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,251}[a-z0-9]$`)

// Publication binds the permission to the exact signed configuration from
// which it was derived. A rollback must have its own newer publication.
type Publication struct {
	ServingGroupID      string    `json:"serving_group_id"`
	ReleaseSetID        string    `json:"release_set_id"`
	ReleaseSetDigest    string    `json:"release_set_digest"`
	RouteArtifactID     string    `json:"route_artifact_id"`
	RouteArtifactDigest string    `json:"route_artifact_digest"`
	PolicyDigest        string    `json:"policy_digest"`
	IntentDigest        string    `json:"intent_digest"`
	InputSnapshotDigest string    `json:"input_snapshot_digest"`
	TopologyDigest      string    `json:"topology_digest"`
	ScopeKey            string    `json:"scope_key"`
	ReleaseID           string    `json:"release_id"`
	Channel             string    `json:"channel"`
	FencingToken        int64     `json:"fencing_token"`
	PublishedAt         time.Time `json:"published_at"`
}

// PolicyReference is independent of any cell's traffic publication. Policies
// and rollbacks are ordered by their own published authority, not a code SHA.
type PolicyReference struct {
	ArtifactID     string    `json:"artifact_id"`
	ArtifactDigest string    `json:"artifact_digest"`
	ReleaseID      string    `json:"release_id"`
	Channel        string    `json:"channel"`
	FencingToken   int64     `json:"fencing_token"`
	PublishedAt    time.Time `json:"published_at"`
}

// Policy is signed configuration, not an Agent-local fallback default.
type Policy struct {
	ProbeIntervalSeconds     int `json:"probe_interval_seconds"`
	ProbeTimeoutMilliseconds int `json:"probe_timeout_milliseconds"`
	FactMaxAgeSeconds        int `json:"fact_max_age_seconds"`
	FailureThreshold         int `json:"failure_threshold"`
	BetterSampleThreshold    int `json:"better_sample_threshold"`
	SwitchImprovementPercent int `json:"switch_improvement_percent"`
	SwitchCooldownSeconds    int `json:"switch_cooldown_seconds"`
	StandbyCount             int `json:"standby_count"`
	DesiredDistinctCells     int `json:"desired_distinct_cells"`
	MaxCandidates            int `json:"max_candidates"`
}

func (p Policy) Validate() error {
	if p.ProbeIntervalSeconds < 5 || p.ProbeIntervalSeconds > 300 ||
		p.ProbeTimeoutMilliseconds < 100 || p.ProbeTimeoutMilliseconds > 10000 ||
		p.ProbeTimeoutMilliseconds >= p.ProbeIntervalSeconds*1000 ||
		p.FactMaxAgeSeconds < p.ProbeIntervalSeconds*2 || p.FactMaxAgeSeconds > 300 ||
		p.FailureThreshold < 1 || p.FailureThreshold > 10 ||
		p.BetterSampleThreshold < 2 || p.BetterSampleThreshold > 20 ||
		p.SwitchImprovementPercent < 1 || p.SwitchImprovementPercent > 100 ||
		p.SwitchCooldownSeconds < p.ProbeIntervalSeconds || p.SwitchCooldownSeconds > 86400 ||
		p.StandbyCount < 1 || p.StandbyCount > 4 ||
		p.DesiredDistinctCells < 1 || p.DesiredDistinctCells > p.StandbyCount+1 ||
		p.MaxCandidates < p.StandbyCount+1 || p.MaxCandidates > 32 {
		return errors.New("Agent Edge selection policy is outside bounded limits")
	}
	return nil
}

type Candidate struct {
	Publication        Publication       `json:"publication"`
	EdgeID             string            `json:"edge_id"`
	AuthorityCellID    string            `json:"authority_cell_id"`
	Address            string            `json:"address"`
	RouteDigests       []string          `json:"route_digests"`
	FailureDomains     map[string]string `json:"failure_domains"`
	EvidenceDigest     string            `json:"evidence_digest"`
	EvidenceObservedAt time.Time         `json:"evidence_observed_at"`
	EvidenceValidUntil time.Time         `json:"evidence_valid_until"`
}

type Grant struct {
	Schema             string          `json:"schema"`
	Purpose            string          `json:"purpose"`
	Audience           string          `json:"audience"`
	Origin             string          `json:"origin"`
	Mode               string          `json:"mode"`
	PolicyReference    PolicyReference `json:"policy_reference"`
	Policy             Policy          `json:"policy"`
	MinimumCandidates  int             `json:"minimum_candidates"`
	MinDistinctCells   int             `json:"min_distinct_cells"`
	MinDistinctDomains map[string]int  `json:"min_distinct_domains"`
	Candidates         []Candidate     `json:"candidates"`
	IssuedAt           time.Time       `json:"issued_at"`
	ValidUntil         time.Time       `json:"valid_until"`
}

func (g Grant) Validate() error {
	u, err := url.Parse(g.Origin)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" ||
		!hostnamePattern.MatchString(u.Hostname()) || u.Host != u.Hostname() || g.Origin != "https://"+u.Hostname() {
		return errors.New("Agent Edge grant requires a canonical HTTPS origin on port 443")
	}
	for _, label := range strings.Split(u.Hostname(), ".") {
		if len(label) == 0 || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return errors.New("Agent Edge grant hostname is invalid")
		}
	}
	if _, err := netip.ParseAddr(u.Hostname()); err == nil {
		return errors.New("Agent Edge grant TLS origin must be a DNS hostname")
	}
	if g.Schema != GrantSchema || g.Purpose != GrantPurpose || !identifier.MatchString(g.Audience) ||
		(g.Mode != "shadow" && g.Mode != "active") ||
		g.IssuedAt.IsZero() || !g.ValidUntil.After(g.IssuedAt) || g.ValidUntil.Sub(g.IssuedAt) > 5*time.Minute ||
		g.Policy.Validate() != nil || g.MinimumCandidates < 1 || len(g.Candidates) < g.MinimumCandidates || len(g.Candidates) > g.Policy.MaxCandidates ||
		g.MinDistinctCells < 1 || g.MinDistinctCells > len(g.Candidates) || len(g.MinDistinctDomains) > 8 {
		return errors.New("Agent Edge grant identity, lifetime or candidate bounds are invalid")
	}
	p := g.PolicyReference
	if !identifier.MatchString(p.ArtifactID) || !digestPattern.MatchString(p.ArtifactDigest) || !identifier.MatchString(p.ReleaseID) ||
		(p.Channel != "shadow" && p.Channel != "full") ||
		p.FencingToken <= 0 || p.PublishedAt.IsZero() || p.PublishedAt.After(g.IssuedAt) {
		return errors.New("Agent Edge policy reference is invalid")
	}
	cells, addresses := map[string]bool{}, map[string]bool{}
	cellPublications := map[string]Publication{}
	domains := map[string]map[string]bool{}
	for dimension, minimum := range g.MinDistinctDomains {
		if len(dimension) > 128 || !topologyIdentifier.MatchString(dimension) || dimension == "country" || dimension == "region" || minimum < 1 || minimum > len(g.Candidates) {
			return errors.New("Agent Edge grant failure-domain constraint is invalid")
		}
		domains[dimension] = map[string]bool{}
	}
	for i, c := range g.Candidates {
		if err := c.Publication.validate(g.IssuedAt); err != nil {
			return err
		}
		if previous, exists := cellPublications[c.AuthorityCellID]; exists && previous != c.Publication {
			return errors.New("Agent Edge cell contains mixed publication evidence")
		}
		cellPublications[c.AuthorityCellID] = c.Publication
		ip, parseErr := netip.ParseAddr(c.Address)
		if len(c.EdgeID) > 128 || len(c.AuthorityCellID) > 128 || !topologyIdentifier.MatchString(c.EdgeID) || !topologyIdentifier.MatchString(c.AuthorityCellID) || i > 0 && g.Candidates[i-1].EdgeID >= c.EdgeID ||
			parseErr != nil || ip.String() != c.Address || ip.Zone() != "" || !platformconfig.PublicDNSFlattenIP(ip) || addresses[c.Address] ||
			len(c.RouteDigests) == 0 || len(c.RouteDigests) > 64 || len(c.FailureDomains) == 0 || len(c.FailureDomains) > 8 ||
			!digestPattern.MatchString(c.EvidenceDigest) || c.EvidenceObservedAt.IsZero() || c.EvidenceObservedAt.After(g.IssuedAt) ||
			!c.EvidenceValidUntil.After(c.EvidenceObservedAt) || g.ValidUntil.After(c.EvidenceValidUntil) ||
			g.ValidUntil.After(c.EvidenceObservedAt.Add(time.Duration(g.Policy.FactMaxAgeSeconds)*time.Second)) {
			return errors.New("Agent Edge grant candidate or original evidence lifetime is invalid")
		}
		addresses[c.Address], cells[c.AuthorityCellID] = true, true
		for j, digest := range c.RouteDigests {
			if !digestPattern.MatchString(digest) || j > 0 && c.RouteDigests[j-1] >= digest {
				return errors.New("Agent Edge grant route requirements are not canonical")
			}
		}
		for dimension, value := range c.FailureDomains {
			if len(dimension) > 128 || len(value) > 128 || !topologyIdentifier.MatchString(dimension) || !topologyIdentifier.MatchString(value) || dimension == "country" || dimension == "region" {
				return errors.New("Agent Edge failure domains cannot be country or region labels")
			}
		}
		for dimension := range domains {
			if c.FailureDomains[dimension] == "" {
				return errors.New("Agent Edge candidate lacks a required failure domain")
			}
			domains[dimension][c.FailureDomains[dimension]] = true
		}
	}
	if len(cells) < g.MinDistinctCells {
		return errors.New("Agent Edge candidates do not meet authority-cell diversity")
	}
	for dimension, values := range domains {
		if len(values) < g.MinDistinctDomains[dimension] {
			return errors.New("Agent Edge candidates do not meet failure-domain diversity")
		}
	}
	return nil
}

func (source Publication) validate(issuedAt time.Time) error {
	if !identifier.MatchString(source.ReleaseSetID) || !identifier.MatchString(source.RouteArtifactID) || !identifier.MatchString(source.ReleaseID) ||
		len(source.ServingGroupID) > 128 || !topologyIdentifier.MatchString(source.ServingGroupID) ||
		(source.Channel != "gray" && source.Channel != "full") || source.ScopeKey != "global" || source.FencingToken <= 0 ||
		source.PublishedAt.IsZero() || source.PublishedAt.After(issuedAt) {
		return errors.New("Agent Edge grant publication is invalid")
	}
	for _, digest := range []string{source.ReleaseSetDigest, source.RouteArtifactDigest, source.PolicyDigest, source.IntentDigest, source.InputSnapshotDigest, source.TopologyDigest} {
		if !digestPattern.MatchString(digest) {
			return errors.New("Agent Edge grant publication digest is invalid")
		}
	}
	return nil
}

type SignedGrant struct {
	Grant     Grant  `json:"grant"`
	KeyID     string `json:"key_id"`
	Digest    string `json:"digest"`
	Signature string `json:"signature"`
}

// TrustKey comes from independently provisioned client configuration. It is
// never read from a discovery response or from the signed grant itself.
type TrustKey struct {
	PublicKey ed25519.PublicKey
	NotBefore time.Time
	NotAfter  time.Time
	Revoked   bool
}

type VerifiedGrant struct {
	signed         SignedGrant
	cellWatermarks map[string]Publication
}

func (g VerifiedGrant) View() Grant    { return cloneGrant(g.signed.Grant) }
func (g VerifiedGrant) Digest() string { return g.signed.Digest }

func grantDigest(g Grant) string {
	raw, _ := json.Marshal(g)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func signedMessage(keyID, digest string) []byte {
	return []byte(signatureDomain + keyID + "\n" + digest)
}

func Sign(g Grant, keyID string, privateKey ed25519.PrivateKey) (SignedGrant, error) {
	if err := g.Validate(); err != nil {
		return SignedGrant{}, err
	}
	if !identifier.MatchString(keyID) || len(privateKey) != ed25519.PrivateKeySize {
		return SignedGrant{}, errors.New("Agent Edge grant signing key is invalid")
	}
	s := SignedGrant{Grant: cloneGrant(g), KeyID: keyID, Digest: grantDigest(g)}
	s.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, signedMessage(keyID, s.Digest)))
	return s, nil
}

// Verify rejects stale, foreign, equivocated and replayed permission without
// modifying the previously accepted grant. Publication timestamps order lanes;
// a full/gray fencing token alone cannot safely order different lanes.
func Verify(raw []byte, keys map[string]TrustKey, audience, origin string, previous *VerifiedGrant, now time.Time) (VerifiedGrant, error) {
	var s SignedGrant
	if len(raw) > MaxGrantBytes || staticedgecontract.StrictJSON(raw, &s) != nil || s.Grant.Validate() != nil ||
		!identifier.MatchString(s.KeyID) || s.Digest != grantDigest(s.Grant) ||
		s.Grant.Audience != audience || s.Grant.Origin != origin || now.IsZero() || now.Before(s.Grant.IssuedAt) || !now.Before(s.Grant.ValidUntil) {
		return VerifiedGrant{}, errors.New("Agent Edge grant is invalid, foreign or expired")
	}
	key, exists := keys[s.KeyID]
	signature, err := base64.RawURLEncoding.DecodeString(s.Signature)
	if !exists || key.Revoked || len(key.PublicKey) != ed25519.PublicKeySize || key.NotBefore.IsZero() || key.NotAfter.IsZero() ||
		now.Before(key.NotBefore) || !now.Before(key.NotAfter) || s.Grant.IssuedAt.Before(key.NotBefore) || s.Grant.ValidUntil.After(key.NotAfter) ||
		err != nil || !ed25519.Verify(key.PublicKey, signedMessage(s.KeyID, s.Digest), signature) {
		return VerifiedGrant{}, errors.New("Agent Edge grant has no valid independently trusted signature")
	}
	watermarks := map[string]Publication{}
	if previous != nil && previous.signed.Digest != "" {
		old := previous.signed.Grant
		if old.Audience != audience || old.Origin != origin || s.Grant.IssuedAt.Before(old.IssuedAt) ||
			s.Grant.PolicyReference.PublishedAt.Before(old.PolicyReference.PublishedAt) ||
			s.Grant.PolicyReference.PublishedAt.Equal(old.PolicyReference.PublishedAt) && s.Grant.PolicyReference != old.PolicyReference ||
			s.Grant.PolicyReference.PublishedAt.After(old.PolicyReference.PublishedAt) && s.Grant.PolicyReference.Channel == old.PolicyReference.Channel && s.Grant.PolicyReference.FencingToken <= old.PolicyReference.FencingToken ||
			s.Grant.IssuedAt.Equal(old.IssuedAt) && s.Digest != previous.signed.Digest {
			return VerifiedGrant{}, errors.New("Agent Edge grant replay or publication equivocation rejected")
		}
		if s.Grant.PolicyReference == old.PolicyReference && (s.Grant.Mode != old.Mode || s.Grant.Policy != old.Policy ||
			s.Grant.MinimumCandidates != old.MinimumCandidates || s.Grant.MinDistinctCells != old.MinDistinctCells || !maps.Equal(s.Grant.MinDistinctDomains, old.MinDistinctDomains)) {
			return VerifiedGrant{}, errors.New("Agent Edge policy fields changed without a new configuration publication")
		}
		watermarks = maps.Clone(previous.cellWatermarks)
	}
	for _, c := range s.Grant.Candidates {
		if old, exists := watermarks[c.AuthorityCellID]; exists && (c.Publication.PublishedAt.Before(old.PublishedAt) ||
			c.Publication.PublishedAt.Equal(old.PublishedAt) && c.Publication != old ||
			c.Publication.PublishedAt.After(old.PublishedAt) && c.Publication.Channel == old.Channel && c.Publication.FencingToken <= old.FencingToken) {
			return VerifiedGrant{}, errors.New("Agent Edge cell publication replay or equivocation rejected")
		}
		watermarks[c.AuthorityCellID] = c.Publication
	}
	if len(watermarks) > 256 {
		return VerifiedGrant{}, errors.New("Agent Edge publication history exceeds the bounded cell inventory")
	}
	return VerifiedGrant{signed: s, cellWatermarks: watermarks}, nil
}

func (g VerifiedGrant) Live(keys map[string]TrustKey, now time.Time) bool {
	if g.signed.Digest == "" {
		return false
	}
	raw, err := json.Marshal(g.signed)
	if err != nil {
		return false
	}
	_, err = Verify(raw, keys, g.signed.Grant.Audience, g.signed.Grant.Origin, nil, now)
	return err == nil
}

func cloneGrant(g Grant) Grant {
	g.MinDistinctDomains = maps.Clone(g.MinDistinctDomains)
	g.Candidates = slices.Clone(g.Candidates)
	for i := range g.Candidates {
		g.Candidates[i].RouteDigests = slices.Clone(g.Candidates[i].RouteDigests)
		g.Candidates[i].FailureDomains = maps.Clone(g.Candidates[i].FailureDomains)
	}
	return g
}
