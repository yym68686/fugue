package edge

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/edgegroupfront"
	"fugue/internal/model"
)

const (
	edgeControlRouteSourceV1          = "edge-control-group-authority/v1"
	edgeControlSigningKeyringSchemaV1 = "edge-control-group-bundle-signing-keyring/v1"
	edgeControlBundlePath             = "/v1/edge/routes"
	edgeControlCandidateBundlePath    = "/v1/edge/candidate-routes"
	edgeControlGroupHeader            = "X-Fugue-Edge-Group"
	edgeControlGenerationHeader       = "X-Fugue-Edge-Route-Bundle-Generation"
	edgeControlPublicationHeader      = "X-Fugue-Edge-Publication-Sequence"
	edgeControlRecoveryEpochHeader    = "X-Fugue-Edge-Recovery-Epoch"
	edgeControlCandidateRecordHeader  = "X-Fugue-Candidate-Record-Digest"
	edgeControlReleaseRecordHeader    = "X-Fugue-Release-Record-Digest"
	edgeControlCandidateSlotHeader    = "X-Fugue-Candidate-Worker-Slot"
	maxEdgeRouteCredentialBytes       = 512
	maxEdgeRouteVerifierKeyringBytes  = 128 << 10
)

var (
	edgeRouteKeyIDPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{2,63}$`)
	edgeRouteDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type routePublicationMetadata struct {
	Source              string
	GroupID             string
	Generation          string
	PublicationSequence uint64
	RecoveryEpoch       uint64
	Candidate           bool
	CandidateRecord     string
	ReleaseRecord       string
	WorkerSlot          string
}

type edgeRouteVerifierKeyringFile struct {
	Schema     string                   `json:"schema"`
	Generation uint64                   `json:"generation"`
	Group      edgeRouteVerifierKeyring `json:"group"`
}

type edgeRouteVerifierKeyring struct {
	GroupID       string   `json:"edge_group_id"`
	PrimaryKeyID  string   `json:"primary_key_id"`
	PrimaryKey    string   `json:"primary_key"`
	PreviousKeyID string   `json:"previous_key_id,omitempty"`
	PreviousKey   string   `json:"previous_key,omitempty"`
	RevokedKeyIDs []string `json:"revoked_key_ids,omitempty"`
}

// RouteBundleSourceConfig is owned by the Edge worker and remains independent
// from the Core API configuration used for heartbeat and desired state.
type RouteBundleSourceConfig struct {
	URL                 string
	CandidateURL        string
	TokenFile           string
	VerifierKeyringFile string
	ActivationStateFile string
}

// RouteBundleSourceFromEnv reads only the Edge-owned group publication source.
func RouteBundleSourceFromEnv() RouteBundleSourceConfig {
	return RouteBundleSourceConfig{
		URL:                 strings.TrimSpace(os.Getenv("FUGUE_EDGE_ROUTE_BUNDLE_URL")),
		CandidateURL:        strings.TrimSpace(os.Getenv("FUGUE_EDGE_CANDIDATE_ROUTE_BUNDLE_URL")),
		TokenFile:           strings.TrimSpace(os.Getenv("FUGUE_EDGE_ROUTE_BUNDLE_TOKEN_FILE")),
		VerifierKeyringFile: strings.TrimSpace(os.Getenv("FUGUE_EDGE_ROUTE_BUNDLE_VERIFIER_KEYRING_FILE")),
		ActivationStateFile: strings.TrimSpace(os.Getenv("FUGUE_EDGE_INVENTORY_ACTIVATION_STATE_FILE")),
	}
}

func (s *Service) edgeControlRouteSourceEnabled() bool {
	if s == nil {
		return false
	}
	return strings.TrimSpace(s.RouteBundleSource.URL) != "" ||
		strings.TrimSpace(s.RouteBundleSource.TokenFile) != "" ||
		strings.TrimSpace(s.RouteBundleSource.VerifierKeyringFile) != ""
}

func validateEdgeControlRouteSourceConfig(cfg configEdgeRouteSource) error {
	if cfg.url == "" && cfg.tokenFile == "" && cfg.verifierFile == "" {
		return nil
	}
	if cfg.url == "" || cfg.tokenFile == "" || cfg.verifierFile == "" {
		return errors.New("FUGUE_EDGE_ROUTE_BUNDLE_URL, FUGUE_EDGE_ROUTE_BUNDLE_TOKEN_FILE, and FUGUE_EDGE_ROUTE_BUNDLE_VERIFIER_KEYRING_FILE must be configured together")
	}
	parsed, err := url.Parse(cfg.url)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.Path != edgeControlBundlePath || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("FUGUE_EDGE_ROUTE_BUNDLE_URL must be an exact HTTP(S) /v1/edge/routes endpoint without credentials, query, or fragment")
	}
	for _, item := range []struct {
		name string
		path string
	}{{"FUGUE_EDGE_ROUTE_BUNDLE_TOKEN_FILE", cfg.tokenFile}, {"FUGUE_EDGE_ROUTE_BUNDLE_VERIFIER_KEYRING_FILE", cfg.verifierFile}} {
		if !filepath.IsAbs(item.path) || filepath.Clean(item.path) != item.path {
			return fmt.Errorf("%s must be an absolute normalized path", item.name)
		}
	}
	if cfg.candidateURL == "" {
		return nil
	}
	candidate, err := url.Parse(cfg.candidateURL)
	if err != nil || candidate.Scheme != parsed.Scheme || candidate.Host != parsed.Host || candidate.User != nil ||
		candidate.Path != edgeControlCandidateBundlePath || candidate.RawQuery != "" || candidate.Fragment != "" ||
		!filepath.IsAbs(cfg.activationFile) || filepath.Clean(cfg.activationFile) != cfg.activationFile {
		return errors.New("candidate route source must use the current authority host and an exact activation-bound endpoint")
	}
	return nil
}

type configEdgeRouteSource struct {
	url            string
	candidateURL   string
	tokenFile      string
	verifierFile   string
	activationFile string
}

type routeSourceSelection struct {
	config               configEdgeRouteSource
	candidate            bool
	activationGeneration uint64
	activeSlot           string
	expectedGeneration   string
}

func newEdgeRouteBundleHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ForceAttemptHTTP2 = false
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("edge route bundle redirect is forbidden")
		},
	}
}

func (s *Service) edgeRouteSourceConfig() configEdgeRouteSource {
	return configEdgeRouteSource{
		url:            strings.TrimSpace(s.RouteBundleSource.URL),
		candidateURL:   strings.TrimSpace(s.RouteBundleSource.CandidateURL),
		tokenFile:      strings.TrimSpace(s.RouteBundleSource.TokenFile),
		verifierFile:   strings.TrimSpace(s.RouteBundleSource.VerifierKeyringFile),
		activationFile: strings.TrimSpace(s.RouteBundleSource.ActivationStateFile),
	}
}

func (s *Service) selectRouteBundleSource() (routeSourceSelection, error) {
	cfg := s.edgeRouteSourceConfig()
	if err := validateEdgeControlRouteSourceConfig(cfg); err != nil {
		return routeSourceSelection{}, err
	}
	selection := routeSourceSelection{config: cfg}
	if cfg.candidateURL == "" {
		return selection, nil
	}
	slot := strings.TrimSpace(s.Config.EdgeSlot)
	if slot != model.EdgeSlotA && slot != model.EdgeSlotB {
		return routeSourceSelection{}, errors.New("candidate route source requires an exact worker slot identity")
	}
	activation, exists, err := edgegroupfront.ReadActivationState(cfg.activationFile)
	if err != nil || !exists || activation.GroupID != strings.TrimSpace(s.Config.EdgeGroupID) ||
		(activation.ActiveSlot != model.EdgeSlotA && activation.ActiveSlot != model.EdgeSlotB) {
		return routeSourceSelection{}, errors.New("candidate route source activation state is unavailable or unbound")
	}
	selection.activationGeneration = activation.Generation
	selection.activeSlot = activation.ActiveSlot
	if activation.ActiveSlot != slot {
		selection.candidate = true
		selection.config.url = cfg.candidateURL
	} else {
		// Front activation is the traffic authority. Until Edge Control has
		// promoted the exact verified candidate into its current stream, an
		// active worker must retain its already-loaded bundle instead of
		// accepting an older current publication during the CAS window.
		selection.expectedGeneration = activation.BundleGeneration
	}
	return selection, nil
}

func validateRouteBundleActivationBinding(selection routeSourceSelection, publication routePublicationMetadata) error {
	if selection.candidate || selection.expectedGeneration == "" {
		return nil
	}
	if publication.Generation != selection.expectedGeneration {
		return errors.New("edge-control current publication is not bound to Front activation")
	}
	return nil
}

func (s *Service) validateRouteBundleSourceSelection(selection routeSourceSelection) error {
	if selection.config.candidateURL == "" {
		return nil
	}
	activation, exists, err := edgegroupfront.ReadActivationState(selection.config.activationFile)
	if err != nil || !exists || activation.GroupID != strings.TrimSpace(s.Config.EdgeGroupID) ||
		activation.Generation != selection.activationGeneration || activation.ActiveSlot != selection.activeSlot {
		return errors.New("edge route authority changed while loading a bundle")
	}
	isCandidate := activation.ActiveSlot != strings.TrimSpace(s.Config.EdgeSlot)
	if isCandidate != selection.candidate {
		return errors.New("edge route authority slot changed while loading a bundle")
	}
	return nil
}

func loadEdgeRouteReaderToken(path string) (string, error) {
	raw, err := readBoundedPrivateFile(path, maxEdgeRouteCredentialBytes)
	if err != nil {
		return "", fmt.Errorf("read edge route reader credential: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	zeroBytes(raw)
	if len(token) < 32 || len(token) > 256 || strings.ContainsAny(token, "\r\n\t ") {
		return "", errors.New("edge route reader credential is invalid")
	}
	return token, nil
}

func loadEdgeRouteVerifierKeyring(path, expectedGroupID string) (bundleauth.Keyring, error) {
	raw, err := readBoundedPrivateFile(path, maxEdgeRouteVerifierKeyringBytes)
	if err != nil {
		return bundleauth.Keyring{}, fmt.Errorf("read edge route verifier keyring: %w", err)
	}
	defer zeroBytes(raw)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var file edgeRouteVerifierKeyringFile
	if err := decoder.Decode(&file); err != nil {
		return bundleauth.Keyring{}, errors.New("edge route verifier keyring is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || file.Schema != edgeControlSigningKeyringSchemaV1 || file.Generation == 0 {
		return bundleauth.Keyring{}, errors.New("edge route verifier keyring is invalid")
	}
	group := file.Group
	expectedGroupID = strings.TrimSpace(expectedGroupID)
	if group.GroupID != expectedGroupID || !edgeRouteKeyIDPattern.MatchString(group.PrimaryKeyID) || (group.PreviousKeyID == "") != (group.PreviousKey == "") {
		return bundleauth.Keyring{}, errors.New("edge route verifier keyring identity is invalid")
	}
	decodeKey := func(id, encoded string) (string, error) {
		if id == "" && encoded == "" {
			return "", nil
		}
		if !edgeRouteKeyIDPattern.MatchString(id) {
			return "", errors.New("edge route verifier key id is invalid")
		}
		decoded, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil || len(decoded) < 32 || len(decoded) > 64 {
			zeroBytes(decoded)
			return "", errors.New("edge route verifier key material is invalid")
		}
		value := string(decoded)
		zeroBytes(decoded)
		return value, nil
	}
	primary, err := decodeKey(group.PrimaryKeyID, group.PrimaryKey)
	if err != nil {
		return bundleauth.Keyring{}, err
	}
	previous, err := decodeKey(group.PreviousKeyID, group.PreviousKey)
	if err != nil {
		zeroString(&primary)
		return bundleauth.Keyring{}, err
	}
	if (group.PreviousKeyID != "" && group.PreviousKeyID == group.PrimaryKeyID) || (primary == previous && previous != "") {
		zeroString(&primary)
		zeroString(&previous)
		return bundleauth.Keyring{}, errors.New("edge route verifier keys are duplicated")
	}
	seenRevoked := make(map[string]struct{}, len(group.RevokedKeyIDs))
	for _, keyID := range group.RevokedKeyIDs {
		if !edgeRouteKeyIDPattern.MatchString(keyID) || keyID == group.PrimaryKeyID {
			zeroString(&primary)
			zeroString(&previous)
			return bundleauth.Keyring{}, errors.New("edge route revoked verifier key id is invalid")
		}
		if _, exists := seenRevoked[keyID]; exists {
			zeroString(&primary)
			zeroString(&previous)
			return bundleauth.Keyring{}, errors.New("edge route revoked verifier key id is duplicated")
		}
		seenRevoked[keyID] = struct{}{}
	}
	return bundleauth.NewKeyring(primary, group.PrimaryKeyID, previous, group.PreviousKeyID, group.RevokedKeyIDs), nil
}

func readBoundedPrivateFile(path string, maximum int64) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("projected file path is invalid")
	}
	root, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return nil, errors.New("projected file directory is unavailable")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(root, resolved)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("projected file escaped its mount directory")
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, err
	}
	privateMode := info.Mode().Perm() == 0o600
	if info.Mode().Perm() == 0o640 {
		if stat, ok := info.Sys().(*syscall.Stat_t); ok && int(stat.Gid) == os.Getegid() {
			privateMode = true
		}
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum || !privateMode {
		return nil, errors.New("projected file mode or size is invalid")
	}
	raw, err := os.ReadFile(resolved)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maximum || int64(len(raw)) != info.Size() {
		zeroBytes(raw)
		return nil, errors.New("projected file exceeds size limit")
	}
	return raw, nil
}

func routePublicationFromResponse(headers map[string][]string, bundle model.EdgeRouteBundle, expectedGroupID string) (routePublicationMetadata, error) {
	get := func(key string) string {
		for header, values := range headers {
			if strings.EqualFold(header, key) && len(values) == 1 {
				return strings.TrimSpace(values[0])
			}
		}
		return ""
	}
	groupID := get(edgeControlGroupHeader)
	generation := get(edgeControlGenerationHeader)
	sequence, sequenceErr := strconv.ParseUint(get(edgeControlPublicationHeader), 10, 64)
	recoveryEpoch, recoveryErr := strconv.ParseUint(get(edgeControlRecoveryEpochHeader), 10, 64)
	if groupID == "" || groupID != strings.TrimSpace(expectedGroupID) || groupID != strings.TrimSpace(bundle.EdgeGroupID) ||
		generation == "" || generation != strings.TrimSpace(bundle.Generation) || sequenceErr != nil || sequence == 0 || recoveryErr != nil ||
		bundle.Version != groupPublicationVersion(generation, sequence, recoveryEpoch) {
		return routePublicationMetadata{}, errors.New("edge-control route publication headers are invalid or unbound")
	}
	return routePublicationMetadata{Source: edgeControlRouteSourceV1, GroupID: groupID, Generation: generation, PublicationSequence: sequence, RecoveryEpoch: recoveryEpoch}, nil
}

func bindCandidatePublication(headers map[string][]string, publication routePublicationMetadata, expectedSlot string) (routePublicationMetadata, error) {
	get := func(key string) string {
		for header, values := range headers {
			if strings.EqualFold(header, key) && len(values) == 1 {
				return strings.TrimSpace(values[0])
			}
		}
		return ""
	}
	candidateRecord := get(edgeControlCandidateRecordHeader)
	releaseRecord := get(edgeControlReleaseRecordHeader)
	workerSlot := get(edgeControlCandidateSlotHeader)
	if !edgeRouteDigestPattern.MatchString(candidateRecord) || !edgeRouteDigestPattern.MatchString(releaseRecord) ||
		(workerSlot != model.EdgeSlotA && workerSlot != model.EdgeSlotB) || workerSlot != strings.TrimSpace(expectedSlot) {
		return routePublicationMetadata{}, errors.New("edge-control candidate publication identity is invalid or unbound")
	}
	publication.Candidate = true
	publication.CandidateRecord = candidateRecord
	publication.ReleaseRecord = releaseRecord
	publication.WorkerSlot = workerSlot
	return publication, nil
}

func groupPublicationVersion(generation string, sequence, recoveryEpoch uint64) string {
	return strings.TrimSpace(generation) + ".p" + strconv.FormatUint(sequence, 10) + ".r" + strconv.FormatUint(recoveryEpoch, 10)
}

func validateRoutePublicationAdvance(current, candidate routePublicationMetadata) error {
	if candidate.Source != edgeControlRouteSourceV1 || candidate.GroupID == "" || candidate.Generation == "" || candidate.PublicationSequence == 0 {
		return errors.New("edge-control route publication metadata is invalid")
	}
	if current.Source == "" {
		return nil
	}
	if current.Source != candidate.Source || current.GroupID != candidate.GroupID || candidate.PublicationSequence <= current.PublicationSequence {
		// Candidate and current are separate publication streams. Once the
		// activation CAS promotes this worker, the first current observation
		// may legitimately have a lower sequence but must carry a newer
		// recovery epoch. The activation-bound source selection is validated
		// by the caller before this transition is accepted.
		if !(current.Candidate && !candidate.Candidate && candidate.RecoveryEpoch > current.RecoveryEpoch) {
			return errors.New("edge-control route publication CAS regressed or replayed")
		}
	}
	// Candidate publications are deliberately issued from the candidate ledger
	// and carry recovery epoch zero.  They are compared against the current
	// traffic publication only by the monotonic publication sequence; requiring
	// the current recovery epoch here would reject a valid inactive candidate
	// whenever the current/LKG had previously undergone compensation.
	if !candidate.Candidate && candidate.RecoveryEpoch < current.RecoveryEpoch {
		return errors.New("edge-control route publication CAS regressed or replayed")
	}
	return nil
}

func routePublicationFromCache(cached cacheFile) routePublicationMetadata {
	if strings.TrimSpace(cached.RouteBundleSource) == "" {
		return routePublicationMetadata{}
	}
	return routePublicationMetadata{
		Source:              strings.TrimSpace(cached.RouteBundleSource),
		GroupID:             strings.TrimSpace(cached.Bundle.EdgeGroupID),
		Generation:          strings.TrimSpace(cached.Bundle.Generation),
		PublicationSequence: cached.PublicationSequence,
		RecoveryEpoch:       cached.RecoveryEpoch,
		Candidate:           cached.Candidate,
		CandidateRecord:     strings.TrimSpace(cached.CandidateRecordDigest),
		ReleaseRecord:       strings.TrimSpace(cached.ReleaseRecordDigest),
		WorkerSlot:          strings.TrimSpace(cached.CandidateWorkerSlot),
	}
}

func (s *Service) currentRoutePublicationAndBundle() (routePublicationMetadata, *model.EdgeRouteBundle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	metadata := s.routePublication
	if s.bundle == nil {
		return metadata, nil
	}
	bundle := *s.bundle
	return metadata, &bundle
}

func (s *Service) validateCachedRouteSource(cached cacheFile) error {
	metadata := routePublicationFromCache(cached)
	if !s.edgeControlRouteSourceEnabled() {
		if metadata.Source != "" {
			return errors.New("legacy edge route source cannot load an edge-control publication cache")
		}
		return nil
	}
	if cached.Version != cacheFileVersion || metadata.Source != edgeControlRouteSourceV1 || metadata.GroupID != strings.TrimSpace(s.Config.EdgeGroupID) ||
		metadata.Generation == "" || metadata.PublicationSequence == 0 || cached.Bundle.Version != groupPublicationVersion(metadata.Generation, metadata.PublicationSequence, metadata.RecoveryEpoch) {
		return errors.New("edge-control route publication cache is invalid or belongs to another source")
	}
	if metadata.Candidate && (!edgeRouteDigestPattern.MatchString(metadata.CandidateRecord) || !edgeRouteDigestPattern.MatchString(metadata.ReleaseRecord) ||
		(metadata.WorkerSlot != model.EdgeSlotA && metadata.WorkerSlot != model.EdgeSlotB)) {
		return errors.New("edge-control candidate publication cache is invalid")
	}
	return validateNonCatastrophicGroupBundle(nil, cached.Bundle, false)
}

func validateNonCatastrophicGroupBundle(current *model.EdgeRouteBundle, candidate model.EdgeRouteBundle, recoveryAdvanced bool) error {
	candidateRoutes := routableRouteIdentities(candidate)
	if len(candidateRoutes) == 0 {
		return errors.New("edge-control route candidate has no active routable upstreams")
	}
	if current == nil || recoveryAdvanced {
		return nil
	}
	currentRoutes := routableRouteIdentities(*current)
	if len(currentRoutes) >= 2 && len(candidateRoutes)*2 < len(currentRoutes) {
		return fmt.Errorf("edge-control route candidate catastrophically drops routable routes from %d to %d", len(currentRoutes), len(candidateRoutes))
	}
	return nil
}

func routableRouteIdentities(bundle model.EdgeRouteBundle) map[string]struct{} {
	out := make(map[string]struct{})
	for _, route := range bundle.Routes {
		if !strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive) || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) || !routeHasRoutableUpstream(route) {
			continue
		}
		host := normalizeRouteHost(route.Hostname)
		if host == "" {
			continue
		}
		out[host+"\x00"+model.NormalizeAppRoutePathPrefix(route.PathPrefix)] = struct{}{}
	}
	return out
}

func routeHasRoutableUpstream(route model.EdgeRouteBinding) bool {
	if len(route.Upstreams) == 0 {
		return validRouteUpstreamURL(route.UpstreamURL)
	}
	for _, upstream := range route.Upstreams {
		if upstream.Weight > 0 && validRouteUpstreamURL(upstream.UpstreamURL) {
			return true
		}
	}
	return false
}

func validRouteUpstreamURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" && parsed.User == nil
}

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func zeroString(value *string) {
	if value != nil {
		*value = ""
	}
}
