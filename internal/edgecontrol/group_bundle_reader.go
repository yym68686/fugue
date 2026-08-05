package edgecontrol

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	GroupBundleReadPathV1            = "/v1/edge/routes"
	GroupBundleReaderKeyringSchemaV1 = "edge-control-group-bundle-reader-keyring/v1"
	GroupBundleGenerationHeader      = "X-Fugue-Edge-Route-Bundle-Generation"
	GroupBundleGroupHeader           = "X-Fugue-Edge-Group"
	GroupBundlePublicationHeader     = "X-Fugue-Edge-Publication-Sequence"
	GroupBundleRecoveryEpochHeader   = "X-Fugue-Edge-Recovery-Epoch"
	maxGroupBundleReaderKeyringBytes = 128 << 10
)

var groupBundleReaderIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{2,63}$`)

type GroupBundleReaderStore interface {
	ReadGroupAuthority(context.Context, string) (GroupAuthorityState, error)
}

type GroupBundleHandlerConfig struct {
	Store      GroupBundleReaderStore
	GroupIDs   []string
	KeyringDir string
	Now        func() time.Time
}

type groupBundleReaderCredential struct {
	CredentialID  string `json:"credential_id"`
	EdgeID        string `json:"edge_id"`
	TokenDigest   string `json:"token_digest"`
	NotBeforeUnix int64  `json:"not_before_unix"`
	NotAfterUnix  int64  `json:"not_after_unix"`
	Revoked       bool   `json:"revoked"`
}

type groupBundleReaderKeyring struct {
	Schema      string                        `json:"schema"`
	Generation  uint64                        `json:"generation"`
	GroupID     string                        `json:"edge_group_id"`
	Credentials []groupBundleReaderCredential `json:"credentials"`
}

type groupBundleHandler struct {
	store      GroupBundleReaderStore
	groups     map[string]struct{}
	keyringDir string
	now        func() time.Time
}

func NewGroupBundleHandler(config GroupBundleHandlerConfig) (http.Handler, error) {
	if config.Store == nil {
		return nil, errors.New("edge-control group bundle reader store is nil")
	}
	groups, err := normalizeGroupIDs(config.GroupIDs)
	if err != nil {
		return nil, err
	}
	keyringDir := strings.TrimSpace(config.KeyringDir)
	if keyringDir == "" || !filepath.IsAbs(keyringDir) || filepath.Clean(keyringDir) != keyringDir {
		return nil, errors.New("edge-control group bundle reader keyring directory must be an absolute normalized path")
	}
	allowed := make(map[string]struct{}, len(groups))
	for _, groupID := range groups {
		allowed[groupID] = struct{}{}
	}
	now := func() time.Time { return time.Now().UTC() }
	if config.Now != nil {
		now = func() time.Time { return config.Now().UTC() }
	}
	return &groupBundleHandler{store: config.Store, groups: allowed, keyringDir: keyringDir, now: now}, nil
}

func (handler *groupBundleHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet || request.URL.Path != GroupBundleReadPathV1 || request.URL.RawQuery == "" {
		http.NotFound(w, request)
		return
	}
	query := request.URL.Query()
	if len(query) != 2 || len(query["edge_id"]) != 1 || len(query["edge_group_id"]) != 1 {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	groupID := normalizeGroupID(query.Get("edge_group_id"))
	edgeID := normalizeEdgeIdentity(query.Get("edge_id"))
	authorizations := request.Header.Values("Authorization")
	token := ""
	if len(authorizations) == 1 && strings.HasPrefix(authorizations[0], "Bearer ") {
		token = strings.TrimPrefix(authorizations[0], "Bearer ")
	}
	if query.Get("edge_group_id") != groupID || query.Get("edge_id") != edgeID || edgeID == "" || len(token) < 32 || len(token) > 256 || strings.ContainsAny(token, "\r\n\t ") {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	if _, allowed := handler.groups[groupID]; !allowed {
		writeGroupBundleError(w, http.StatusForbidden, "group_rejected")
		return
	}
	if err := authenticateGroupBundleReader(filepath.Join(handler.keyringDir, groupID+".json"), groupID, edgeID, token, handler.now()); err != nil {
		writeGroupBundleError(w, http.StatusUnauthorized, "credential_rejected")
		return
	}
	state, err := handler.store.ReadGroupAuthority(request.Context(), groupID)
	if err != nil || !state.PublishedExists {
		writeGroupBundleError(w, http.StatusServiceUnavailable, "group_bundle_unavailable")
		return
	}
	published := state.Published
	if err := validateGroupPublishedBundle(groupID, published); err != nil || !published.Bundle.ValidUntil.After(handler.now()) {
		writeGroupBundleError(w, http.StatusServiceUnavailable, "group_bundle_unavailable")
		return
	}
	etag := strconv.Quote(published.Digest)
	w.Header().Set("ETag", etag)
	w.Header().Set(GroupBundleGenerationHeader, published.Bundle.Generation)
	w.Header().Set(GroupBundleGroupHeader, groupID)
	w.Header().Set(GroupBundlePublicationHeader, strconv.FormatUint(published.PublicationSequence, 10))
	w.Header().Set(GroupBundleRecoveryEpochHeader, strconv.FormatUint(published.RecoveryEpoch, 10))
	w.Header().Set("Cache-Control", "private, no-cache, no-store")
	if strings.TrimSpace(request.Header.Get("If-None-Match")) == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, published.Bundle)
}

func authenticateGroupBundleReader(path, groupID, edgeID, token string, now time.Time) error {
	keyring, err := loadGroupBundleReaderKeyring(path, groupID)
	if err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(token))
	now = now.UTC()
	matched := false
	active := false
	for _, credential := range keyring.Credentials {
		if credential.EdgeID != edgeID {
			continue
		}
		provided, decodeErr := hex.DecodeString(strings.TrimPrefix(credential.TokenDigest, "sha256:"))
		if decodeErr != nil || len(provided) != sha256.Size || !hmac.Equal(provided, digest[:]) {
			continue
		}
		matched = true
		notBefore := time.Unix(credential.NotBeforeUnix, 0).UTC()
		notAfter := time.Unix(credential.NotAfterUnix, 0).UTC()
		if !credential.Revoked && !now.Before(notBefore) && now.Before(notAfter) {
			active = true
		}
	}
	if !matched || !active {
		return errors.New("edge-control group bundle reader credential is inactive")
	}
	return nil
}

func loadGroupBundleReaderKeyring(path, expectedGroupID string) (groupBundleReaderKeyring, error) {
	raw, err := readPrivateProjectedFile(path, maxGroupBundleReaderKeyringBytes)
	if err != nil {
		return groupBundleReaderKeyring{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var keyring groupBundleReaderKeyring
	if err := decoder.Decode(&keyring); err != nil {
		return groupBundleReaderKeyring{}, errors.New("edge-control group bundle reader keyring is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || keyring.Schema != GroupBundleReaderKeyringSchemaV1 || keyring.Generation == 0 ||
		normalizeGroupID(keyring.GroupID) != normalizeGroupID(expectedGroupID) || keyring.GroupID != normalizeGroupID(keyring.GroupID) ||
		len(keyring.Credentials) == 0 || len(keyring.Credentials) > 64 {
		return groupBundleReaderKeyring{}, errors.New("edge-control group bundle reader keyring is invalid")
	}
	seenIDs := make(map[string]struct{}, len(keyring.Credentials))
	seenDigests := make(map[string]struct{}, len(keyring.Credentials))
	for _, credential := range keyring.Credentials {
		if !groupBundleReaderIDPattern.MatchString(credential.CredentialID) || normalizeEdgeIdentity(credential.EdgeID) == "" || credential.EdgeID != normalizeEdgeIdentity(credential.EdgeID) ||
			credential.NotBeforeUnix <= 0 || credential.NotAfterUnix <= credential.NotBeforeUnix ||
			credential.TokenDigest != strings.ToLower(credential.TokenDigest) || !strings.HasPrefix(credential.TokenDigest, "sha256:") || len(credential.TokenDigest) != len("sha256:")+sha256.Size*2 {
			return groupBundleReaderKeyring{}, errors.New("edge-control group bundle reader credential is invalid")
		}
		if _, err := hex.DecodeString(strings.TrimPrefix(credential.TokenDigest, "sha256:")); err != nil {
			return groupBundleReaderKeyring{}, errors.New("edge-control group bundle reader credential digest is invalid")
		}
		if _, duplicate := seenIDs[credential.CredentialID]; duplicate {
			return groupBundleReaderKeyring{}, errors.New("edge-control group bundle reader credential id is duplicated")
		}
		if _, duplicate := seenDigests[credential.TokenDigest]; duplicate {
			return groupBundleReaderKeyring{}, errors.New("edge-control group bundle reader credential digest is duplicated")
		}
		seenIDs[credential.CredentialID] = struct{}{}
		seenDigests[credential.TokenDigest] = struct{}{}
	}
	return keyring, nil
}

func writeGroupBundleError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, struct {
		Schema string `json:"schema"`
		Error  string `json:"error"`
	}{Schema: "edge-control-error/v1", Error: code})
}
