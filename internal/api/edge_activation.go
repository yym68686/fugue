package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/edgeauthkey"
	"fugue/internal/httpx"
	"fugue/internal/model"
)

var edgeActivationReleaseFencePattern = regexp.MustCompile(`^github:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+:[1-9][0-9]{0,19}:[1-9][0-9]{0,9}:[0-9a-f]{40}$`)

func (s *Server) handleAdminGetEdgeActivation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect edge activation")
		return
	}
	state, err := s.store.GetEdgeActivationState()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	instances, epochs, err := s.store.ListEdgeNodeInstances("")
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	legacyNodes, legacyGroups, err := s.store.ListEdgeNodes("")
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"activation": state, "instances": instances, "active_epochs": epochs, "legacy_nodes": legacyNodes, "legacy_groups": legacyGroups})
}

func (s *Server) handleAdminAdvanceEdgeActivation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can advance edge activation")
		return
	}
	var advance model.EdgeActivationAdvance
	if err := httpx.DecodeJSON(r, &advance); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.authorizeEdgeActivationAdvance(&advance, time.Now().UTC()); err != nil {
		httpx.WriteError(w, http.StatusForbidden, "edge activation release authorization is invalid")
		return
	}
	advance.Actor = strings.TrimSpace(principal.ActorType + "/" + principal.ActorID)
	state, err := s.store.AdvanceEdgeActivation(advance)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"activation": state})
}

func (s *Server) handleAdminAdvanceEdgeRemediation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can advance edge remediation")
		return
	}
	var advance model.EdgeRemediationAdvance
	if err := httpx.DecodeJSON(r, &advance); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.authorizeEdgeRemediationAdvance(&advance, time.Now().UTC()); err != nil {
		httpx.WriteError(w, http.StatusForbidden, "edge remediation release authorization is invalid")
		return
	}
	advance.Actor = strings.TrimSpace(principal.ActorType + "/" + principal.ActorID)
	state, err := s.store.AdvanceEdgeRemediation(advance)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"activation": state})
}

func (s *Server) authorizeEdgeRemediationAdvance(advance *model.EdgeRemediationAdvance, now time.Time) error {
	if advance == nil || strings.TrimSpace(s.edgeActivationPlanSigningKey) == "" || strings.TrimSpace(s.edgeActivationPlanSigningKeyID) == "" {
		return errInvalidEdgeActivationAuthorization
	}
	authorization := advance.Authorization
	if authorization.Schema != model.EdgeRemediationAuthorizationSchemaV1 || authorization.KeyID != s.edgeActivationPlanSigningKeyID || authorization.KeyGeneration != s.edgeActivationPlanSigningKeyGeneration || !s.edgeActivationSigningProjectionStable() || !edgeActivationReleaseFencePattern.MatchString(authorization.ReleaseFence) || !validSHA256Evidence(authorization.ActionNonce) {
		return errInvalidEdgeActivationAuthorization
	}
	validUntil, err := time.Parse(time.RFC3339, authorization.ValidUntil)
	if err != nil || authorization.ValidUntil != validUntil.UTC().Format(time.RFC3339) || validUntil.Before(now) || validUntil.After(now.Add(15*time.Minute)) {
		return errInvalidEdgeActivationAuthorization
	}
	material := edgeRemediationAuthorizationMaterial(*advance)
	mac := hmac.New(sha256.New, []byte(s.edgeActivationPlanSigningKey))
	_, _ = mac.Write([]byte(material))
	want := "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(authorization.Signature), []byte(want)) {
		return errInvalidEdgeActivationAuthorization
	}
	digest := sha256.Sum256([]byte(material + "\n" + authorization.Signature))
	advance.ReleaseFence = authorization.ReleaseFence
	advance.Nonce = authorization.ActionNonce
	advance.AuthorizationDigest = "sha256:" + hex.EncodeToString(digest[:])
	advance.AuthorizationKeyID = authorization.KeyID
	advance.AuthorizationKeyGeneration = authorization.KeyGeneration
	advance.AuthorizationRunnerObservedSecretUID = authorization.RunnerObservedSecretUID
	advance.AuthorizationRunnerObservedSecretVersion = authorization.RunnerObservedSecretVersion
	return nil
}

func edgeRemediationAuthorizationMaterial(advance model.EdgeRemediationAdvance) string {
	a := advance.Authorization
	t := advance.Target
	return strings.Join([]string{
		a.Schema, a.KeyID, a.KeyGeneration, a.ReleaseFence, a.ActionNonce, a.ValidUntil, a.RunnerObservedSecretUID, a.RunnerObservedSecretVersion,
		strconv.FormatUint(advance.ExpectedActivationGeneration, 10), strconv.FormatUint(advance.ExpectedActionSequence, 10), advance.ToPhase,
		advance.ActiveEvidenceDigest, advance.PlatformEvidenceDigest, advance.KubernetesDigest,
		t.EdgeID, t.EdgeGroupID, t.Slot, t.InstanceUID, t.ReleaseEpoch, t.DaemonSetName, t.DaemonSetUID, t.DaemonSetVersion, t.FailureClass,
	}, "\n")
}

func (s *Server) authorizeEdgeActivationAdvance(advance *model.EdgeActivationAdvance, now time.Time) error {
	if advance == nil || strings.TrimSpace(s.edgeActivationPlanSigningKey) == "" || strings.TrimSpace(s.edgeActivationPlanSigningKeyID) == "" {
		return errInvalidEdgeActivationAuthorization
	}
	authorization := advance.Authorization
	if authorization.Schema != model.EdgeActivationAuthorizationSchemaV1 || authorization.KeyID != s.edgeActivationPlanSigningKeyID || authorization.KeyGeneration != s.edgeActivationPlanSigningKeyGeneration || !s.edgeActivationSigningProjectionStable() || !edgeActivationReleaseFencePattern.MatchString(authorization.ReleaseFence) || !validSHA256Evidence(authorization.PhaseNonce) || !validSHA256Evidence(authorization.ExpectedInstancesDigest) || !validSHA256Evidence(authorization.ActiveEpochsDigest) {
		return errInvalidEdgeActivationAuthorization
	}
	validUntil, err := time.Parse(time.RFC3339, authorization.ValidUntil)
	if err != nil || authorization.ValidUntil != validUntil.UTC().Format(time.RFC3339) || validUntil.Before(now) || validUntil.After(now.Add(15*time.Minute)) {
		return errInvalidEdgeActivationAuthorization
	}
	expectedDigest, epochsDigest, err := edgeActivationMaterialDigests(advance.ExpectedInstances, advance.ActiveEpochs)
	if err != nil || expectedDigest != authorization.ExpectedInstancesDigest || epochsDigest != authorization.ActiveEpochsDigest {
		return errInvalidEdgeActivationAuthorization
	}
	material := edgeActivationAuthorizationMaterial(*advance)
	mac := hmac.New(sha256.New, []byte(s.edgeActivationPlanSigningKey))
	_, _ = mac.Write([]byte(material))
	want := "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(authorization.Signature), []byte(want)) {
		return errInvalidEdgeActivationAuthorization
	}
	digest := sha256.Sum256([]byte(material + "\n" + authorization.Signature))
	advance.ReleaseFence = authorization.ReleaseFence
	advance.PhaseNonce = authorization.PhaseNonce
	advance.AuthorizationDigest = "sha256:" + hex.EncodeToString(digest[:])
	advance.AuthorizationKeyID = authorization.KeyID
	advance.AuthorizationKeyGeneration = authorization.KeyGeneration
	advance.AuthorizationRunnerObservedSecretUID = authorization.RunnerObservedSecretUID
	advance.AuthorizationRunnerObservedSecretVersion = authorization.RunnerObservedSecretVersion
	return nil
}

var errInvalidEdgeActivationAuthorization = &edgeActivationAuthorizationError{}

type edgeActivationAuthorizationError struct{}

func (*edgeActivationAuthorizationError) Error() string {
	return "invalid edge activation authorization"
}

func edgeActivationAuthorizationMaterial(advance model.EdgeActivationAdvance) string {
	a := advance.Authorization
	return strings.Join([]string{
		a.Schema, a.KeyID, a.KeyGeneration, a.ReleaseFence, a.PhaseNonce, a.ValidUntil, a.RunnerObservedSecretUID, a.RunnerObservedSecretVersion,
		strconv.FormatUint(advance.ExpectedGeneration, 10), advance.ToPhase,
		advance.PlanDigest, advance.EvidenceDigest, advance.ReleaseID,
		advance.ReleaseRecordUID, advance.ReleaseRecordVersion, advance.ReleaseRecordDigest,
		advance.LegacySnapshotDigest, advance.APIReplicaGeneration,
		a.ExpectedInstancesDigest, a.ActiveEpochsDigest,
	}, "\n")
}

func (s *Server) edgeActivationSigningProjectionStable() bool {
	if s.edgeActivationPlanSigningProjectionDir == "" {
		return s.edgeActivationPlanSigningKeyGeneration != ""
	}
	snapshot, err := edgeauthkey.Load(s.edgeActivationPlanSigningProjectionDir)
	return err == nil && snapshot.KeyID == s.edgeActivationPlanSigningKeyID && snapshot.Generation == s.edgeActivationPlanSigningKeyGeneration && hmac.Equal([]byte(snapshot.Key), []byte(s.edgeActivationPlanSigningKey))
}

// SignEdgeActivationAdvance is used only by the in-pod signer command. The
// key stays in the read-only Secret projection; callers receive a bounded
// envelope, never the key material.
func SignEdgeActivationAdvance(advance *model.EdgeActivationAdvance, key, keyID, keyGeneration, secretUID, secretVersion string) error {
	if advance == nil || len(key) < 32 || strings.TrimSpace(keyID) == "" || strings.TrimSpace(keyGeneration) == "" || strings.TrimSpace(secretUID) == "" || strings.TrimSpace(secretVersion) == "" {
		return errInvalidEdgeActivationAuthorization
	}
	expectedDigest, epochsDigest, err := edgeActivationMaterialDigests(advance.ExpectedInstances, advance.ActiveEpochs)
	if err != nil {
		return err
	}
	a := &advance.Authorization
	a.Schema = model.EdgeActivationAuthorizationSchemaV1
	a.KeyID = keyID
	a.KeyGeneration = keyGeneration
	a.RunnerObservedSecretUID = secretUID
	a.RunnerObservedSecretVersion = secretVersion
	a.ExpectedInstancesDigest = expectedDigest
	a.ActiveEpochsDigest = epochsDigest
	if !edgeActivationReleaseFencePattern.MatchString(a.ReleaseFence) || !validSHA256Evidence(a.PhaseNonce) {
		return errInvalidEdgeActivationAuthorization
	}
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write([]byte(edgeActivationAuthorizationMaterial(*advance)))
	a.Signature = "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
	return nil
}

func SignEdgeRemediationAdvance(advance *model.EdgeRemediationAdvance, key, keyID, keyGeneration, secretUID, secretVersion string) error {
	if advance == nil || len(key) < 32 || strings.TrimSpace(keyID) == "" || strings.TrimSpace(keyGeneration) == "" || strings.TrimSpace(secretUID) == "" || strings.TrimSpace(secretVersion) == "" {
		return errInvalidEdgeActivationAuthorization
	}
	a := &advance.Authorization
	a.Schema = model.EdgeRemediationAuthorizationSchemaV1
	a.KeyID = keyID
	a.KeyGeneration = keyGeneration
	a.RunnerObservedSecretUID = secretUID
	a.RunnerObservedSecretVersion = secretVersion
	if !edgeActivationReleaseFencePattern.MatchString(a.ReleaseFence) || !validSHA256Evidence(a.ActionNonce) {
		return errInvalidEdgeActivationAuthorization
	}
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write([]byte(edgeRemediationAuthorizationMaterial(*advance)))
	a.Signature = "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
	return nil
}

func edgeActivationMaterialDigests(expected []model.EdgeExpectedInstance, epochs []model.EdgeActiveEpoch) (string, string, error) {
	expectedCopy := append([]model.EdgeExpectedInstance(nil), expected...)
	sort.Slice(expectedCopy, func(i, j int) bool {
		left, _ := json.Marshal(expectedCopy[i])
		right, _ := json.Marshal(expectedCopy[j])
		return string(left) < string(right)
	})
	type epochMaterial struct {
		EdgeGroupID         string `json:"edge_group_id"`
		Slot                string `json:"slot"`
		ReleaseEpoch        string `json:"release_epoch"`
		FenceSequence       uint64 `json:"fence_sequence"`
		MinHealthyInstances int    `json:"min_healthy_instances"`
	}
	epochCopy := make([]epochMaterial, 0, len(epochs))
	for _, epoch := range epochs {
		epochCopy = append(epochCopy, epochMaterial{EdgeGroupID: epoch.EdgeGroupID, Slot: epoch.Slot, ReleaseEpoch: epoch.ReleaseEpoch, FenceSequence: epoch.FenceSequence, MinHealthyInstances: epoch.MinHealthyInstances})
	}
	sort.Slice(epochCopy, func(i, j int) bool { return epochCopy[i].EdgeGroupID < epochCopy[j].EdgeGroupID })
	expectedJSON, err := json.Marshal(expectedCopy)
	if err != nil {
		return "", "", err
	}
	epochJSON, err := json.Marshal(epochCopy)
	if err != nil {
		return "", "", err
	}
	expectedHash := sha256.Sum256(expectedJSON)
	epochHash := sha256.Sum256(epochJSON)
	return "sha256:" + hex.EncodeToString(expectedHash[:]), "sha256:" + hex.EncodeToString(epochHash[:]), nil
}

func validSHA256Evidence(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}
