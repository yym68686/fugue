package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/clientmeasurement"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"fugue/internal/store"
)

func (s *Server) clientProbeKeys() bundleauth.Keyring {
	return bundleauth.NewKeyring(s.bundleSigningKey, s.bundleSigningKeyID, s.bundleSigningPreviousKey, s.bundleSigningPreviousKeyID, s.bundleRevokedKeyIDs)
}

func clientProbeObserver(principal model.Principal) string {
	return clientmeasurement.Digest([]byte(principal.ActorType + "\x00" + principal.ActorID))
}

func (s *Server) handleIssueEdgeClientProbePlan(writer http.ResponseWriter, request *http.Request) {
	principal := mustPrincipal(request)
	if !principal.IsPlatformAdmin() || !principal.HasExplicitScope("edge.quality.observe") {
		httpx.WriteError(writer, http.StatusForbidden, "explicit edge.quality.observe authority required")
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, 4096)
	var input model.EdgeClientProbeRequest
	if httpx.DecodeJSON(request, &input) != nil || normalizeExternalAppDomain(input.Hostname) != input.Hostname || input.Hostname == "" ||
		!strings.HasPrefix(input.Path, "/") || len(input.Path) > 2048 || strings.ContainsAny(input.Path, "?#\r\n") ||
		(input.TrafficClass != "streaming" && input.TrafficClass != "dynamic_api") || input.ObserverLabel == "" || len(input.ObserverLabel) > 128 || strings.ContainsAny(input.ObserverLabel, "\r\n\x00") {
		httpx.WriteError(writer, http.StatusBadRequest, "explicit hostname, path, traffic class and observer label required")
		return
	}
	observer := clientProbeObserver(principal)
	if !s.reserveClientProbePlan(observer, time.Now()) {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "client probe plan budget exhausted")
		return
	}
	nodes, _, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		s.writeStoreError(writer, err)
		return
	}
	policy, err := s.store.GetEdgeRoutePolicy(input.Hostname)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.writeStoreError(writer, err)
		return
	}
	now := time.Now().UTC()
	quarantine := s.activeNodeQuarantineByName()
	eligible := []model.EdgeNode{}
	for _, node := range nodes {
		candidate := edgeQualityRankCandidateForNode(node, policy, now, quarantine)
		if !edgeQualityRankCandidateHardGated(candidate) && node.PublicIPv4 != "" {
			eligible = append(eligible, node)
		}
	}
	if len(eligible) < 2 || len(eligible) > 8 {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "two to eight eligible physical edges required")
		return
	}
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "probe nonce unavailable")
		return
	}
	encodedSeed := hex.EncodeToString(seed)
	payloadDigest := clientmeasurement.Digest(clientmeasurement.Payload(encodedSeed))
	plan := model.EdgeClientProbePlan{Schema: "fugue.client-probe-plan/v1", RoundID: model.NewID("client_probe"), ObserverLabel: input.ObserverLabel, Permits: []model.EdgeClientProbePermit{}}
	ctx, cancel := context.WithTimeout(request.Context(), 12*time.Second)
	defer cancel()
	ready, proofs := clientProbeReadyCandidates(ctx, input, eligible, func(ctx context.Context, node model.EdgeNode) (routeprobe.Proof, error) {
		proof, err := routeprobe.Probe(ctx, input.Hostname, input.Path, node.PublicIPv4, "", 2*time.Second)
		if (err != nil || proof.EdgeID != node.ID || proof.GroupID != node.EdgeGroupID || proof.State != "") && s.log != nil {
			s.log.Printf("client probe plan candidate unavailable; edge_id=%s hostname=%s proof_edge=%s proof_group=%s proof_state=%s error=%v", node.ID, input.Hostname, proof.EdgeID, proof.GroupID, proof.State, err)
		}
		return proof, err
	})
	if ctx.Err() != nil || len(ready) < 2 {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "fewer than two physical edges prove the current serving route")
		return
	}
	eligible = ready
	now = time.Now().UTC()
	targets := make([]string, 0, len(eligible))
	for _, node := range eligible {
		targets = append(targets, node.ID)
	}
	slices.Sort(targets)
	for index, node := range eligible {
		proof := proofs[index]
		permit := model.EdgeClientProbePermit{Schema: "fugue.client-probe-permit/v1", RoundID: plan.RoundID, AttemptID: model.NewID("probe_attempt"), ObserverID: observer,
			Hostname: input.Hostname, Path: input.Path, TrafficClass: input.TrafficClass, EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID, Address: node.PublicIPv4,
			RouteDigest: proof.Digest, BundleVersion: proof.Version, IssuedAt: now, ExpiresAt: now.Add(2 * time.Minute), BodyBytes: clientmeasurement.BodyBytes, BodySeed: encodedSeed, BodySHA256: payloadDigest, TargetEdgeIDs: targets}
		permit, err = clientmeasurement.SignPermit(permit, s.clientProbeKeys())
		if err != nil || clientmeasurement.ValidatePermit(permit) != nil {
			httpx.WriteError(writer, http.StatusServiceUnavailable, "probe permit signing unavailable")
			return
		}
		plan.Permits = append(plan.Permits, permit)
	}
	s.captureClientProbeCapacityWitnesses(ctx, input, eligible, proofs)
	writer.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(writer, http.StatusOK, plan)
}

func clientProbeReadyCandidates(ctx context.Context, input model.EdgeClientProbeRequest, nodes []model.EdgeNode, probe func(context.Context, model.EdgeNode) (routeprobe.Proof, error)) ([]model.EdgeNode, []routeprobe.Proof) {
	ready, proofs := []model.EdgeNode{}, []routeprobe.Proof{}
	for _, node := range nodes {
		if ctx.Err() != nil {
			break
		}
		proof, err := probe(ctx, node)
		if err != nil || proof.EdgeID != node.ID || proof.GroupID != node.EdgeGroupID || proof.State != "" {
			continue
		}
		ready, proofs = append(ready, node), append(proofs, proof)
	}
	return ready, proofs
}

func (s *Server) reserveClientProbePlan(observer string, now time.Time) bool {
	if !s.clientProbePlanMu.TryLock() {
		return false
	}
	defer s.clientProbePlanMu.Unlock()
	if s.clientProbePlans == nil {
		s.clientProbePlans = map[string]time.Time{}
	}
	for identity, at := range s.clientProbePlans {
		if now.Sub(at) >= 5*time.Minute {
			delete(s.clientProbePlans, identity)
		}
	}
	if len(s.clientProbePlans) >= 128 || now.Sub(s.clientProbePlans[observer]) < 30*time.Second {
		return false
	}
	s.clientProbePlans[observer] = now
	return true
}

func (s *Server) handleReportEdgeClientProbeRound(writer http.ResponseWriter, request *http.Request) {
	principal := mustPrincipal(request)
	if !principal.IsPlatformAdmin() || !principal.HasExplicitScope("edge.quality.observe") {
		httpx.WriteError(writer, http.StatusForbidden, "explicit edge.quality.observe authority required")
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, 128<<10)
	var report model.EdgeClientProbeReport
	if httpx.DecodeJSON(request, &report) != nil {
		httpx.WriteError(writer, http.StatusBadRequest, "invalid bounded client report")
		return
	}
	if _, err := clientmeasurement.ValidateReport(report); err != nil {
		httpx.WriteError(writer, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now().UTC()
	keys := s.clientProbeKeys()
	for _, permit := range report.Plan.Permits {
		if permit.ObserverID != clientProbeObserver(principal) || clientmeasurement.VerifyPermit(permit, keys, now) != nil {
			httpx.WriteError(writer, http.StatusForbidden, "probe plan expired or issued to another observer")
			return
		}
		index := slices.IndexFunc(report.Outcomes, func(outcome model.EdgeClientProbeOutcome) bool { return outcome.AttemptID == permit.AttemptID })
		outcome := report.Outcomes[index]
		if outcome.CompletedAt.After(now.Add(clientmeasurement.ClientClockSkew)) || outcome.Attestation != nil && clientmeasurement.VerifyAttestation(*outcome.Attestation, permit, keys) != nil {
			httpx.WriteError(writer, http.StatusBadRequest, "unverified executor or future client outcome")
			return
		}
	}
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.RecordEdgeClientProbeReport(ctx, report); err != nil {
		s.writeStoreError(writer, err)
		return
	}
	raw, _ := json.Marshal(report)
	httpx.WriteJSON(writer, http.StatusOK, map[string]any{"accepted": true, "routing_authorized": false, "report_digest": clientmeasurement.Digest(raw)})
}
