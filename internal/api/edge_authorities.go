package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
)

const edgeAuthorityReadStatusLimit = 128 << 10

// edgeAuthorityReadStatus mirrors the Edge Control authority projection. It
// intentionally does not expose synthetic node IDs: only Edge Control can
// attest the inventory and publication state used by serving.
type edgeAuthorityReadStatus struct {
	EdgeGroupID                 string     `json:"edge_group_id"`
	Status                      string     `json:"status"`
	Ready                       bool       `json:"ready"`
	ServingHealthy              bool       `json:"serving_healthy"`
	BootstrapEligible           bool       `json:"bootstrap_eligible"`
	BootstrapValidUntil         *time.Time `json:"bootstrap_valid_until,omitempty"`
	InventorySequence           uint64     `json:"inventory_sequence,omitempty"`
	InventoryGeneration         string     `json:"inventory_generation,omitempty"`
	InventoryProducerGeneration uint64     `json:"inventory_producer_generation,omitempty"`
	InventoryProducerNodes      int        `json:"inventory_producer_nodes,omitempty"`
	InventoryHeartbeatAt        *time.Time `json:"inventory_heartbeat_at,omitempty"`
	AuthoritySequence           uint64     `json:"authority_sequence,omitempty"`
	PublicationSequence         uint64     `json:"publication_sequence,omitempty"`
	CurrentPublicationSequence  uint64     `json:"current_publication_sequence,omitempty"`
	CandidateEpoch              uint64     `json:"candidate_epoch,omitempty"`
	PublicationDecision         string     `json:"publication_decision,omitempty"`
	BundleGeneration            string     `json:"bundle_generation,omitempty"`
	PublishedBundleDigest       string     `json:"published_bundle_digest,omitempty"`
	RecoveryEpoch               uint64     `json:"recovery_epoch"`
	BundleValidUntil            *time.Time `json:"bundle_valid_until,omitempty"`
	LKGState                    string     `json:"lkg_state"`
	FailureCode                 string     `json:"failure_code,omitempty"`
	RuntimeFailureCode          string     `json:"runtime_failure_code,omitempty"`
}

type edgeAuthorityReadRecord struct {
	EdgeGroupID string                   `json:"edge_group_id"`
	Service     string                   `json:"service"`
	Ready       bool                     `json:"ready"`
	Source      string                   `json:"source"`
	Status      *edgeAuthorityReadStatus `json:"status,omitempty"`
	Error       string                   `json:"error,omitempty"`
}

type edgeAuthorityReadResponse struct {
	Configured  bool                      `json:"configured"`
	AnswerModel string                    `json:"answer_model"`
	AllReady    bool                      `json:"all_ready"`
	Authorities []edgeAuthorityReadRecord `json:"authorities"`
	Error       string                    `json:"error,omitempty"`
}

func (s *Server) handleAdminListEdgeAuthorities(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect Edge Control authority")
		return
	}

	response := edgeAuthorityReadResponse{
		Configured:  len(s.edgeAuthorityServices) > 0,
		AnswerModel: "edge-control-authority",
		AllReady:    false,
		Authorities: make([]edgeAuthorityReadRecord, 0, len(s.edgeAuthorityServices)),
	}
	if len(s.edgeAuthorityServices) == 0 {
		response.Error = "Edge Control authority services are not configured"
		httpx.WriteJSON(w, http.StatusOK, response)
		return
	}

	requestedGroup := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("edge_group_id")))
	groups := make([]string, 0, len(s.edgeAuthorityServices))
	for groupID := range s.edgeAuthorityServices {
		if requestedGroup == "" || requestedGroup == groupID {
			groups = append(groups, groupID)
		}
	}
	sort.Strings(groups)
	if requestedGroup != "" && len(groups) == 0 {
		httpx.WriteError(w, http.StatusNotFound, "Edge Control authority group is not configured")
		return
	}

	response.AllReady = true
	for _, groupID := range groups {
		service := s.edgeAuthorityServices[groupID]
		record := s.readEdgeAuthorityRecord(r.Context(), service, groupID, time.Now().UTC())
		if !record.Ready {
			response.AllReady = false
		}
		response.Authorities = append(response.Authorities, record)
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) readEdgeAuthorityRecord(ctx context.Context, service, groupID string, now time.Time) edgeAuthorityReadRecord {
	record := edgeAuthorityReadRecord{EdgeGroupID: groupID, Service: service, Source: "edge-control-authority"}
	status, httpStatus, err := s.readEdgeAuthorityStatus(ctx, service, groupID)
	if err != nil {
		record.Error = err.Error()
		return record
	}
	if status.EdgeGroupID != groupID {
		record.Error = "Edge Control authority returned a mismatched edge group"
		return record
	}
	record.Status = &status
	record.Ready = status.Ready && httpStatus == http.StatusOK
	if !record.Ready {
		record.Error = fmt.Sprintf("Edge Control authority is not ready (http_status=%d)", httpStatus)
	}
	if record.Ready {
		if err := validateEdgeAuthorityReadStatus(status, now); err != nil {
			record.Ready = false
			record.Error = err.Error()
		}
	}
	return record
}

func (s *Server) readEdgeAuthorityStatus(ctx context.Context, service, groupID string) (edgeAuthorityReadStatus, int, error) {
	if !edgeDNSAuthorityServicePattern.MatchString(service) || !edgeDNSAuthorityGroupPattern.MatchString(groupID) {
		return edgeAuthorityReadStatus{}, 0, errors.New("Edge Control authority mapping is invalid")
	}
	if strings.TrimSpace(s.controlPlaneNamespace) == "" {
		return edgeAuthorityReadStatus{}, 0, errors.New("Edge Control authority namespace is unavailable")
	}
	endpoint := "http://" + service + "." + s.controlPlaneNamespace + ".svc:8092/v1/authority/groups/" + url.PathEscape(groupID) + "/readyz"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return edgeAuthorityReadStatus{}, 0, errors.New("build Edge Control authority request")
	}
	request.Header.Set("Accept", "application/json")
	client := s.edgeDNSAuthorityHTTPClient
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return edgeAuthorityReadStatus{}, 0, errors.New("read Edge Control authority status")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, edgeAuthorityReadStatusLimit+1))
	if err != nil || len(body) == 0 || len(body) > edgeAuthorityReadStatusLimit {
		return edgeAuthorityReadStatus{}, response.StatusCode, errors.New("Edge Control authority status is unavailable")
	}
	var status edgeAuthorityReadStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return edgeAuthorityReadStatus{}, response.StatusCode, errors.New("Edge Control authority status is invalid")
	}
	return status, response.StatusCode, nil
}

func validateEdgeAuthorityReadStatus(status edgeAuthorityReadStatus, now time.Time) error {
	if status.InventorySequence == 0 || status.InventoryProducerNodes <= 0 || status.PublicationSequence == 0 ||
		strings.TrimSpace(status.BundleGeneration) == "" || !strings.HasPrefix(strings.TrimSpace(status.PublishedBundleDigest), "sha256:") ||
		status.InventoryHeartbeatAt == nil || status.BundleValidUntil == nil || status.LKGState == "" {
		return errors.New("Edge Control authority status is missing inventory or publication evidence")
	}
	if now.IsZero() || status.InventoryHeartbeatAt.After(now.Add(30*time.Second)) || now.Sub(*status.InventoryHeartbeatAt) > edgeDNSAuthorityMaxAge || !status.BundleValidUntil.After(now) {
		return errors.New("Edge Control authority status is stale")
	}
	return nil
}
