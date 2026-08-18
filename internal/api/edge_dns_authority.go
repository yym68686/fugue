package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	edgeDNSAuthorityStatusLimit = 64 << 10
	edgeDNSAuthorityMaxAge      = 90 * time.Second
)

var (
	edgeDNSAuthorityServicePattern = regexp.MustCompile(`^edge-control-[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	edgeDNSAuthorityGroupPattern   = regexp.MustCompile(`^edge-group-[a-z0-9]+(?:-[a-z0-9]+)*$`)
)

func parseEdgeAuthorityServices(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var configured map[string]string
	if err := json.Unmarshal([]byte(raw), &configured); err != nil {
		return nil, fmt.Errorf("must be a JSON object of edge group to service mappings: %w", err)
	}
	if len(configured) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(configured))
	for groupID, service := range configured {
		groupID = strings.TrimSpace(strings.ToLower(groupID))
		service = strings.TrimSpace(strings.ToLower(service))
		if !edgeDNSAuthorityGroupPattern.MatchString(groupID) || !edgeDNSAuthorityServicePattern.MatchString(service) {
			return nil, fmt.Errorf("invalid Edge Control authority mapping for group %q", groupID)
		}
		result[groupID] = service
	}
	return result, nil
}

type edgeDNSAuthorityStatus struct {
	EdgeGroupID            string    `json:"edge_group_id"`
	Status                 string    `json:"status"`
	Ready                  bool      `json:"ready"`
	InventorySequence      uint64    `json:"inventory_sequence"`
	InventoryProducerNodes int       `json:"inventory_producer_nodes"`
	InventoryHeartbeatAt   time.Time `json:"inventory_heartbeat_at"`
	PublicationSequence    uint64    `json:"publication_sequence"`
	PublicationDecision    string    `json:"publication_decision"`
	BundleGeneration       string    `json:"bundle_generation"`
	PublishedBundleDigest  string    `json:"published_bundle_digest"`
	BundleValidUntil       time.Time `json:"bundle_valid_until"`
	LKGState               string    `json:"lkg_state"`
}

func (s *Server) edgeDNSAuthorityReady(ctx context.Context, service, groupID string, now time.Time) (string, []string, error) {
	service = strings.TrimSpace(service)
	groupID = strings.TrimSpace(groupID)
	if !edgeDNSAuthorityServicePattern.MatchString(service) || !edgeDNSAuthorityGroupPattern.MatchString(groupID) {
		return "", nil, errors.New("edge DNS authority binding is invalid")
	}
	namespace := strings.TrimSpace(s.controlPlaneNamespace)
	if namespace == "" {
		return "", nil, errors.New("edge DNS authority namespace is unavailable")
	}
	endpoint := "http://" + service + "." + namespace + ".svc:8092/v1/authority/groups/" + url.PathEscape(groupID) + "/readyz"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", nil, errors.New("build edge DNS authority request")
	}
	client := s.edgeDNSAuthorityHTTPClient
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return "", nil, errors.New("read edge DNS authority status")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, edgeDNSAuthorityStatusLimit+1))
	if err != nil || len(body) == 0 || len(body) > edgeDNSAuthorityStatusLimit || response.StatusCode != http.StatusOK {
		return "", nil, errors.New("edge DNS authority status is unavailable")
	}
	var status edgeDNSAuthorityStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return "", nil, errors.New("edge DNS authority status is invalid")
	}
	if err := validateEdgeDNSAuthorityStatus(status, groupID, now); err != nil {
		return "", nil, err
	}
	nodeIDs := make([]string, status.InventoryProducerNodes)
	for index := range nodeIDs {
		nodeIDs[index] = fmt.Sprintf("authority:%s:%d", groupID, index+1)
	}
	return groupID, nodeIDs, nil
}

func validateEdgeDNSAuthorityStatus(status edgeDNSAuthorityStatus, groupID string, now time.Time) error {
	if status.EdgeGroupID != groupID || status.Status != "ready" || !status.Ready ||
		status.InventorySequence == 0 || status.InventoryProducerNodes <= 0 || status.InventoryProducerNodes > 1024 ||
		status.PublicationSequence == 0 || status.PublicationDecision != "published" ||
		strings.TrimSpace(status.BundleGeneration) == "" || !strings.HasPrefix(strings.TrimSpace(status.PublishedBundleDigest), "sha256:") ||
		status.LKGState != "current" || status.InventoryHeartbeatAt.IsZero() || status.BundleValidUntil.IsZero() {
		return errors.New("edge DNS authority status is not ready")
	}
	if now.IsZero() || status.InventoryHeartbeatAt.After(now.Add(30*time.Second)) || now.Sub(status.InventoryHeartbeatAt) > edgeDNSAuthorityMaxAge || !status.BundleValidUntil.After(now) {
		return errors.New("edge DNS authority status is stale")
	}
	return nil
}
