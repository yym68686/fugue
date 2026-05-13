package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"

	miekgdns "github.com/miekg/dns"
)

const (
	defaultDNSDelegationMinHealthyNodes = 2
	defaultDNSDelegationPlanTTL         = 300
	defaultDNSPreflightTimeout          = 3 * time.Second
)

type dnsHeartbeatRequest struct {
	DNSNodeID        string            `json:"dns_node_id"`
	EdgeGroupID      string            `json:"edge_group_id"`
	PublicHostname   string            `json:"public_hostname,omitempty"`
	PublicIPv4       string            `json:"public_ipv4,omitempty"`
	PublicIPv6       string            `json:"public_ipv6,omitempty"`
	MeshIP           string            `json:"mesh_ip,omitempty"`
	Zone             string            `json:"zone"`
	DNSBundleVersion string            `json:"dns_bundle_version,omitempty"`
	RecordCount      int               `json:"record_count"`
	CacheStatus      string            `json:"cache_status,omitempty"`
	CacheWriteErrors uint64            `json:"cache_write_errors,omitempty"`
	CacheLoadErrors  uint64            `json:"cache_load_errors,omitempty"`
	BundleSyncErrors uint64            `json:"bundle_sync_errors,omitempty"`
	QueryCount       uint64            `json:"query_count,omitempty"`
	QueryErrorCount  uint64            `json:"query_error_count,omitempty"`
	QueryRCodeCounts map[string]uint64 `json:"query_rcode_counts,omitempty"`
	QueryQTypeCounts map[string]uint64 `json:"query_qtype_counts,omitempty"`
	ListenAddr       string            `json:"listen_addr,omitempty"`
	UDPAddr          string            `json:"udp_addr,omitempty"`
	TCPAddr          string            `json:"tcp_addr,omitempty"`
	UDPListen        bool              `json:"udp_listen,omitempty"`
	TCPListen        bool              `json:"tcp_listen,omitempty"`
	Status           string            `json:"status"`
	Healthy          bool              `json:"healthy"`
	LastError        string            `json:"last_error,omitempty"`
}

type dnsDelegationProbeFunc func(context.Context, model.DNSNode, string, string) dnsDelegationProbeResult

type dnsParentNSLookupFunc func(context.Context, string) ([]string, error)

type dnsDelegationProbeResult struct {
	UDP53Reachable bool
	TCP53Reachable bool
	ProbeAnswers   []string
	LastError      string
}

func (s *Server) handleListDNSNodes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect DNS nodes")
		return
	}
	nodes, err := s.store.ListDNSNodes(r.URL.Query().Get("edge_group_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
}

func (s *Server) handleGetDNSNode(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect DNS nodes")
		return
	}
	node, err := s.store.GetDNSNode(r.PathValue("dns_node_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node": node})
}

func (s *Server) handleDNSHeartbeat(w http.ResponseWriter, r *http.Request) {
	authContext, ok := s.authorizeEdgeRequest(w, r)
	if !ok {
		return
	}
	var req dnsHeartbeatRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := authContext.constrain(&req.DNSNodeID, &req.EdgeGroupID); err != nil {
		httpx.WriteError(w, http.StatusForbidden, err.Error())
		return
	}
	status := model.NormalizeEdgeHealthStatus(req.Status)
	if status == "" {
		httpx.WriteError(w, http.StatusBadRequest, "status must be unknown, healthy, degraded, or unhealthy")
		return
	}
	req = s.enrichDNSHeartbeatFromClusterNode(r.Context(), req)
	node, err := s.store.UpdateDNSHeartbeat(model.DNSNode{
		ID:               req.DNSNodeID,
		EdgeGroupID:      req.EdgeGroupID,
		PublicHostname:   req.PublicHostname,
		PublicIPv4:       req.PublicIPv4,
		PublicIPv6:       req.PublicIPv6,
		MeshIP:           req.MeshIP,
		Zone:             req.Zone,
		Status:           status,
		Healthy:          req.Healthy,
		DNSBundleVersion: req.DNSBundleVersion,
		RecordCount:      req.RecordCount,
		CacheStatus:      req.CacheStatus,
		CacheWriteErrors: req.CacheWriteErrors,
		CacheLoadErrors:  req.CacheLoadErrors,
		BundleSyncErrors: req.BundleSyncErrors,
		QueryCount:       req.QueryCount,
		QueryErrorCount:  req.QueryErrorCount,
		QueryRCodeCounts: req.QueryRCodeCounts,
		QueryQTypeCounts: req.QueryQTypeCounts,
		ListenAddr:       req.ListenAddr,
		UDPAddr:          req.UDPAddr,
		TCPAddr:          req.TCPAddr,
		UDPListen:        req.UDPListen,
		TCPListen:        req.TCPListen,
		LastError:        req.LastError,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"node":     node,
		"accepted": true,
	})
}

func (s *Server) enrichDNSHeartbeatFromClusterNode(ctx context.Context, req dnsHeartbeatRequest) dnsHeartbeatRequest {
	endpoint := s.discoverClusterNodeEndpoint(ctx, req.DNSNodeID)
	if strings.TrimSpace(req.PublicIPv4) == "" {
		req.PublicIPv4 = endpoint.PublicIPv4
	}
	if strings.TrimSpace(req.PublicIPv6) == "" {
		req.PublicIPv6 = endpoint.PublicIPv6
	}
	if strings.TrimSpace(req.MeshIP) == "" {
		req.MeshIP = endpoint.MeshIP
	}
	return req
}

func (s *Server) handleDNSDelegationPreflight(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can run DNS delegation preflight")
		return
	}
	response := s.buildDNSDelegationPreflight(r.Context(), principal, s.dnsDelegationPreflightOptionsFromRequest(r))
	httpx.WriteJSON(w, http.StatusOK, response)
}

type dnsDelegationPreflightOptions struct {
	Zone            string
	ProbeName       string
	EdgeGroupID     string
	MinHealthyNodes int
}

func (s *Server) dnsDelegationPreflightOptionsFromRequest(r *http.Request) dnsDelegationPreflightOptions {
	query := r.URL.Query()
	zone := normalizeExternalAppDomain(query.Get("zone"))
	if zone == "" {
		zone = normalizeExternalAppDomain(s.appBaseDomain)
	}
	if zone == "" {
		zone = "fugue.pro"
	}
	probeName := normalizeExternalAppDomain(query.Get("probe_name"))
	if probeName == "" {
		probeName = normalizeExternalAppDomain(defaultEdgeDNSProbeLabel + "." + zone)
	}
	minHealthy := defaultDNSDelegationMinHealthyNodes
	if raw := strings.TrimSpace(query.Get("min_healthy_nodes")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			minHealthy = parsed
		}
	}
	return dnsDelegationPreflightOptions{
		Zone:            zone,
		ProbeName:       probeName,
		EdgeGroupID:     strings.TrimSpace(query.Get("edge_group_id")),
		MinHealthyNodes: minHealthy,
	}
}

func (s *Server) buildDNSDelegationPreflight(ctx context.Context, principal model.Principal, opts dnsDelegationPreflightOptions) model.DNSDelegationPreflightResponse {
	nodes, err := s.store.ListDNSNodes(opts.EdgeGroupID)
	checks := []model.DNSDelegationPreflightCheck{}
	if err != nil {
		return model.DNSDelegationPreflightResponse{
			Pass:            false,
			Zone:            opts.Zone,
			ProbeName:       opts.ProbeName,
			MinHealthyNodes: opts.MinHealthyNodes,
			GeneratedAt:     time.Now().UTC(),
			Checks: []model.DNSDelegationPreflightCheck{{
				Name:    "dns_inventory",
				Pass:    false,
				Message: fmt.Sprintf("cannot load DNS node inventory: %v", err),
			}},
		}
	}
	nodes = dnsNodesForZone(nodes, opts.Zone)

	policyByNode, policyErr := s.dnsNodePolicyStatusByName(ctx, principal)
	nodeChecks := make([]model.DNSDelegationNodeCheck, 0, len(nodes))
	for _, node := range nodes {
		nodeChecks = append(nodeChecks, s.buildDNSDelegationNodeCheck(ctx, node, opts, policyByNode, policyErr))
	}

	currentParentNS, lookupErr := s.lookupDNSParentNS(ctx, opts.Zone)
	if lookupErr != nil {
		checks = append(checks, model.DNSDelegationPreflightCheck{
			Name:    "current_parent_ns",
			Pass:    true,
			Message: fmt.Sprintf("current parent NS lookup is informational and failed for %s: %v", opts.Zone, lookupErr),
		})
	} else {
		checks = append(checks, model.DNSDelegationPreflightCheck{
			Name:    "current_parent_ns",
			Pass:    true,
			Message: strings.Join(currentParentNS, ", "),
		})
	}

	healthyNodeCount := countPassingDNSNodeChecks(nodeChecks)
	bundleVersion, bundleStable := stableDNSBundleVersion(nodeChecks)
	checks = append(checks,
		model.DNSDelegationPreflightCheck{
			Name:    "dns_node_count",
			Pass:    len(nodes) >= opts.MinHealthyNodes,
			Message: fmt.Sprintf("%d DNS nodes registered for %s, need at least %d", len(nodes), opts.Zone, opts.MinHealthyNodes),
		},
		model.DNSDelegationPreflightCheck{
			Name:    "healthy_dns_nodes",
			Pass:    healthyNodeCount >= opts.MinHealthyNodes,
			Message: fmt.Sprintf("%d DNS nodes passed preflight, need at least %d", healthyNodeCount, opts.MinHealthyNodes),
		},
		model.DNSDelegationPreflightCheck{
			Name:    "dns_bundle_version_stable",
			Pass:    bundleStable,
			Message: dnsBundleStableMessage(bundleVersion, bundleStable),
		},
		model.DNSDelegationPreflightCheck{
			Name:    "cache_errors_zero",
			Pass:    dnsNodeCacheErrorsZero(nodeChecks),
			Message: "cache write/load errors must be zero on every DNS node",
		},
		model.DNSDelegationPreflightCheck{
			Name:    "kubernetes_health_gate",
			Pass:    dnsNodeKubernetesHealthOK(nodeChecks) && policyErr == nil,
			Message: dnsKubernetesHealthMessage(policyErr),
		},
		model.DNSDelegationPreflightCheck{
			Name:    "dns_reachability_and_probe",
			Pass:    dnsNodeReachabilityOK(nodeChecks),
			Message: fmt.Sprintf("each node must answer %s A on UDP/TCP 53 with its public IPv4", opts.ProbeName),
		},
	)

	pass := true
	for _, check := range checks {
		if !check.Pass {
			pass = false
			break
		}
	}
	return model.DNSDelegationPreflightResponse{
		Pass:             pass,
		Zone:             opts.Zone,
		ProbeName:        opts.ProbeName,
		MinHealthyNodes:  opts.MinHealthyNodes,
		HealthyNodeCount: healthyNodeCount,
		DNSBundleVersion: bundleVersion,
		GeneratedAt:      time.Now().UTC(),
		Checks:           checks,
		Nodes:            nodeChecks,
		DelegationPlan:   buildDNSDelegationPlan(opts.Zone, nodeChecks, currentParentNS, dnsDelegationPlanHints(opts.Zone, s.dnsStaticRecords)),
	}
}

func (s *Server) dnsNodePolicyStatusByName(ctx context.Context, principal model.Principal) (map[string]model.ClusterNodePolicyStatus, error) {
	statuses, err := s.loadClusterNodePolicyStatuses(ctx, principal)
	if err != nil {
		return nil, err
	}
	out := make(map[string]model.ClusterNodePolicyStatus, len(statuses))
	for _, status := range statuses {
		out[strings.TrimSpace(strings.ToLower(status.NodeName))] = status
	}
	return out, nil
}

func (s *Server) buildDNSDelegationNodeCheck(ctx context.Context, node model.DNSNode, opts dnsDelegationPreflightOptions, policyByNode map[string]model.ClusterNodePolicyStatus, policyErr error) model.DNSDelegationNodeCheck {
	publicIP := dnsNodePublicIPv4(node)
	probe := s.probeDNSDelegationNode(ctx, node, opts.Zone, opts.ProbeName)
	expectedAnswer := publicIP
	probePass := probe.UDP53Reachable && probe.TCP53Reachable && expectedAnswer != "" && stringSliceContains(probe.ProbeAnswers, expectedAnswer)
	policy, known := policyByNode[strings.TrimSpace(strings.ToLower(node.ID))]
	nodeReady := known && policy.Ready
	nodeDiskPressure := known && policy.DiskPressure
	cacheOK := node.CacheWriteErrors == 0 && node.CacheLoadErrors == 0
	bundleOK := strings.TrimSpace(node.DNSBundleVersion) != ""
	healthOK := node.Healthy && node.Status == model.EdgeHealthHealthy
	kubeOK := policyErr == nil && known && nodeReady && !nodeDiskPressure
	pass := healthOK && bundleOK && cacheOK && kubeOK && probePass

	messageParts := []string{}
	if !healthOK {
		messageParts = append(messageParts, "DNS heartbeat is not healthy")
	}
	if !bundleOK {
		messageParts = append(messageParts, "DNS bundle version is empty")
	}
	if !cacheOK {
		messageParts = append(messageParts, "cache write/load errors are non-zero")
	}
	if policyErr != nil {
		messageParts = append(messageParts, "Kubernetes node health is unavailable")
	} else if !known {
		messageParts = append(messageParts, "Kubernetes node policy status is missing")
	} else if !nodeReady {
		messageParts = append(messageParts, "Kubernetes node Ready is not true")
	} else if nodeDiskPressure {
		messageParts = append(messageParts, "Kubernetes node has DiskPressure")
	}
	if !probePass {
		if probe.LastError != "" {
			messageParts = append(messageParts, probe.LastError)
		} else {
			messageParts = append(messageParts, "DNS probe did not return expected public IPv4 on UDP/TCP 53")
		}
	}

	return model.DNSDelegationNodeCheck{
		DNSNodeID:           node.ID,
		EdgeGroupID:         node.EdgeGroupID,
		PublicIP:            publicIP,
		Zone:                node.Zone,
		Status:              node.Status,
		Healthy:             node.Healthy,
		DNSBundleVersion:    node.DNSBundleVersion,
		RecordCount:         node.RecordCount,
		CacheStatus:         node.CacheStatus,
		CacheWriteErrors:    node.CacheWriteErrors,
		CacheLoadErrors:     node.CacheLoadErrors,
		BundleSyncErrors:    node.BundleSyncErrors,
		QueryCount:          node.QueryCount,
		QueryErrorCount:     node.QueryErrorCount,
		UDP53Reachable:      probe.UDP53Reachable,
		TCP53Reachable:      probe.TCP53Reachable,
		ProbePass:           probePass,
		ProbeAnswers:        probe.ProbeAnswers,
		KubernetesNodeKnown: known,
		NodeReady:           nodeReady,
		NodeDiskPressure:    nodeDiskPressure,
		LastSeenAt:          node.LastSeenAt,
		Pass:                pass,
		Message:             strings.Join(messageParts, "; "),
	}
}

func (s *Server) probeDNSDelegationNode(ctx context.Context, node model.DNSNode, zone, probeName string) dnsDelegationProbeResult {
	probe := s.dnsDelegationProbe
	if probe == nil {
		probe = defaultDNSDelegationProbe
	}
	return probe(ctx, node, zone, probeName)
}

func (s *Server) lookupDNSParentNS(ctx context.Context, zone string) ([]string, error) {
	lookup := s.dnsParentNSLookup
	if lookup == nil {
		lookup = defaultDNSParentNSLookup
	}
	return lookup(ctx, zone)
}

func defaultDNSDelegationProbe(ctx context.Context, node model.DNSNode, _ string, probeName string) dnsDelegationProbeResult {
	ip := dnsNodePublicIPv4(node)
	if ip == "" {
		return dnsDelegationProbeResult{LastError: "DNS node has no public IPv4 for A-record probe"}
	}
	address := net.JoinHostPort(ip, "53")
	udpAnswers, udpErr := dnsQueryA(ctx, "udp", address, probeName)
	tcpAnswers, tcpErr := dnsQueryA(ctx, "tcp", address, probeName)
	answers := uniqueSortedStrings(append(udpAnswers, tcpAnswers...))
	errs := []string{}
	if udpErr != nil {
		errs = append(errs, "udp: "+udpErr.Error())
	}
	if tcpErr != nil {
		errs = append(errs, "tcp: "+tcpErr.Error())
	}
	return dnsDelegationProbeResult{
		UDP53Reachable: udpErr == nil,
		TCP53Reachable: tcpErr == nil,
		ProbeAnswers:   answers,
		LastError:      strings.Join(errs, "; "),
	}
}

func dnsQueryA(ctx context.Context, network, address, name string) ([]string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, defaultDNSPreflightTimeout)
	defer cancel()
	msg := new(miekgdns.Msg)
	msg.SetQuestion(miekgdns.Fqdn(normalizeExternalAppDomain(name)), miekgdns.TypeA)
	client := &miekgdns.Client{Net: network, Timeout: defaultDNSPreflightTimeout}
	resp, _, err := client.ExchangeContext(queryCtx, msg, address)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("empty DNS response")
	}
	if resp.Rcode != miekgdns.RcodeSuccess {
		return nil, fmt.Errorf("rcode=%s", miekgdns.RcodeToString[resp.Rcode])
	}
	answers := []string{}
	for _, answer := range resp.Answer {
		if a, ok := answer.(*miekgdns.A); ok && a.A != nil {
			answers = append(answers, a.A.String())
		}
	}
	if len(answers) == 0 {
		return nil, fmt.Errorf("no A answers")
	}
	return uniqueSortedStrings(answers), nil
}

func defaultDNSParentNSLookup(ctx context.Context, zone string) ([]string, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, defaultDNSPreflightTimeout)
	defer cancel()
	records, err := net.DefaultResolver.LookupNS(lookupCtx, normalizeExternalAppDomain(zone))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(records))
	for _, record := range records {
		if record != nil {
			out = append(out, normalizeExternalAppDomain(record.Host))
		}
	}
	sort.Strings(out)
	return out, nil
}

func dnsNodesForZone(nodes []model.DNSNode, zone string) []model.DNSNode {
	zone = normalizeExternalAppDomain(zone)
	out := make([]model.DNSNode, 0, len(nodes))
	for _, node := range nodes {
		if normalizeExternalAppDomain(node.Zone) == zone {
			out = append(out, node)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EdgeGroupID != out[j].EdgeGroupID {
			return out[i].EdgeGroupID < out[j].EdgeGroupID
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func countPassingDNSNodeChecks(checks []model.DNSDelegationNodeCheck) int {
	count := 0
	for _, check := range checks {
		if check.Pass {
			count++
		}
	}
	return count
}

func stableDNSBundleVersion(checks []model.DNSDelegationNodeCheck) (string, bool) {
	if len(checks) == 0 {
		return "", false
	}
	versionsByGroup := map[string]string{}
	groups := []string{}
	for _, check := range checks {
		next := strings.TrimSpace(check.DNSBundleVersion)
		if next == "" {
			return "", false
		}
		group := strings.TrimSpace(check.EdgeGroupID)
		if group == "" {
			group = "default"
		}
		version, ok := versionsByGroup[group]
		if !ok {
			versionsByGroup[group] = next
			groups = append(groups, group)
			continue
		}
		if version != next {
			return "", false
		}
	}
	sort.Strings(groups)
	if len(groups) == 1 {
		return versionsByGroup[groups[0]], true
	}
	parts := make([]string, 0, len(groups))
	for _, group := range groups {
		parts = append(parts, fmt.Sprintf("%s=%s", group, versionsByGroup[group]))
	}
	return strings.Join(parts, ", "), true
}

func dnsBundleStableMessage(version string, stable bool) string {
	if stable {
		return "DNS bundle version is consistent per edge group: " + version
	}
	return "each DNS node must report a non-empty DNS bundle version, and nodes in the same edge group must agree"
}

func dnsNodeCacheErrorsZero(checks []model.DNSDelegationNodeCheck) bool {
	if len(checks) == 0 {
		return false
	}
	for _, check := range checks {
		if check.CacheWriteErrors != 0 || check.CacheLoadErrors != 0 {
			return false
		}
	}
	return true
}

func dnsNodeKubernetesHealthOK(checks []model.DNSDelegationNodeCheck) bool {
	if len(checks) == 0 {
		return false
	}
	for _, check := range checks {
		if !check.KubernetesNodeKnown || !check.NodeReady || check.NodeDiskPressure {
			return false
		}
	}
	return true
}

func dnsKubernetesHealthMessage(err error) string {
	if err != nil {
		return "cannot load Kubernetes node policy status: " + err.Error()
	}
	return "every DNS node must be Ready=True and DiskPressure=False"
}

func dnsNodeReachabilityOK(checks []model.DNSDelegationNodeCheck) bool {
	if len(checks) == 0 {
		return false
	}
	for _, check := range checks {
		if !check.UDP53Reachable || !check.TCP53Reachable || !check.ProbePass {
			return false
		}
	}
	return true
}

type dnsDelegationPlanHint struct {
	Nameservers []string
	ARecords    map[string][]string
}

func dnsDelegationPlanHints(zone string, staticRecords []model.EdgeDNSRecord) dnsDelegationPlanHint {
	zone = normalizeExternalAppDomain(zone)
	hint := dnsDelegationPlanHint{ARecords: map[string][]string{}}
	seenNS := map[string]struct{}{}
	for _, record := range edgeDNSStaticRecordsForZone(staticRecords, zone) {
		recordName := normalizeExternalAppDomain(record.Name)
		recordType := strings.ToUpper(strings.TrimSpace(record.Type))
		switch recordType {
		case model.EdgeDNSRecordTypeNS:
			if recordName != zone {
				continue
			}
			for _, value := range record.Values {
				nsHost := normalizeExternalAppDomain(value)
				if nsHost == "" {
					continue
				}
				if _, ok := seenNS[nsHost]; ok {
					continue
				}
				seenNS[nsHost] = struct{}{}
				hint.Nameservers = append(hint.Nameservers, nsHost)
			}
		case model.EdgeDNSRecordTypeA:
			if recordName == "" {
				continue
			}
			for _, value := range record.Values {
				if ip := net.ParseIP(strings.TrimSpace(value)); ip != nil && ip.To4() != nil {
					hint.ARecords[recordName] = append(hint.ARecords[recordName], ip.String())
				}
			}
		}
	}
	sort.Strings(hint.Nameservers)
	return hint
}

func buildDNSDelegationPlan(zone string, nodes []model.DNSDelegationNodeCheck, currentParentNS []string, hint dnsDelegationPlanHint) model.DNSDelegationPlan {
	zone = normalizeExternalAppDomain(zone)
	eligible := make([]model.DNSDelegationNodeCheck, 0, len(nodes))
	for _, node := range nodes {
		if node.Pass && strings.TrimSpace(node.PublicIP) != "" {
			eligible = append(eligible, node)
		}
	}
	sort.Slice(eligible, func(i, j int) bool {
		return eligible[i].DNSNodeID < eligible[j].DNSNodeID
	})
	plannedA := []model.DNSDelegationRecord{}
	plannedNS := []model.DNSDelegationRecord{}
	rollback := []model.DNSDelegationRecord{}
	notes := []string{
		"Run preflight until pass=true before changing parent-zone records.",
		"Do not update registrar or parent-zone delegation until at least two DNS nodes are healthy.",
	}
	limit := len(eligible)
	if limit > 2 {
		limit = 2
	}
	orderedNodes := orderDNSDelegationNodesByNameserverHint(eligible, hint, limit)
	for index, node := range orderedNodes {
		nsHost := fmt.Sprintf("ns%d.%s", index+1, zone)
		if index < len(hint.Nameservers) {
			nsHost = hint.Nameservers[index]
		}
		ip := strings.TrimSpace(node.PublicIP)
		plannedA = append(plannedA, model.DNSDelegationRecord{
			Name:    nsHost,
			Type:    "A",
			Values:  []string{ip},
			TTL:     defaultDNSDelegationPlanTTL,
			Comment: "authoritative DNS node " + node.DNSNodeID,
		})
		plannedNS = append(plannedNS, model.DNSDelegationRecord{
			Name:    zone,
			Type:    "NS",
			Values:  []string{nsHost},
			TTL:     defaultDNSDelegationPlanTTL,
			Comment: "delegate child zone to fugue-dns",
		})
		rollback = append(rollback,
			model.DNSDelegationRecord{Name: nsHost, Type: "A", Values: []string{ip}, Comment: "delete if delegation is rolled back"},
			model.DNSDelegationRecord{Name: zone, Type: "NS", Values: []string{nsHost}, Comment: "delete if delegation is rolled back"},
		)
	}
	if len(eligible) < defaultDNSDelegationMinHealthyNodes {
		notes = append(notes, fmt.Sprintf("Only %d DNS nodes have public IPv4; add another node before production delegation.", len(eligible)))
	}
	return model.DNSDelegationPlan{
		CurrentParentNS:       currentParentNS,
		PlannedARecords:       plannedA,
		PlannedNSRecords:      plannedNS,
		RollbackDeleteRecords: rollback,
		Notes:                 notes,
	}
}

func orderDNSDelegationNodesByNameserverHint(nodes []model.DNSDelegationNodeCheck, hint dnsDelegationPlanHint, limit int) []model.DNSDelegationNodeCheck {
	if limit <= 0 {
		return nil
	}
	byIP := map[string]model.DNSDelegationNodeCheck{}
	for _, node := range nodes {
		ip := strings.TrimSpace(node.PublicIP)
		if ip == "" {
			continue
		}
		if _, ok := byIP[ip]; !ok {
			byIP[ip] = node
		}
	}
	used := map[string]struct{}{}
	ordered := make([]model.DNSDelegationNodeCheck, 0, limit)
	for _, nsHost := range hint.Nameservers {
		if len(ordered) >= limit {
			break
		}
		for _, ip := range hint.ARecords[nsHost] {
			node, ok := byIP[ip]
			if !ok {
				continue
			}
			if _, alreadyUsed := used[node.DNSNodeID]; alreadyUsed {
				continue
			}
			ordered = append(ordered, node)
			used[node.DNSNodeID] = struct{}{}
			break
		}
	}
	for _, node := range nodes {
		if len(ordered) >= limit {
			break
		}
		if _, alreadyUsed := used[node.DNSNodeID]; alreadyUsed {
			continue
		}
		ordered = append(ordered, node)
		used[node.DNSNodeID] = struct{}{}
	}
	return ordered
}

func dnsNodePublicIPv4(node model.DNSNode) string {
	if ip := net.ParseIP(strings.TrimSpace(node.PublicIPv4)); ip != nil && ip.To4() != nil {
		return ip.String()
	}
	return ""
}

func stringSliceContains(values []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}
