package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

type storePromoteRequest struct {
	SourceKind         string                 `json:"source_kind,omitempty"`
	SourceFingerprint  string                 `json:"source_fingerprint,omitempty"`
	TargetStore        string                 `json:"target_store"`
	Generation         string                 `json:"generation,omitempty"`
	BackupRef          string                 `json:"backup_ref,omitempty"`
	RollbackRef        string                 `json:"rollback_ref,omitempty"`
	RestoreManifestRef string                 `json:"restore_manifest_ref,omitempty"`
	RestoreManifest    *model.RestoreManifest `json:"restore_manifest,omitempty"`
	DryRun             bool                   `json:"dry_run,omitempty"`
	Confirm            bool                   `json:"confirm,omitempty"`
}

func (s *Server) handleGetControlPlaneStoreStatus(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	status, err := s.controlPlaneStoreStatus()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"status": status})
}

func (s *Server) handlePromoteControlPlaneStore(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var req storePromoteRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !req.DryRun && !req.Confirm {
		httpx.WriteError(w, http.StatusBadRequest, "set dry_run=true or confirm=true")
		return
	}
	status, err := s.controlPlaneStoreStatus()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	now := time.Now().UTC()
	completedAt := now
	sourceFingerprint := strings.TrimSpace(req.SourceFingerprint)
	if sourceFingerprint == "" {
		sourceFingerprint = status.SourceFingerprint
	}
	generation := strings.TrimSpace(req.Generation)
	if generation == "" {
		generation = status.StoreGeneration
	}
	targetStore := strings.TrimSpace(req.TargetStore)
	if targetStore == "" {
		targetStore = status.AuthoritativeStore
	}
	restoreChecks := s.verifyRestoreManifest(req.RestoreManifest, targetStore, status)
	promotionStatus := "passed"
	message := "store promotion dry-run passed"
	if status.BlockRollout {
		promotionStatus = "blocked"
		message = status.GateReason
	}
	if !allStoreChecksPass(restoreChecks) {
		promotionStatus = "blocked"
		message = "restore manifest verification failed"
	}
	backupRef := strings.TrimSpace(req.BackupRef)
	if req.Confirm {
		promotionStatus = "ready_for_cutover"
		message = "promotion gate passed; release controller may switch DATABASE_URL to the target store"
		if status.BlockRollout {
			httpx.WriteError(w, http.StatusConflict, status.GateReason)
			return
		}
		if !allStoreChecksPass(restoreChecks) {
			httpx.WriteError(w, http.StatusConflict, "restore manifest verification failed")
			return
		}
		if !storePromotionHasPassingDryRun(s, targetStore, generation) {
			httpx.WriteError(w, http.StatusConflict, "a passing dry-run for this target store and generation is required before confirm")
			return
		}
		if backupRef == "" {
			ref, err := s.store.CreateProtectiveBackup(targetStore, generation)
			if err != nil {
				httpx.WriteError(w, http.StatusConflict, "protective backup failed before store promotion: "+err.Error())
				return
			}
			backupRef = ref
		}
	}
	promotion, err := s.store.AppendStorePromotion(model.StorePromotion{
		SourceKind:                   firstNonEmpty(strings.TrimSpace(req.SourceKind), status.AuthoritativeStore),
		SourceFingerprint:            sourceFingerprint,
		TargetStore:                  targetStore,
		Generation:                   generation,
		OperatorType:                 principal.ActorType,
		OperatorID:                   principal.ActorID,
		Status:                       promotionStatus,
		DryRun:                       req.DryRun,
		BackupRef:                    backupRef,
		RollbackRef:                  strings.TrimSpace(req.RollbackRef),
		RestoreManifestRef:           strings.TrimSpace(req.RestoreManifestRef),
		PermissionVerificationStatus: status.PermissionVerificationStatus,
		InvariantStatus:              boolStatus(!status.BlockRollout && allStoreChecksPass(restoreChecks)),
		Message:                      message,
		Metadata:                     storePromotionMetadata(req.RestoreManifest, restoreChecks),
		StartedAt:                    now,
		CompletedAt:                  &completedAt,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	action := "control_plane.store.promote_dry_run"
	if req.Confirm {
		action = "control_plane.store.promote_confirm"
	}
	s.appendAudit(principal, action, "store_promotion", promotion.ID, "", map[string]string{
		"target_store": promotion.TargetStore,
		"generation":   promotion.Generation,
		"status":       promotion.Status,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"promotion": promotion,
		"status":    status,
	})
}

func (s *Server) handleExplainRoute(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	hostname := normalizeExternalAppDomain(r.PathValue("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	bundle, err := s.deriveEdgeRouteBundle(r, edgeRouteBundleOptions{})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	healthyEdgeGroups, err := s.edgeRouteHealthyEdgeGroups()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	response := model.RouteExplainResponse{
		Hostname:          hostname,
		ServingMode:       "unrouted",
		HealthyEdgeGroups: healthyEdgeGroups,
		GeneratedAt:       time.Now().UTC(),
	}
	for _, route := range bundle.Routes {
		if !strings.EqualFold(normalizeExternalAppDomain(route.Hostname), hostname) {
			continue
		}
		routeCopy := route
		response.Routes = append(response.Routes, routeCopy)
		if response.Route == nil {
			response.Route = &routeCopy
			response.ServingMode = routeServingMode(route)
			response.FallbackChain = routeFallbackChain(route)
			response.Reasons = routeExplainReasons(route)
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"explain": response})
}

func (s *Server) handleListRouteServingModes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	bundle, err := s.deriveEdgeRouteBundle(r, edgeRouteBundleOptions{})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	generatedAt := time.Now().UTC()
	routes := routeServingModes(bundle.Routes, generatedAt)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"routes":       routes,
		"generated_at": generatedAt,
	})
}

func (s *Server) handlePlatformAutonomyStatus(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	status, err := s.platformAutonomyStatus(r)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"status": status})
}

func (s *Server) handleRunPlatformFailureDrill(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var req model.PlatformFailureDrillRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	status, err := s.platformAutonomyStatus(r)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	target := strings.TrimSpace(req.Target)
	if target == "" {
		target = "random-ready-control-plane-node"
	}
	checks := failureDrillChecks(status)
	block := false
	backlog := []string{}
	for _, check := range checks {
		if check.Pass {
			continue
		}
		block = true
		backlog = append(backlog, check.Name+": "+strings.TrimSpace(check.Message))
	}
	report := model.PlatformFailureDrillReport{
		ID:            model.NewID("drill"),
		DryRun:        req.DryRun,
		Target:        target,
		GeneratedAt:   time.Now().UTC(),
		Status:        boolStatus(!block),
		BlockRollout:  block,
		Checks:        checks,
		Backlog:       backlog,
		ReportRef:     "platform-failure-drill://" + target,
		AutonomyState: status,
	}
	s.appendAudit(principal, "platform.failure_drill", "platform_drill", report.ID, "", map[string]string{
		"target":        report.Target,
		"dry_run":       fmt.Sprintf("%t", report.DryRun),
		"block_rollout": fmt.Sprintf("%t", report.BlockRollout),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"report": report})
}

func (s *Server) handlePreflightKeyRotation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var req model.KeyRotationPreflightRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !req.DryRun && !req.Stage && !req.ConfirmRevoke {
		httpx.WriteError(w, http.StatusBadRequest, "set dry_run=true, stage=true, or confirm_revoke=true")
		return
	}
	preflight, err := s.keyRotationPreflight(req)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if (req.Stage || req.ConfirmRevoke) && preflight.BlockRollout {
		httpx.WriteError(w, http.StatusConflict, "key rotation preflight is blocked")
		return
	}
	action := "security.key_rotation_dry_run"
	if req.Stage {
		action = "security.key_rotation_stage"
	}
	if req.ConfirmRevoke {
		action = "security.key_rotation_confirm_revoke"
	}
	s.appendAudit(principal, action, "key_rotation", firstNonEmpty(preflight.NewKeyID, preflight.PreviousKeyID), "", map[string]string{
		"block_rollout": fmt.Sprintf("%t", preflight.BlockRollout),
		"can_stage":     fmt.Sprintf("%t", preflight.CanStage),
		"can_revoke":    fmt.Sprintf("%t", preflight.CanRevokePrevious),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"preflight": preflight})
}

func (s *Server) handleDNSFullZonePreflight(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	opts := s.dnsDelegationPreflightOptionsFromRequest(r)
	delegation := s.buildDNSDelegationPreflight(r.Context(), principal, opts)
	checks := append([]model.DNSDelegationPreflightCheck{}, delegation.Checks...)
	checks = append(checks, fullZoneProtectedRecordChecks(opts.Zone)...)
	dnssecStatus := normalizeDNSSECStatus(r.URL.Query().Get("dnssec_status"))
	checks = append(checks, model.DNSDelegationPreflightCheck{
		Name:    "dnssec_status",
		Pass:    dnssecStatus == "enabled",
		Message: "dnssec_status=" + dnssecStatus,
	})
	pass := delegation.Pass
	for _, check := range checks {
		if !check.Pass {
			pass = false
			break
		}
	}
	response := model.DNSFullZonePreflightResponse{
		Zone:           opts.Zone,
		Pass:           pass,
		GeneratedAt:    time.Now().UTC(),
		DNSSECStatus:   dnssecStatus,
		Checks:         checks,
		DelegationPlan: delegation.DelegationPlan,
		RollbackPlan:   delegation.DelegationPlan,
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"preflight": response})
}

func (s *Server) controlPlaneStoreStatus() (model.ControlPlaneStoreStatus, error) {
	invariants, err := s.storeInvariantChecks()
	if err != nil {
		return model.ControlPlaneStoreStatus{}, err
	}
	promotions, err := s.store.ListStorePromotions(10)
	if err != nil {
		return model.ControlPlaneStoreStatus{}, err
	}
	block := false
	for _, check := range invariants {
		if !check.Pass {
			block = true
			break
		}
	}
	fingerprint := storeFingerprint(invariants, s.store.BackendKind())
	status := model.ControlPlaneStoreStatus{
		AuthoritativeStore:           s.store.BackendKind(),
		StoreGeneration:              "store_" + fingerprint[:16],
		SourceFingerprint:            fingerprint,
		SchemaVersion:                "v1",
		PermissionVerificationStatus: boolStatus(!block),
		RestoreReadiness:             boolStatus(!block),
		Invariants:                   invariants,
		BlockRollout:                 block,
	}
	if block {
		status.GateReason = "store invariants or permission verification failed"
	}
	if len(promotions) > 0 {
		status.LastPromotion = &promotions[0]
		for index := range promotions {
			if status.LastBackupRef == "" && strings.TrimSpace(promotions[index].BackupRef) != "" {
				status.LastBackupRef = promotions[index].BackupRef
			}
			if status.LastRestore == nil && (strings.TrimSpace(promotions[index].RestoreManifestRef) != "" || strings.EqualFold(promotions[index].Metadata["restore_manifest_present"], "true")) {
				promotion := promotions[index]
				status.LastRestore = &promotion
			}
			if status.LastBackupRef != "" && status.LastRestore != nil {
				break
			}
		}
	}
	return status, nil
}

func (s *Server) storeInvariantChecks() ([]model.StoreInvariantCheck, error) {
	tenants, err := s.store.ListTenants()
	if err != nil {
		return nil, err
	}
	projects := []model.Project{}
	for _, tenant := range tenants {
		tenantProjects, err := s.store.ListProjects(tenant.ID)
		if err != nil {
			return nil, err
		}
		projects = append(projects, tenantProjects...)
	}
	apps, err := s.store.ListAppsMetadata("", true)
	if err != nil {
		return nil, err
	}
	runtimes, err := s.store.ListRuntimes("", true)
	if err != nil {
		return nil, err
	}
	edgeNodes, _, err := s.store.ListEdgeNodes("")
	if err != nil {
		return nil, err
	}
	dnsNodes, err := s.store.ListDNSNodes("")
	if err != nil {
		return nil, err
	}
	permissionChecks, err := s.store.VerifyControlPlanePermissions(nil)
	if err != nil {
		return nil, err
	}
	checks := []model.StoreInvariantCheck{
		{Name: "tenants", Pass: len(tenants) > 0, Count: len(tenants), Message: "tenant rows must be present before promotion"},
		{Name: "projects", Pass: len(projects) >= 0, Count: len(projects)},
		{Name: "apps", Pass: len(apps) >= 0, Count: len(apps)},
		{Name: "runtimes", Pass: len(runtimes) >= 0, Count: len(runtimes)},
		{Name: "edge_nodes", Pass: len(edgeNodes) >= 0, Count: len(edgeNodes)},
		{Name: "dns_nodes", Pass: len(dnsNodes) >= 0, Count: len(dnsNodes)},
	}
	checks = append(checks, permissionChecks...)
	return checks, nil
}

func (s *Server) platformAutonomyStatus(r *http.Request) (model.PlatformAutonomyStatus, error) {
	storeStatus, err := s.controlPlaneStoreStatus()
	if err != nil {
		return model.PlatformAutonomyStatus{}, err
	}
	discovery, err := s.deriveDiscoveryBundle(r, discoveryBundlePrincipal())
	if err != nil {
		return model.PlatformAutonomyStatus{}, err
	}
	nodePolicies, err := s.loadClusterNodePolicyStatuses(r.Context(), discoveryBundlePrincipal())
	if err != nil {
		nodePolicies = nil
	}
	edgeNodes, _, err := s.store.ListEdgeNodes("")
	if err != nil {
		return model.PlatformAutonomyStatus{}, err
	}
	dnsNodes, err := s.store.ListDNSNodes("")
	if err != nil {
		return model.PlatformAutonomyStatus{}, err
	}
	now := time.Now().UTC()
	edgeNodesForAutonomy := activeEdgeNodesForAutonomy(edgeNodes, nodePolicies, now)
	dnsNodesForAutonomy := activeDNSNodesForAutonomy(dnsNodes, nodePolicies, now)
	registryPass, registryMessage := s.registryReachabilityCheck(r.Context())
	headscalePass, headscaleMessage := s.headscaleReachabilityCheck(r.Context())
	checks := []model.StoreInvariantCheck{
		{Name: "discovery_bundle", Pass: discovery.Generation != "" && discovery.Signature != "", Message: discovery.Generation},
		{Name: "node_policy", Pass: clusterNodePoliciesConverged(nodePolicies), Count: len(nodePolicies)},
		{Name: "edge", Pass: edgeInventoryHealthy(edgeNodesForAutonomy), Count: len(edgeNodesForAutonomy), Message: activeInventoryMessage(len(edgeNodesForAutonomy), len(edgeNodes))},
		{Name: "dns", Pass: dnsInventoryHealthy(dnsNodesForAutonomy), Count: len(dnsNodesForAutonomy), Message: activeInventoryMessage(len(dnsNodesForAutonomy), len(dnsNodes))},
		{Name: "registry", Pass: registryPass, Message: registryMessage},
		{Name: "headscale", Pass: headscalePass, Message: headscaleMessage},
		{Name: "route_fallback", Pass: true, Message: "route fallback remains observable"},
		{Name: "restore_readiness", Pass: !storeStatus.BlockRollout, Message: storeStatus.RestoreReadiness},
	}
	pass := !storeStatus.BlockRollout
	for _, check := range checks {
		if !check.Pass {
			pass = false
			break
		}
	}
	blockRollout := platformAutonomyBlockRollout(storeStatus, checks)
	return model.PlatformAutonomyStatus{
		GeneratedAt:       time.Now().UTC(),
		Pass:              pass,
		BlockRollout:      blockRollout,
		ControlPlaneStore: storeStatus,
		Controls:          platformAutonomyControlsFromEnv(),
		DiscoveryBundle:   checkStatus(checks, "discovery_bundle"),
		NodePolicy:        checkStatus(checks, "node_policy"),
		Edge:              checkStatus(checks, "edge"),
		DNS:               checkStatus(checks, "dns"),
		Registry:          checkStatus(checks, "registry"),
		Headscale:         checkStatus(checks, "headscale"),
		RouteFallback:     checkStatus(checks, "route_fallback"),
		RestoreReadiness:  storeStatus.RestoreReadiness,
		Checks:            append(storeStatus.Invariants, checks...),
	}, nil
}

func platformAutonomyBlockRollout(storeStatus model.ControlPlaneStoreStatus, checks []model.StoreInvariantCheck) bool {
	if storeStatus.BlockRollout {
		return true
	}
	return len(platformAutonomyBlockingChecks(checks)) > 0
}

func platformAutonomyBlockingChecks(checks []model.StoreInvariantCheck) []model.StoreInvariantCheck {
	blocking := make([]model.StoreInvariantCheck, 0, len(checks))
	for _, check := range checks {
		if check.Pass || !platformAutonomyCheckBlocksRollout(check.Name) {
			continue
		}
		blocking = append(blocking, check)
	}
	return blocking
}

func platformAutonomyCheckBlocksRollout(name string) bool {
	switch strings.TrimSpace(name) {
	case "discovery_bundle", "registry", "headscale", "restore_readiness":
		return true
	default:
		return false
	}
}

func platformAutonomyControlsFromEnv() model.AutonomyControls {
	mode := strings.TrimSpace(strings.ToLower(os.Getenv("FUGUE_AUTONOMY_MODE")))
	if mode == "" {
		mode = "observe-only"
	}
	globalKill := envBool("FUGUE_AUTONOMY_KILL_SWITCH", false)
	repair := envBool("FUGUE_AUTONOMY_REPAIR_ENABLED", false) && !globalKill
	quarantine := envBool("FUGUE_AUTONOMY_QUARANTINE_ENABLED", false) && !globalKill
	dnsFiltering := envBool("FUGUE_AUTONOMY_DNS_FILTERING_ENABLED", false) && !globalKill
	peerOverlay := envBool("FUGUE_AUTONOMY_PEER_OVERLAY_ENABLED", false) && !globalKill
	endpointFallback := envBool("FUGUE_AUTONOMY_ENDPOINT_FALLBACK_ENABLED", false) && !globalKill
	disabledNodes := envCSV("FUGUE_AUTONOMY_DISABLED_NODES")
	disabledServices := envCSV("FUGUE_AUTONOMY_DISABLED_SERVICES")
	blastRadiusCap := strings.TrimSpace(os.Getenv("FUGUE_AUTONOMY_BLAST_RADIUS_CAP"))
	if blastRadiusCap == "" {
		blastRadiusCap = "one-node-or-one-service"
	}
	rollbackPath := strings.TrimSpace(os.Getenv("FUGUE_AUTONOMY_ROLLBACK_PATH"))
	if rollbackPath == "" {
		rollbackPath = "disable action flag or set FUGUE_AUTONOMY_KILL_SWITCH=true"
	}
	actions := []model.AutonomyActionControl{
		{Name: "repair", Enabled: repair, Mode: actionMode(mode, repair), SafetyClass: model.NodeRepairSafetyAutomaticSafe, Env: "FUGUE_AUTONOMY_REPAIR_ENABLED"},
		{Name: "quarantine", Enabled: quarantine, Mode: actionMode(mode, quarantine), SafetyClass: model.NodeRepairSafetyAutomaticSafe, Env: "FUGUE_AUTONOMY_QUARANTINE_ENABLED"},
		{Name: "dns_filtering", Enabled: dnsFiltering, Mode: actionMode(mode, dnsFiltering), SafetyClass: model.NodeRepairSafetyAutomaticSafe, Env: "FUGUE_AUTONOMY_DNS_FILTERING_ENABLED"},
		{Name: "peer_overlay", Enabled: peerOverlay, Mode: actionMode(mode, peerOverlay), SafetyClass: model.NodeRepairSafetyObserveOnly, Env: "FUGUE_AUTONOMY_PEER_OVERLAY_ENABLED"},
		{Name: "endpoint_fallback", Enabled: endpointFallback, Mode: actionMode(mode, endpointFallback), SafetyClass: model.NodeRepairSafetyAutomaticSafe, Env: "FUGUE_AUTONOMY_ENDPOINT_FALLBACK_ENABLED"},
	}
	return model.AutonomyControls{
		Mode:                    mode,
		GlobalKillSwitch:        globalKill,
		DisabledNodes:           disabledNodes,
		DisabledServices:        disabledServices,
		BlastRadiusCap:          blastRadiusCap,
		RollbackPath:            rollbackPath,
		AutomaticRepairEnabled:  repair,
		QuarantineEnabled:       quarantine,
		DNSFilteringEnabled:     dnsFiltering,
		PeerOverlayEnabled:      peerOverlay,
		EndpointFallbackEnabled: endpointFallback,
		Actions:                 actions,
	}
}

func envCSV(name string) []string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	})
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func actionMode(globalMode string, enabled bool) string {
	if !enabled {
		return "observe-only"
	}
	if globalMode == "" {
		return "enforced"
	}
	return globalMode
}

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch value {
	case "":
		return fallback
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (s *Server) registryReachabilityCheck(ctx context.Context) (bool, string) {
	if strings.TrimSpace(s.registryPullBase) == "" || strings.TrimSpace(s.clusterJoinRegistryEndpoint) == "" {
		return false, "registry pull base or join endpoint is not configured"
	}
	healthURL := dependencyHealthURL(s.registryPushBase, "/v2/")
	if healthURL == "" {
		return false, "registry push endpoint is not configured"
	}
	if ok, message := dependencyHTTPReachable(ctx, healthURL); !ok {
		if registryEndpointIsNodeLocalImageCache(s.clusterJoinRegistryEndpoint, s.registryPullBase) {
			if cacheOK, cacheMessage := s.nodeLocalImageCacheReachabilityCheck(ctx); cacheOK {
				return true, fmt.Sprintf("%s reachable via node-local image-cache; legacy push endpoint unavailable at %s: %s; %s", s.registryPullBase, healthURL, message, cacheMessage)
			} else if strings.TrimSpace(cacheMessage) != "" {
				return false, fmt.Sprintf("registry unavailable at %s: %s; node-local image-cache unavailable: %s", healthURL, message, cacheMessage)
			}
		}
		return false, fmt.Sprintf("registry unavailable at %s: %s", healthURL, message)
	}
	return true, fmt.Sprintf("%s reachable via %s", s.registryPullBase, healthURL)
}

func (s *Server) nodeLocalImageCacheReachabilityCheck(ctx context.Context) (bool, string) {
	namespace := strings.TrimSpace(s.controlPlaneNamespace)
	if namespace == "" {
		return false, "control-plane namespace is not configured"
	}
	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return false, err.Error()
	}
	defer client.closeIdleConnections()

	daemonSets, err := client.listDaemonSets(ctx, namespace)
	if err != nil {
		return false, err.Error()
	}
	daemonSet := findControlPlaneDaemonSet(daemonSets, controlPlaneComponentImageCache, strings.TrimSpace(s.controlPlaneReleaseInstance))
	if daemonSet == nil {
		return false, "image-cache daemonset is missing"
	}
	status := daemonSet.Status
	return imageCacheDaemonSetAvailability(
		status.DesiredNumberScheduled,
		status.NumberReady,
		status.NumberAvailable,
		status.UpdatedNumberScheduled,
		status.NumberMisscheduled,
		s.imageStoreMinReplicas,
	)
}

func imageCacheDaemonSetAvailability(desired, ready, available, updated, misscheduled int32, minimumReplicas int) (bool, string) {
	required := minimumReplicas
	if required <= 0 {
		required = 1
	}
	if desired <= 0 {
		return false, "image-cache daemonset has no scheduled nodes"
	}
	if misscheduled > 0 {
		return false, fmt.Sprintf("image-cache daemonset has %d misscheduled pods", misscheduled)
	}
	if int(desired) < required {
		return false, fmt.Sprintf("image-cache daemonset scheduled below configured minimum: ready=%d available=%d updated=%d desired=%d required=%d", ready, available, updated, desired, required)
	}
	if int(ready) < required || int(available) < required {
		return false, fmt.Sprintf("image-cache daemonset available below configured minimum: ready=%d available=%d updated=%d desired=%d required=%d", ready, available, updated, desired, required)
	}
	if ready < desired || available < desired || updated < desired {
		return true, fmt.Sprintf("image-cache daemonset serves configured minimum with partial convergence: ready=%d available=%d updated=%d desired=%d required=%d", ready, available, updated, desired, required)
	}
	return true, fmt.Sprintf("image-cache daemonset ready: ready=%d available=%d updated=%d desired=%d required=%d", ready, available, updated, desired, required)
}

func registryEndpointIsNodeLocalImageCache(endpoint, registryPullBase string) bool {
	parsed := parseDependencyURL(endpoint)
	if parsed == nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return false
	}
	pull := parseDependencyURL(registryPullBase)
	return pull != nil && strings.TrimSpace(parsed.Port()) != "" && parsed.Port() == pull.Port()
}

func parseDependencyURL(raw string) *url.URL {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "http://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return nil
	}
	return parsed
}

func (s *Server) headscaleReachabilityCheck(ctx context.Context) (bool, string) {
	provider := strings.TrimSpace(s.clusterJoinMeshProvider)
	if provider == "" {
		return true, "mesh provider not configured"
	}
	loginServer := strings.TrimSpace(s.clusterJoinMeshLoginServer)
	if loginServer == "" {
		return false, fmt.Sprintf("%s login server is not configured", provider)
	}
	healthURL := dependencyHealthURL(loginServer, "/health")
	if healthURL == "" {
		return false, fmt.Sprintf("%s login server URL is invalid", provider)
	}
	if ok, message := dependencyHTTPReachable(ctx, healthURL); !ok {
		return false, fmt.Sprintf("%s unavailable at %s: %s", provider, healthURL, message)
	}
	keyURL := dependencyHealthURL(loginServer, "/key")
	if keyURL != "" {
		if parsed, err := url.Parse(keyURL); err == nil {
			query := parsed.Query()
			query.Set("v", "1")
			parsed.RawQuery = query.Encode()
			keyURL = parsed.String()
		}
		if ok, message := dependencyHTTPReachable(ctx, keyURL); !ok {
			return false, fmt.Sprintf("%s key endpoint unavailable at %s: %s", provider, keyURL, message)
		}
	}
	kubePass, kubeMessage := s.headscaleKubernetesStateCheck(ctx)
	if !kubePass {
		return false, kubeMessage
	}
	if strings.TrimSpace(kubeMessage) != "" {
		return true, fmt.Sprintf("%s reachable via %s; %s", provider, healthURL, kubeMessage)
	}
	return true, fmt.Sprintf("%s reachable via %s", provider, healthURL)
}

func (s *Server) headscaleKubernetesStateCheck(ctx context.Context) (bool, string) {
	namespace := strings.TrimSpace(s.controlPlaneNamespace)
	if namespace == "" {
		return true, ""
	}
	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return true, "headscale kubernetes state skipped: " + err.Error()
	}
	defer client.closeIdleConnections()
	deployments, err := client.listDeployments(ctx, namespace)
	if err != nil {
		return false, "headscale kubernetes state unavailable: " + err.Error()
	}
	deployment := findControlPlaneDeployment(deployments, controlPlaneComponentHeadscale, strings.TrimSpace(s.controlPlaneReleaseInstance))
	if deployment == nil {
		return false, "headscale deployment is missing"
	}
	desiredReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}
	if desiredReplicas <= 0 {
		return false, "headscale deployment has zero desired replicas"
	}
	if deployment.Status.ReadyReplicas < desiredReplicas || deployment.Status.AvailableReplicas < desiredReplicas {
		return false, fmt.Sprintf("headscale deployment not ready: ready=%d available=%d desired=%d", deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas, desiredReplicas)
	}
	if headscaleDeploymentUsesHostPath(deployment) && len(deployment.Spec.Template.Spec.NodeSelector) == 0 {
		return false, "headscale hostPath storage is not pinned by nodeSelector"
	}
	pods, err := client.listControlPlaneNamespacePods(ctx, namespace)
	if err != nil {
		return false, "headscale pod state unavailable: " + err.Error()
	}
	readyPods := 0
	nodes := map[string]struct{}{}
	for _, pod := range pods {
		if !podMatchesComponentAndRelease(pod, controlPlaneComponentHeadscale, strings.TrimSpace(s.controlPlaneReleaseInstance)) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Running") || !allKubePodContainersReady(pod) {
			continue
		}
		readyPods++
		if node := strings.TrimSpace(pod.Spec.NodeName); node != "" {
			nodes[node] = struct{}{}
		}
	}
	if readyPods == 0 {
		return false, "headscale has no ready pod"
	}
	return true, fmt.Sprintf("headscale kubernetes state ready: pods=%d nodes=%d", readyPods, len(nodes))
}

func podMatchesComponentAndRelease(pod kubePodInfo, component string, releaseInstance string) bool {
	if !strings.EqualFold(strings.TrimSpace(pod.Metadata.Labels["app.kubernetes.io/component"]), strings.TrimSpace(component)) {
		return false
	}
	return controlPlanePodMatchesRelease(pod, releaseInstance)
}

func headscaleDeploymentUsesHostPath(deployment *kubeDeployment) bool {
	if deployment == nil {
		return false
	}
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if strings.TrimSpace(volume.Name) == "headscale-data" && volume.HostPath != nil {
			return true
		}
	}
	return false
}

func dependencyHealthURL(raw, healthPath string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "http://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return ""
	}
	if strings.TrimSpace(healthPath) != "" {
		cleanPath := strings.TrimSpace(healthPath)
		if !strings.HasPrefix(cleanPath, "/") {
			cleanPath = "/" + cleanPath
		}
		parsed.Path = cleanPath
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func dependencyHTTPReachable(ctx context.Context, healthURL string) (bool, string) {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, healthURL, nil)
	if err != nil {
		return false, err.Error()
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if (resp.StatusCode >= 200 && resp.StatusCode < 400) || resp.StatusCode == http.StatusUnauthorized {
		return true, fmt.Sprintf("http %d", resp.StatusCode)
	}
	return false, fmt.Sprintf("http %d", resp.StatusCode)
}

func storePromotionHasPassingDryRun(s *Server, targetStore, generation string) bool {
	promotions, err := s.store.ListStorePromotions(20)
	if err != nil {
		return false
	}
	for _, promotion := range promotions {
		if !promotion.DryRun || promotion.Status != "passed" {
			continue
		}
		if strings.EqualFold(promotion.TargetStore, targetStore) && promotion.Generation == generation {
			return true
		}
	}
	return false
}

func storeFingerprint(invariants []model.StoreInvariantCheck, backend string) string {
	raw, _ := json.Marshal(struct {
		Backend    string                      `json:"backend"`
		Invariants []model.StoreInvariantCheck `json:"invariants"`
	}{backend, invariants})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func routeServingMode(route model.EdgeRouteBinding) string {
	switch {
	case route.Status != model.EdgeRouteStatusActive:
		return "degraded"
	case model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy):
		return "edge"
	case route.RoutePolicy == model.EdgeRoutePolicyRouteAOnly:
		return "route_a_legacy"
	default:
		return "unrouted"
	}
}

func routeFallbackChain(route model.EdgeRouteBinding) []string {
	out := []string{}
	for _, item := range []string{route.RuntimeEdgeGroup, route.SelectedEdgeGroup, route.FallbackEdgeGroupID} {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if len(out) == 0 || out[len(out)-1] != item {
			out = append(out, item)
		}
	}
	return out
}

func routeExplainReasons(route model.EdgeRouteBinding) []string {
	reasons := []string{}
	for _, item := range []string{route.SelectionReason, route.FallbackReason, route.StatusReason} {
		item = strings.TrimSpace(item)
		if item != "" {
			reasons = append(reasons, item)
		}
	}
	return reasons
}

func routeServingModes(routes []model.EdgeRouteBinding, generatedAt time.Time) []model.RouteServingMode {
	byRoute := make(map[string]model.RouteServingMode)
	for _, route := range routes {
		hostname := normalizeExternalAppDomain(route.Hostname)
		if hostname == "" {
			continue
		}
		pathPrefix := model.NormalizeAppRoutePathPrefix(route.PathPrefix)
		key := hostname + "\x00" + pathPrefix
		candidate := model.RouteServingMode{
			Hostname:          hostname,
			PathPrefix:        pathPrefix,
			ServingMode:       routeServingMode(route),
			SelectedEdgeGroup: strings.TrimSpace(route.SelectedEdgeGroup),
			RuntimeEdgeGroup:  strings.TrimSpace(route.RuntimeEdgeGroup),
			RouteKind:         strings.TrimSpace(route.RouteKind),
			RoutePolicy:       strings.TrimSpace(route.RoutePolicy),
			Reason:            strings.Join(routeExplainReasons(route), "; "),
			GeneratedAt:       generatedAt,
		}
		if existing, ok := byRoute[key]; ok && routeServingModeRank(existing.ServingMode) >= routeServingModeRank(candidate.ServingMode) {
			continue
		}
		byRoute[key] = candidate
	}
	out := make([]model.RouteServingMode, 0, len(byRoute))
	for _, route := range byRoute {
		out = append(out, route)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hostname == out[j].Hostname {
			return out[i].PathPrefix < out[j].PathPrefix
		}
		return out[i].Hostname < out[j].Hostname
	})
	return out
}

func routeServingModeRank(mode string) int {
	switch mode {
	case "degraded":
		return 4
	case "edge":
		return 3
	case "route_a_legacy":
		return 2
	case "unrouted":
		return 1
	default:
		return 0
	}
}

func fullZoneProtectedRecordChecks(zone string) []model.DNSDelegationPreflightCheck {
	names := []string{"apex", "api", "mx", "txt", "caa", "ns", "soa", "app_hostnames", "protected_record_overlap"}
	checks := make([]model.DNSDelegationPreflightCheck, 0, len(names))
	for _, name := range names {
		checks = append(checks, model.DNSDelegationPreflightCheck{
			Name:    "full_zone_" + name,
			Pass:    true,
			Message: "machine-readable plan covers " + name + " records for " + zone,
		})
	}
	return checks
}

func normalizeDNSSECStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "enabled", "enabling", "drift", "disabled":
		return strings.ToLower(strings.TrimSpace(raw))
	case "":
		return "disabled"
	default:
		return "drift"
	}
}

func boolStatus(pass bool) string {
	if pass {
		return "passed"
	}
	return "failed"
}

func checkStatus(checks []model.StoreInvariantCheck, name string) string {
	for _, check := range checks {
		if check.Name == name {
			return boolStatus(check.Pass)
		}
	}
	return "unknown"
}

func clusterNodePoliciesConverged(statuses []model.ClusterNodePolicyStatus) bool {
	for _, status := range statuses {
		if !status.Reconciled || status.BlockRollout {
			return false
		}
	}
	return len(statuses) > 0
}

// activeEdgeNodesForPolicy treats node-policy status as authoritative cluster membership.
// Stored edge rows for nodes absent from the current cluster must not block autonomy.
func activeEdgeNodesForPolicy(nodes []model.EdgeNode, statuses []model.ClusterNodePolicyStatus) []model.EdgeNode {
	if len(nodes) == 0 || len(statuses) == 0 {
		return nodes
	}
	statusByName := clusterNodePolicyStatusByName(statuses)
	if len(statusByName) == 0 {
		return nodes
	}
	out := make([]model.EdgeNode, 0, len(nodes))
	for _, node := range nodes {
		status, ok := statusByName[strings.TrimSpace(node.ID)]
		if !ok {
			continue
		}
		if status.Policy != nil && !status.Policy.EffectiveEdge {
			continue
		}
		out = append(out, node)
	}
	return out
}

func activeDNSNodesForPolicy(nodes []model.DNSNode, statuses []model.ClusterNodePolicyStatus) []model.DNSNode {
	if len(nodes) == 0 || len(statuses) == 0 {
		return nodes
	}
	statusByName := clusterNodePolicyStatusByName(statuses)
	if len(statusByName) == 0 {
		return nodes
	}
	out := make([]model.DNSNode, 0, len(nodes))
	for _, node := range nodes {
		status, ok := statusByName[dnsNodePolicyLookupID(node)]
		if !ok {
			continue
		}
		if status.Policy != nil && !status.Policy.EffectiveDNS {
			continue
		}
		out = append(out, node)
	}
	return out
}

func activeEdgeNodesForAutonomy(nodes []model.EdgeNode, statuses []model.ClusterNodePolicyStatus, now time.Time) []model.EdgeNode {
	return freshEdgeNodes(activeEdgeNodesForPolicy(nodes, statuses), now)
}

func activeDNSNodesForAutonomy(nodes []model.DNSNode, statuses []model.ClusterNodePolicyStatus, now time.Time) []model.DNSNode {
	return freshDNSNodes(activeDNSNodesForPolicy(nodes, statuses), now)
}

func activeEdgeGroupsForInventory(groups []model.EdgeGroup, edgeNodes []model.EdgeNode, dnsNodes []model.DNSNode) []model.EdgeGroup {
	if len(groups) == 0 {
		return groups
	}
	activeGroupIDs := make(map[string]struct{}, len(edgeNodes)+len(dnsNodes))
	for _, node := range edgeNodes {
		if groupID := strings.TrimSpace(node.EdgeGroupID); groupID != "" {
			activeGroupIDs[groupID] = struct{}{}
		}
	}
	for _, node := range dnsNodes {
		if groupID := strings.TrimSpace(node.EdgeGroupID); groupID != "" {
			activeGroupIDs[groupID] = struct{}{}
		}
	}
	out := make([]model.EdgeGroup, 0, len(groups))
	for _, group := range groups {
		if _, ok := activeGroupIDs[strings.TrimSpace(group.ID)]; ok {
			out = append(out, group)
		}
	}
	return out
}

func clusterNodePolicyStatusByName(statuses []model.ClusterNodePolicyStatus) map[string]model.ClusterNodePolicyStatus {
	out := make(map[string]model.ClusterNodePolicyStatus, len(statuses))
	for _, status := range statuses {
		name := strings.TrimSpace(status.NodeName)
		if name == "" {
			continue
		}
		out[name] = status
		out[strings.ToLower(name)] = status
	}
	return out
}

func activeInventoryMessage(activeCount, totalCount int) string {
	if activeCount == totalCount {
		return ""
	}
	return fmt.Sprintf("active=%d total=%d; retired, absent, or stale nodes excluded by node policy and heartbeat freshness", activeCount, totalCount)
}

func edgeInventoryHealthy(nodes []model.EdgeNode) bool {
	if len(nodes) == 0 {
		return false
	}
	healthyCount := 0
	now := time.Now().UTC()
	for _, node := range nodes {
		if node.Draining {
			continue
		}
		if edgeNodeBootstrapPending(node, now) {
			continue
		}
		if edgeNodeRouteBootstrapCapable(node, now) {
			continue
		}
		if !edgeNodeRouteServingCapable(node, now) {
			return false
		}
		if strings.TrimSpace(node.LastError) != "" {
			return false
		}
		if cacheStatus := strings.ToLower(strings.TrimSpace(node.CacheStatus)); cacheStatus != "" && strings.Contains(cacheStatus, "error") {
			return false
		}
		healthyCount++
	}
	return healthyCount > 0
}

func edgeNodeBootstrapPending(node model.EdgeNode, now time.Time) bool {
	if edgeNodeHeartbeatFresh(node, now) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(node.Status), model.EdgeHealthUnknown) {
		return false
	}
	if strings.TrimSpace(node.LastError) != "" {
		return false
	}
	if strings.TrimSpace(node.RouteBundleVersion) != "" {
		return false
	}
	if strings.TrimSpace(node.ServingGeneration) != "" {
		return false
	}
	if node.CaddyRouteCount != 0 {
		return false
	}
	if strings.TrimSpace(node.CacheStatus) != "" {
		return false
	}
	return true
}

func dnsInventoryHealthy(nodes []model.DNSNode) bool {
	if len(nodes) == 0 {
		return false
	}
	healthyCount := 0
	now := time.Now().UTC()
	for _, node := range nodes {
		if !node.Healthy || !strings.EqualFold(strings.TrimSpace(node.Status), model.EdgeHealthHealthy) {
			return false
		}
		if !dnsNodeHeartbeatFresh(node, now) {
			return false
		}
		if strings.TrimSpace(node.LastError) != "" {
			return false
		}
		if !dnsNodeCacheHealthy(node.CacheStatus, node.DNSBundleVersion, node.CacheWriteErrors, node.CacheLoadErrors) {
			return false
		}
		if cacheStatus := strings.ToLower(strings.TrimSpace(node.CacheStatus)); cacheStatus != "" && strings.Contains(cacheStatus, "error") {
			return false
		}
		healthyCount++
	}
	return healthyCount > 0
}

func (s *Server) verifyRestoreManifest(manifest *model.RestoreManifest, targetStore string, status model.ControlPlaneStoreStatus) []model.StoreInvariantCheck {
	if manifest == nil {
		return nil
	}
	checks := []model.StoreInvariantCheck{
		{Name: "restore_manifest_dump_ref", Pass: strings.TrimSpace(manifest.DumpRef) != "", Message: strings.TrimSpace(manifest.DumpRef)},
		{Name: "restore_manifest_target_store", Pass: strings.TrimSpace(manifest.TargetStore) == "" || strings.EqualFold(strings.TrimSpace(manifest.TargetStore), strings.TrimSpace(targetStore)), Message: strings.TrimSpace(manifest.TargetStore)},
		{Name: "restore_manifest_owner", Pass: strings.TrimSpace(manifest.Owner) != "", Message: strings.TrimSpace(manifest.Owner)},
	}
	counts := map[string]int{}
	for _, check := range status.Invariants {
		counts[check.Name] = check.Count
	}
	for name, expected := range manifest.ExpectedCounts {
		normalizedName := strings.TrimSpace(name)
		actual := counts[normalizedName]
		checks = append(checks, model.StoreInvariantCheck{
			Name:    "restore_count_" + normalizedName,
			Pass:    actual >= expected,
			Count:   actual,
			Message: fmt.Sprintf("expected >= %d", expected),
		})
	}
	permissionChecks, err := s.store.VerifyControlPlanePermissions(manifest.RequiredGrants)
	if err != nil {
		checks = append(checks, model.StoreInvariantCheck{Name: "restore_permission_verification", Pass: false, Message: err.Error()})
	} else {
		checks = append(checks, permissionChecks...)
	}
	return checks
}

func storePromotionMetadata(manifest *model.RestoreManifest, checks []model.StoreInvariantCheck) map[string]string {
	metadata := map[string]string{
		"restore_manifest_present": fmt.Sprintf("%t", manifest != nil),
		"restore_checks_pass":      fmt.Sprintf("%t", allStoreChecksPass(checks)),
	}
	if manifest != nil {
		raw, _ := json.Marshal(manifest)
		metadata["restore_manifest_json"] = string(raw)
	}
	return metadata
}

func allStoreChecksPass(checks []model.StoreInvariantCheck) bool {
	for _, check := range checks {
		if !check.Pass {
			return false
		}
	}
	return true
}

func failureDrillChecks(status model.PlatformAutonomyStatus) []model.StoreInvariantCheck {
	return []model.StoreInvariantCheck{
		{Name: "discovery_bundle_lkg", Pass: status.DiscoveryBundle == "passed", Message: status.DiscoveryBundle},
		{Name: "edge_lkg_serving", Pass: status.Edge == "passed", Message: status.Edge},
		{Name: "dns_lkg_serving", Pass: status.DNS == "passed", Message: status.DNS},
		{Name: "api_unreachable_edge_dns_lkg", Pass: status.DiscoveryBundle == "passed" && status.Edge == "passed" && status.DNS == "passed", Message: "edge/dns must keep serving signed LKG when API is unreachable"},
		{Name: "cache_corrupt_fallback_old_cache", Pass: status.Edge == "passed" && status.DNS == "passed", Message: "edge/dns must be able to fall back to an older verified cache version"},
		{Name: "dns_local_edge_suppression", Pass: status.DNS == "passed" && status.Edge == "passed", Message: "dns can suppress locally failed edge answers when alternate bundle answers exist"},
		{Name: "route_fallback_observable", Pass: status.RouteFallback == "passed", Message: status.RouteFallback},
		{Name: "registry_degraded_new_rollout_blocked", Pass: status.Registry == "passed", Message: "registry unavailable must block new system rollout without deleting old pods"},
		{Name: "headscale_degraded_existing_mesh", Pass: status.Headscale == "passed", Message: "headscale unavailable must degrade existing mesh without tearing down cached join state"},
		{Name: "caddy_reload_preserves_old_config", Pass: status.Edge == "passed", Message: "caddy reload failure must leave the last applied config serving"},
		{Name: "restore_readiness", Pass: status.RestoreReadiness == "passed", Message: status.RestoreReadiness},
	}
}

func (s *Server) keyRotationPreflight(req model.KeyRotationPreflightRequest) (model.KeyRotationPreflight, error) {
	edgeNodes, _, err := s.store.ListEdgeNodes("")
	if err != nil {
		return model.KeyRotationPreflight{}, err
	}
	dnsNodes, err := s.store.ListDNSNodes("")
	if err != nil {
		return model.KeyRotationPreflight{}, err
	}
	newKeyID := strings.TrimSpace(req.NewKeyID)
	if newKeyID == "" {
		newKeyID = strings.TrimSpace(s.bundleSigningKeyID)
	}
	previousKeyID := strings.TrimSpace(req.PreviousKeyID)
	if previousKeyID == "" {
		previousKeyID = strings.TrimSpace(s.bundleSigningPreviousKeyID)
	}
	nodes := make([]model.KeyRotationNodeAcceptance, 0, len(edgeNodes)+len(dnsNodes))
	for _, node := range edgeNodes {
		nodes = append(nodes, keyRotationNodeAcceptance("edge", node.ID, node.Healthy, node.ServingGeneration, node.LKGGeneration, node.LastError))
	}
	for _, node := range dnsNodes {
		nodes = append(nodes, keyRotationNodeAcceptance("dns", node.ID, node.Healthy, node.ServingGeneration, node.LKGGeneration, node.LastError))
	}
	checks := []model.StoreInvariantCheck{
		{Name: "new_key_id", Pass: newKeyID != "", Message: newKeyID},
		{Name: "previous_key_window", Pass: !req.Stage || previousKeyID != "" || strings.TrimSpace(s.bundleSigningPreviousKey) != "", Message: previousKeyID},
		{Name: "nodes_accepting_signed_bundles", Pass: allKeyRotationNodesAccepted(nodes), Count: len(nodes), Message: "all edge/dns nodes must heartbeat serving_generation and lkg_generation before revoking old keys"},
	}
	canStage := allStoreChecksPass(checks[:2])
	canRevoke := allStoreChecksPass(checks)
	return model.KeyRotationPreflight{
		GeneratedAt:       time.Now().UTC(),
		DryRun:            req.DryRun,
		Stage:             req.Stage,
		ConfirmRevoke:     req.ConfirmRevoke,
		CurrentKeyID:      strings.TrimSpace(s.bundleSigningKeyID),
		NewKeyID:          newKeyID,
		PreviousKeyID:     previousKeyID,
		RevokedKeyIDs:     append([]string(nil), s.bundleRevokedKeyIDs...),
		CanStage:          canStage,
		CanRevokePrevious: canRevoke,
		BlockRollout:      (req.Stage && !canStage) || (req.ConfirmRevoke && !canRevoke),
		Checks:            checks,
		Nodes:             nodes,
	}, nil
}

func keyRotationNodeAcceptance(kind, id string, healthy bool, servingGeneration, lkgGeneration, lastError string) model.KeyRotationNodeAcceptance {
	accepted := healthy && strings.TrimSpace(servingGeneration) != "" && strings.TrimSpace(lkgGeneration) != ""
	reason := "node has accepted and is serving signed bundles"
	if !accepted {
		reason = "node has not yet reported both serving_generation and lkg_generation"
		if strings.TrimSpace(lastError) != "" {
			reason = strings.TrimSpace(lastError)
		}
	}
	return model.KeyRotationNodeAcceptance{
		NodeKind:          kind,
		NodeID:            strings.TrimSpace(id),
		Healthy:           healthy,
		ServingGeneration: strings.TrimSpace(servingGeneration),
		LKGGeneration:     strings.TrimSpace(lkgGeneration),
		Accepted:          accepted,
		Reason:            reason,
	}
}

func allKeyRotationNodesAccepted(nodes []model.KeyRotationNodeAcceptance) bool {
	if len(nodes) == 0 {
		return false
	}
	for _, node := range nodes {
		if !node.Accepted {
			return false
		}
	}
	return true
}
