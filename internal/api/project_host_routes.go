package api

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

var errProjectHostPlanChanged = errors.New("hostname plan is stale; preview current routes before applying again")
var errProjectHostDomainNotReady = errors.New("hostname must have a verified DNS and ready TLS binding")
var errProjectHostOwnerPath = errors.New("at least one effective path must continue serving the hostname owner")
var errProjectHostDNSContinuity = errors.New("managed hostname route changes require verified, continuity-capable authoritative DNS consumers")
var errProjectHostWorkerTLS = errors.New("managed hostname route changes require all required Edge Workers to support shared hostname TLS")

func (s *Server) handlePlanProjectHostRoutes(w http.ResponseWriter, r *http.Request) {
	var edit model.ProjectHostRouteEdit
	if err := httpx.DecodeJSON(r, &edit); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.projectHostRoutes(w, r, edit, "", false)
}

func (s *Server) handleApplyProjectHostRoutes(w http.ResponseWriter, r *http.Request) {
	var request model.ProjectHostRoutePlan
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if request.ProjectID != r.PathValue("id") || len(request.PlanID) != len("sha256:")+64 || !strings.HasPrefix(request.PlanID, "sha256:") {
		httpx.WriteError(w, http.StatusBadRequest, "a plan for this project is required")
		return
	}
	s.projectHostRoutes(w, r, request.Edit, request.PlanID, true)
}

func (s *Server) handleProjectHostRouteStatus(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	// Like the existing route diagnostics, this reads live consumer evidence
	// across the platform. Keep it off the tenant polling path.
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required for live publication verification")
		return
	}
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	var plan model.ProjectHostRoutePlan
	if err := httpx.DecodeJSON(r, &plan); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if plan.ProjectID != project.ID || plan.Edit.Hostname == "" || normalizeExternalAppDomain(plan.Edit.Hostname) != plan.Edit.Hostname || !validProjectHostRouteEdit(plan.Edit) || len(plan.After) == 0 {
		httpx.WriteError(w, http.StatusBadRequest, "a hostname plan for this project is required")
		return
	}
	status := model.ProjectHostRoutePublicationStatus{State: "pending", Reason: "no exact full global publication with fresh consumer evidence"}
	desired := false
	var unapplied, managed bool
	err := s.store.EditProjectHostRoutes(r.Context(), project.ID, plan.Edit.Hostname, false, func(in store.ProjectHostRouteInputs) (*model.ProjectRouteTable, error) {
		current, _, changed, err := buildProjectHostRoutePlan(in, plan.Edit)
		if err != nil {
			return nil, err
		}
		switch {
		case current.HostnameOwnerAppID != plan.HostnameOwnerAppID:
			status.State, status.Reason = "superseded", "current hostname owner differs from the plan"
		case !changed && maps.Equal(current.Before, plan.After):
			desired = true
		case current.PlanID == plan.PlanID && maps.Equal(current.Before, plan.Before) && maps.Equal(current.After, plan.After):
			status.Reason = "hostname plan has not been applied"
			unapplied = true
			managed = in.Domain.DNSMode == model.AppDomainDNSModeManaged
		default:
			status.State, status.Reason = "superseded", "current desired routes differ from the plan"
		}
		return nil, nil
	})
	if err != nil {
		if errors.Is(err, errProjectHostDomainNotReady) || errors.Is(err, errProjectHostOwnerPath) || errors.Is(err, store.ErrNotFound) {
			status.State, status.Reason = "superseded", "hostname or its target applications are no longer ready"
		} else {
			s.writeStoreError(w, err)
			return
		}
	}
	if status.State == "pending" && desired {
		snapshot, healthy, err := s.publishedRouteDiagnostics(r.Context(), "")
		if err == nil {
			status = projectHostPublicationStatus(plan, snapshot, healthy)
		}
	}
	if r.URL.Query().Get("preflight") == "true" {
		check := model.ProjectHostRoutePreflight{State: "not_applicable", Reason: status.Reason}
		switch {
		case desired:
			check.Reason = "the planned routes are already desired; see publication state"
		case unapplied:
			check = model.ProjectHostRoutePreflight{State: "ready", Reason: "the plan and its current compatibility gates passed; apply rechecks them"}
			if managed {
				dnsErr, workerErr := s.checkProjectHostConsumerCapabilities(r.Context())
				if err := projectHostManagedPreflight(plan, dnsErr, workerErr); err != nil {
					check = model.ProjectHostRoutePreflight{State: "blocked", Reason: err.Error()}
				}
			}
		}
		status.ApplyPreflight = &check
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, status)
}

func projectHostPublicationStatus(plan model.ProjectHostRoutePlan, snapshot model.EdgeRouteIntentSnapshot, healthy map[string]bool) model.ProjectHostRoutePublicationStatus {
	status := model.ProjectHostRoutePublicationStatus{State: "pending", Reason: "no exact full global publication with fresh consumer evidence"}
	if snapshot.TrafficRelease == nil || snapshot.TrafficRelease.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || len(healthy) == 0 || slices.Contains(slices.Collect(maps.Values(healthy)), false) {
		return status
	}
	actual := map[string]string{}
	for _, route := range publishedRouteDiagnosticBindings(snapshot, healthy) {
		if route.Hostname != plan.Edit.Hostname {
			continue
		}
		if route.Status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) || actual[route.PathPrefix] != "" {
			return status
		}
		actual[route.PathPrefix] = route.AppID
	}
	if !maps.Equal(actual, plan.After) {
		return status
	}
	for _, entry := range snapshot.TLSAllowlist {
		if entry.Hostname == plan.Edit.Hostname && entry.Status == model.AppDomainStatusVerified && entry.AppID == plan.HostnameOwnerAppID {
			return model.ProjectHostRoutePublicationStatus{State: "published", Reason: "full global publication matches the planned paths; verify authoritative DNS and public HTTPS separately", ReleaseID: snapshot.TrafficRelease.ReleaseID}
		}
	}
	return status
}

func (s *Server) projectHostRoutes(w http.ResponseWriter, r *http.Request, edit model.ProjectHostRouteEdit, expected string, apply bool) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("project.write") && !principal.HasScope("app.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing project.write or app.write scope")
		return
	}
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	edit.Hostname = normalizeExternalAppDomain(edit.Hostname)
	cfg := s.legacyApplicationDomains()
	if !cfg.managedEdgeCustomDomain(edit.Hostname) || cfg.projectRouteHostnameUsesPlatformTLS(edit.Hostname) || s.isReservedAppHostname(edit.Hostname) || !validProjectHostRouteEdit(edit) {
		httpx.WriteError(w, http.StatusBadRequest, "provide one verified custom hostname and canonical path prefixes with explicit application IDs")
		return
	}
	slices.Sort(edit.RemovePaths)
	var dnsContinuityErr, workerTLSErr error
	if apply {
		// Observe production outside the project write transaction. This is a
		// compatibility gate; fresh signed data-plane probes remain mandatory.
		dnsContinuityErr, workerTLSErr = s.checkProjectHostConsumerCapabilities(r.Context())
	}
	var plan model.ProjectHostRoutePlan
	err := s.store.EditProjectHostRoutes(r.Context(), project.ID, edit.Hostname, apply, func(in store.ProjectHostRouteInputs) (*model.ProjectRouteTable, error) {
		if in.Project.TenantID != project.TenantID {
			return nil, store.ErrNotFound
		}
		var next model.ProjectRouteTable
		var changed bool
		var err error
		plan, next, changed, err = buildProjectHostRoutePlan(in, edit)
		if err != nil || !apply {
			return nil, err
		}
		if !changed {
			plan.PlanID = expected
			plan.State = "already_current"
			return nil, nil
		}
		if plan.PlanID != expected {
			return nil, errProjectHostPlanChanged
		}
		if in.Domain.DNSMode == model.AppDomainDNSModeManaged {
			if err := projectHostManagedPreflight(plan, dnsContinuityErr, workerTLSErr); err != nil {
				return nil, err
			}
		}
		plan.State = "saved"
		return &next, nil
	})
	if err != nil {
		switch {
		case errors.Is(err, errProjectHostPlanChanged), errors.Is(err, errProjectHostDomainNotReady), errors.Is(err, errProjectHostOwnerPath), errors.Is(err, errProjectHostDNSContinuity), errors.Is(err, errProjectHostWorkerTLS):
			httpx.WriteError(w, http.StatusConflict, err.Error())
		default:
			s.writeStoreError(w, err)
		}
		return
	}
	if plan.State == "saved" {
		s.appendAudit(principal, "project.routes.host.apply", "project", project.ID, project.TenantID, map[string]string{"hostname": edit.Hostname, "plan_id": plan.PlanID})
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, plan)
}

func (s *Server) checkProjectHostConsumerCapabilities(ctx context.Context) (dns, worker error) {
	dns = s.checkProjectHostDNSContinuity(ctx)
	if dns == nil {
		worker = s.checkProjectHostWorkerTLS(ctx)
	}
	return dns, worker
}

func projectHostManagedPreflight(plan model.ProjectHostRoutePlan, dnsContinuityErr, workerTLSErr error) error {
	beforeShared := projectHostHasSharedPaths(plan.Before, plan.HostnameOwnerAppID)
	afterShared := projectHostHasSharedPaths(plan.After, plan.HostnameOwnerAppID)
	if (beforeShared || afterShared) && dnsContinuityErr != nil {
		return dnsContinuityErr
	}
	// Reverting to the owner's single-application routing no longer
	// relies on Worker support for shared hostname TLS.
	if afterShared {
		return workerTLSErr
	}
	return nil
}

func projectHostHasSharedPaths(paths map[string]string, owner string) bool {
	for _, app := range paths {
		if app != owner {
			return true
		}
	}
	return false
}

func validProjectHostRouteEdit(edit model.ProjectHostRouteEdit) bool {
	seen := make(map[string]bool)
	add := func(path string) bool {
		key := strings.ToLower(path)
		if path == "" || len(path) > 1024 || strings.ContainsAny(path, "*?#%\\ \t\r\n") || model.NormalizeAppRoutePathPrefix(path) != path || seen[key] {
			return false
		}
		seen[key] = true
		return true
	}
	for path, app := range edit.Paths {
		if app == "" || len(app) > 256 || app != strings.TrimSpace(app) || !add(path) {
			return false
		}
	}
	for _, path := range edit.RemovePaths {
		if !add(path) {
			return false
		}
	}
	return len(seen) > 0 && len(seen) <= 64
}

func buildProjectHostRoutePlan(in store.ProjectHostRouteInputs, edit model.ProjectHostRouteEdit) (model.ProjectHostRoutePlan, model.ProjectRouteTable, bool, error) {
	var plan model.ProjectHostRoutePlan
	domain := in.Domain
	apps := make(map[string]model.App, len(in.Apps))
	inherited := map[string]string{"/": domain.AppID}
	for _, app := range in.Apps {
		if app.TenantID != in.Project.TenantID || app.ProjectID != in.Project.ID {
			continue
		}
		apps[app.ID] = app
		if app.Route != nil && normalizeExternalAppDomain(app.Route.Hostname) == edit.Hostname {
			path := model.NormalizeAppRoutePathPrefix(app.Route.PathPrefix)
			if path == "/" && app.ID != domain.AppID || inherited[path] != "" && inherited[path] != app.ID {
				return plan, in.Table, false, store.ErrConflict
			}
			inherited[path] = app.ID
		}
	}
	if domain.Hostname != edit.Hostname || domain.TenantID != in.Project.TenantID || apps[domain.AppID].ID == "" {
		return plan, in.Table, false, store.ErrNotFound
	}
	if domain.Status != model.AppDomainStatusVerified || domain.DNSStatus != model.AppDomainDNSStatusReady || domain.TLSStatus != model.AppDomainTLSStatusReady {
		return plan, in.Table, false, errProjectHostDomainNotReady
	}
	var dnsIdentity any
	if domain.DNSMode == model.AppDomainDNSModeManaged {
		record := in.DNS
		app, ok := resolveDNSIntentApp(record, apps)
		if in.Zone.Status != model.HostedZoneStatusActive || in.Zone.TenantID != domain.TenantID ||
			!ok || app.ID != domain.AppID || record.Status != model.DNSRecordStatusActive || record.Type != model.DNSRecordTypeFUGUEAPP ||
			record.Source != model.DNSRecordSourceAppDomain || record.SourceRefType != model.DNSRecordSourceRefTypeAppDomain || record.SourceRefID != domain.Hostname ||
			normalizeExternalAppDomain(record.FQDN) != domain.Hostname {
			return plan, in.Table, false, errProjectHostDomainNotReady
		}
		dnsIdentity = []any{record.ID, record.TTL, record.Values, record.FlattenIPv4Policy, record.FlattenIPv6Policy, record.FlattenTTLPolicy, record.FlattenFallbackPolicy}
	}
	for _, id := range edit.Paths {
		app, ok := apps[id]
		if !ok {
			return plan, in.Table, false, store.ErrNotFound
		}
		if app.Spec.Replicas < 1 {
			return plan, in.Table, false, fmt.Errorf("%w: target application is not enabled", store.ErrInvalidInput)
		}
	}
	// App primary routes and domain claims already project independently.
	// Do not persist the synthetic project route view; that could override
	// routes belonging to other hostnames.
	current := model.NormalizeProjectRouteTable(in.Table)
	current.ProjectID, current.TenantID = in.Project.ID, in.Project.TenantID
	original, err := store.CompileProjectRouteTableBindings(current, in.Apps)
	if err != nil {
		return plan, in.Table, false, err
	}
	before := projectHostEffectiveRoutes(edit.Hostname, inherited, original)
	next := model.NormalizeProjectRouteTable(current)
	names := map[string]string{}
	for _, d := range next.Domains {
		if d.Name != "" {
			names[strings.ToLower(d.Name)] = normalizeExternalAppDomain(d.Hostname)
		}
	}
	remove := make(map[string]bool)
	for _, path := range edit.RemovePaths {
		remove[path] = true
	}
	for path := range edit.Paths {
		remove[path] = true
	}
	changed := false
	for i := range next.Entrypoints {
		ep := &next.Entrypoints[i]
		host := names[strings.ToLower(ep.Domain)]
		if host == "" {
			host = normalizeExternalAppDomain(ep.Domain)
		}
		if host == "" && len(next.Domains) == 1 {
			host = normalizeExternalAppDomain(next.Domains[0].Hostname)
		}
		if host != edit.Hostname {
			continue
		}
		ep.Routes = slices.DeleteFunc(ep.Routes, func(route model.ProjectRouteEntrypointRoute) bool {
			if !remove[route.PathPrefix] {
				return false
			}
			if target, set := edit.Paths[route.PathPrefix]; set && target == route.AppID && !route.StripPrefix && route.Rewrite == "" {
				delete(remove, route.PathPrefix)
				return false
			}
			changed = true
			return true
		})
	}
	var additions []model.ProjectRouteEntrypointRoute
	for _, path := range slices.Sorted(maps.Keys(edit.Paths)) {
		if remove[path] {
			additions = append(additions, model.ProjectRouteEntrypointRoute{PathPrefix: path, AppID: edit.Paths[path]})
		}
	}
	if len(additions) != 0 {
		// A domain alias may itself equal a hostname. Refuse to create a
		// route through an alias that resolves to some other domain.
		if host, ok := names[edit.Hostname]; ok && host != edit.Hostname {
			return plan, in.Table, false, store.ErrConflict
		}
		next.Entrypoints = append(next.Entrypoints, model.ProjectRouteEntrypoint{Name: edit.Hostname, Domain: edit.Hostname, Routes: additions})
		changed = true
	}
	bindings, err := store.CompileProjectRouteTableBindings(next, in.Apps)
	if err != nil {
		return plan, in.Table, false, err
	}
	after := projectHostEffectiveRoutes(edit.Hostname, inherited, bindings)
	if !slices.Contains(slices.Collect(maps.Values(after)), domain.AppID) {
		return plan, in.Table, false, errProjectHostOwnerPath
	}
	// Bind only business identity and declared routes; recurring certificate
	// and domain checks update timestamps without changing the plan.
	id, err := platformconfig.Digest([]any{"project-host-routes-v1", current, original, bindings, edit, inherited, domain.AppID, domain.DNSMode, domain.DNSZoneID, domain.DNSRecordID, domain.RouteTarget, domain.CreatedAt, dnsIdentity})
	plan = model.ProjectHostRoutePlan{PlanID: id, ProjectID: in.Project.ID, Edit: edit, HostnameOwnerAppID: domain.AppID, Before: before, After: after, State: "planned"}
	return plan, next, changed, err
}

func projectHostEffectiveRoutes(host string, inherited map[string]string, bindings []model.ProjectRouteBinding) map[string]string {
	out := maps.Clone(inherited)
	for _, b := range bindings {
		if normalizeExternalAppDomain(b.Hostname) == host {
			out[b.PathPrefix] = b.AppID
		}
	}
	return out
}
