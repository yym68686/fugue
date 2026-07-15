package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

type managedPostgresOrphanBackingServiceSummary struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	RuntimeID   string `json:"runtime_id,omitempty"`
	ServiceName string `json:"service_name,omitempty"`
	StorageSize string `json:"storage_size,omitempty"`
	Suspended   bool   `json:"suspended"`
}

type managedPostgresOrphanSummary struct {
	AppID           string                                       `json:"app_id"`
	TenantID        string                                       `json:"tenant_id"`
	ProjectID       string                                       `json:"project_id"`
	Name            string                                       `json:"name"`
	Namespace       string                                       `json:"namespace"`
	ManagedAppName  string                                       `json:"managed_app_name"`
	Phase           string                                       `json:"phase"`
	Message         string                                       `json:"message,omitempty"`
	BackingServices []managedPostgresOrphanBackingServiceSummary `json:"backing_services"`
}

func (s *Server) handleListManagedPostgresOrphans(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform administrator access is required")
		return
	}

	managedApps, err := s.managedAppInventory(r.Context(), false)
	if err != nil {
		s.appendAudit(principal, "backing_service.orphan.list", "managed_postgres_orphans", "", "", map[string]string{"result": "inventory_error"})
		httpx.WriteError(w, http.StatusServiceUnavailable, "managed app inventory is unavailable")
		return
	}
	orphans := make([]managedPostgresOrphanSummary, 0)
	for _, managed := range managedApps {
		summary, _, eligible := managedPostgresOrphanCandidate(managed)
		if !eligible {
			continue
		}
		_, err := s.store.GetApp(summary.AppID)
		switch {
		case err == nil:
			continue
		case errors.Is(err, store.ErrNotFound):
			orphans = append(orphans, summary)
		default:
			s.appendAudit(principal, "backing_service.orphan.list", "managed_postgres_orphans", "", "", map[string]string{"result": "store_error"})
			s.writeStoreError(w, err)
			return
		}
	}
	sort.Slice(orphans, func(i, j int) bool { return orphans[i].AppID < orphans[j].AppID })
	s.appendAudit(principal, "backing_service.orphan.list", "managed_postgres_orphans", "", "", map[string]string{
		"result": "success",
		"count":  strconv.Itoa(len(orphans)),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"orphans": orphans})
}

func (s *Server) handleAdoptManagedPostgresOrphan(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform administrator access is required")
		return
	}
	appID := strings.TrimSpace(r.PathValue("app_id"))
	if appID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "app_id is required")
		return
	}

	managedApps, err := s.managedAppInventory(r.Context(), true)
	if err != nil {
		s.appendAudit(principal, "backing_service.orphan.adopt", "app", appID, "", map[string]string{"result": "inventory_error"})
		httpx.WriteError(w, http.StatusServiceUnavailable, "managed app inventory is unavailable")
		return
	}
	managed, found := managedApps[appID]
	if !found {
		s.appendAudit(principal, "backing_service.orphan.adopt", "app", appID, "", map[string]string{"result": "not_found"})
		s.writeStoreError(w, store.ErrNotFound)
		return
	}
	auditIdentity := managedAppAdoptionAuditIdentity(managed)

	summary, snapshot, validSnapshot := managedPostgresAdoptionSnapshot(managed)
	if !validSnapshot {
		auditIdentity["result"] = "rejected_not_adoptable"
		s.appendAudit(principal, "backing_service.orphan.adopt", "app", appID, strings.TrimSpace(managed.Spec.TenantID), auditIdentity)
		httpx.WriteError(w, http.StatusConflict, "managed app does not contain a valid retained PostgreSQL snapshot")
		return
	}
	verifyExisting := false
	_, getErr := s.store.GetApp(appID)
	switch {
	case getErr == nil:
		verifyExisting = true
	case errors.Is(getErr, store.ErrNotFound):
		if !managedPostgresExplicitlyOrphaned(managed) {
			auditIdentity["result"] = "rejected_not_orphaned"
			s.appendAudit(principal, "backing_service.orphan.adopt", "app", appID, snapshot.TenantID, auditIdentity)
			httpx.WriteError(w, http.StatusConflict, "managed app is not an adoptable disabled PostgreSQL orphan")
			return
		}
		if err := s.verifyManagedPostgresOrphanStorage(r.Context(), managed, snapshot); err != nil {
			if errors.Is(err, errManagedPostgresOrphanStorageUnavailable) {
				auditIdentity["result"] = "storage_evidence_unavailable"
				s.appendAudit(principal, "backing_service.orphan.adopt", "app", appID, snapshot.TenantID, auditIdentity)
				httpx.WriteError(w, http.StatusServiceUnavailable, "managed PostgreSQL storage evidence is unavailable; adoption was not performed")
				return
			}
			auditIdentity["result"] = "rejected_storage_evidence"
			s.appendAudit(principal, "backing_service.orphan.adopt", "app", appID, snapshot.TenantID, auditIdentity)
			httpx.WriteError(w, http.StatusConflict, err.Error())
			return
		}
	default:
		auditIdentity["result"] = "store_error"
		s.appendAudit(principal, "backing_service.orphan.adopt", "app", appID, snapshot.TenantID, auditIdentity)
		s.writeStoreError(w, getErr)
		return
	}

	var (
		app            model.App
		alreadyAdopted bool
	)
	if verifyExisting {
		// This store path is atomic and existing-only: it never inserts or
		// undeletes if the app disappears between the lookup and verification.
		app, err = s.store.VerifyAdoptedOrphanManagedApp(snapshot)
		alreadyAdopted = err == nil
	} else {
		app, alreadyAdopted, err = s.store.AdoptOrphanManagedApp(snapshot)
	}
	if err != nil {
		auditIdentity["result"] = lifecycleStoreErrorResult(err)
		s.appendAudit(principal, "backing_service.orphan.adopt", "app", appID, snapshot.TenantID, auditIdentity)
		s.writeStoreError(w, err)
		return
	}
	app, err = s.hydrateAdoptedOrphanBackingServices(app)
	if err != nil {
		auditIdentity["result"] = "hydrate_error"
		s.appendAudit(principal, "backing_service.orphan.adopt", "app", appID, snapshot.TenantID, auditIdentity)
		s.writeStoreError(w, err)
		return
	}

	result := "adopted"
	statusCode := http.StatusCreated
	if alreadyAdopted {
		result = "already_adopted"
		statusCode = http.StatusOK
	}
	auditIdentity["result"] = result
	auditIdentity["managed_app_name"] = summary.ManagedAppName
	auditIdentity["namespace"] = summary.Namespace
	auditIdentity["service_count"] = strconv.Itoa(len(app.BackingServices))
	s.appendAudit(principal, "backing_service.orphan.adopt", "app", app.ID, app.TenantID, auditIdentity)

	safeApp := redactAppForDebugBundle(app)
	httpx.WriteJSON(w, statusCode, map[string]any{
		"app":              safeApp,
		"backing_services": redactBackingServicesForDebugBundle(app.BackingServices),
		"already_adopted":  alreadyAdopted,
	})
}

func managedPostgresOrphanCandidate(managed runtime.ManagedAppObject) (managedPostgresOrphanSummary, model.App, bool) {
	summary, snapshot, valid := managedPostgresAdoptionSnapshot(managed)
	if !valid || !managedPostgresExplicitlyOrphaned(managed) {
		return managedPostgresOrphanSummary{}, snapshot, false
	}
	return summary, snapshot, true
}

func managedPostgresExplicitlyOrphaned(managed runtime.ManagedAppObject) bool {
	message := strings.ToLower(strings.TrimSpace(managed.Status.Message))
	return strings.EqualFold(strings.TrimSpace(managed.Status.Phase), runtime.ManagedAppPhaseDisabled) &&
		strings.Contains(message, "orphaned managed app: app not found in store") &&
		strings.Contains(message, "retained storage") &&
		managed.Metadata.Generation > 0 &&
		managed.Status.ObservedGeneration == managed.Metadata.Generation &&
		managedPostgresOrphanWorkloadZeroVerified(managed.Status.Conditions)
}

func managedPostgresOrphanWorkloadZeroVerified(conditions []runtime.ManagedAppCondition) bool {
	for _, condition := range conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), "OrphanWorkloadZero") &&
			strings.EqualFold(strings.TrimSpace(condition.Status), "True") &&
			strings.EqualFold(strings.TrimSpace(condition.Reason), "Verified") {
			return true
		}
	}
	return false
}

func managedPostgresAdoptionSnapshot(managed runtime.ManagedAppObject) (managedPostgresOrphanSummary, model.App, bool) {
	snapshot := runtime.AppFromManagedApp(managed)
	message := strings.TrimSpace(managed.Status.Message)
	identityMatches := strings.TrimSpace(managed.APIVersion) == runtime.ManagedAppAPIVersion &&
		strings.TrimSpace(managed.Kind) == runtime.ManagedAppKind &&
		strings.TrimSpace(managed.Metadata.Namespace) == runtime.NamespaceForTenant(snapshot.TenantID) &&
		strings.TrimSpace(managed.Metadata.Name) == runtime.ManagedAppResourceName(snapshot) &&
		strings.TrimSpace(managed.Metadata.UID) != "" &&
		strings.TrimSpace(managed.Metadata.ResourceVersion) != "" &&
		strings.TrimSpace(managed.Metadata.DeletionTimestamp) == ""
	if !identityMatches || snapshot.ID == "" || snapshot.TenantID == "" || snapshot.ProjectID == "" || snapshot.Name == "" || strings.TrimSpace(snapshot.Spec.RuntimeID) == "" || len(snapshot.BackingServices) == 0 || len(snapshot.Bindings) == 0 {
		return managedPostgresOrphanSummary{}, snapshot, false
	}

	serviceSummaries := make([]managedPostgresOrphanBackingServiceSummary, 0)
	servicesByID := make(map[string]model.BackingService, len(snapshot.BackingServices))
	serviceNames := make(map[string]struct{}, len(snapshot.BackingServices))
	for _, service := range snapshot.BackingServices {
		ownerAppID := strings.TrimSpace(service.OwnerAppID)
		serviceID := strings.TrimSpace(service.ID)
		serviceName := strings.TrimSpace(service.Name)
		if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) ||
			!strings.EqualFold(strings.TrimSpace(service.Provisioner), model.BackingServiceProvisionerManaged) ||
			service.Spec.Postgres == nil ||
			serviceID == "" ||
			serviceName == "" ||
			strings.TrimSpace(service.TenantID) != snapshot.TenantID ||
			strings.TrimSpace(service.ProjectID) != snapshot.ProjectID ||
			(ownerAppID != "" && ownerAppID != snapshot.ID) ||
			strings.TrimSpace(service.Spec.Postgres.Password) == "" {
			return managedPostgresOrphanSummary{}, snapshot, false
		}
		if _, exists := servicesByID[serviceID]; exists {
			return managedPostgresOrphanSummary{}, snapshot, false
		}
		nameKey := strings.ToLower(serviceName)
		if _, exists := serviceNames[nameKey]; exists {
			return managedPostgresOrphanSummary{}, snapshot, false
		}
		servicesByID[serviceID] = service
		serviceNames[nameKey] = struct{}{}
		postgres := service.Spec.Postgres
		serviceSummaries = append(serviceSummaries, managedPostgresOrphanBackingServiceSummary{
			ID:          serviceID,
			Name:        serviceName,
			Type:        model.BackingServiceTypePostgres,
			RuntimeID:   strings.TrimSpace(postgres.RuntimeID),
			ServiceName: strings.TrimSpace(postgres.ServiceName),
			StorageSize: strings.TrimSpace(postgres.StorageSize),
			Suspended:   postgres.Suspended,
		})
	}
	boundServiceIDs := make(map[string]struct{}, len(snapshot.Bindings))
	bindingIDs := make(map[string]struct{}, len(snapshot.Bindings))
	for _, binding := range snapshot.Bindings {
		bindingID := strings.TrimSpace(binding.ID)
		serviceID := strings.TrimSpace(binding.ServiceID)
		if bindingID == "" ||
			strings.TrimSpace(binding.TenantID) != snapshot.TenantID ||
			strings.TrimSpace(binding.AppID) != snapshot.ID {
			return managedPostgresOrphanSummary{}, snapshot, false
		}
		if _, exists := servicesByID[serviceID]; !exists {
			return managedPostgresOrphanSummary{}, snapshot, false
		}
		if _, exists := bindingIDs[bindingID]; exists {
			return managedPostgresOrphanSummary{}, snapshot, false
		}
		if _, exists := boundServiceIDs[serviceID]; exists {
			return managedPostgresOrphanSummary{}, snapshot, false
		}
		bindingIDs[bindingID] = struct{}{}
		boundServiceIDs[serviceID] = struct{}{}
	}
	if len(serviceSummaries) == 0 || len(boundServiceIDs) != len(servicesByID) {
		return managedPostgresOrphanSummary{}, snapshot, false
	}
	for serviceID, service := range servicesByID {
		if strings.TrimSpace(service.OwnerAppID) == "" {
			if _, bound := boundServiceIDs[serviceID]; !bound {
				return managedPostgresOrphanSummary{}, snapshot, false
			}
		}
	}
	sort.Slice(serviceSummaries, func(i, j int) bool { return serviceSummaries[i].ID < serviceSummaries[j].ID })
	return managedPostgresOrphanSummary{
		AppID:           snapshot.ID,
		TenantID:        snapshot.TenantID,
		ProjectID:       snapshot.ProjectID,
		Name:            snapshot.Name,
		Namespace:       strings.TrimSpace(managed.Metadata.Namespace),
		ManagedAppName:  strings.TrimSpace(managed.Metadata.Name),
		Phase:           strings.TrimSpace(managed.Status.Phase),
		Message:         message,
		BackingServices: serviceSummaries,
	}, snapshot, true
}

func managedAppAdoptionAuditIdentity(managed runtime.ManagedAppObject) map[string]string {
	metadata := map[string]string{
		"managed_app_name": strings.TrimSpace(managed.Metadata.Name),
		"namespace":        strings.TrimSpace(managed.Metadata.Namespace),
	}
	if uid := strings.TrimSpace(managed.Metadata.UID); uid != "" {
		metadata["managed_app_uid"] = uid
	}
	if resourceVersion := strings.TrimSpace(managed.Metadata.ResourceVersion); resourceVersion != "" {
		metadata["managed_app_resource_version"] = resourceVersion
	}
	return metadata
}

var errManagedPostgresOrphanStorageUnavailable = errors.New("managed PostgreSQL orphan storage evidence unavailable")

type managedPostgresOrphanKubeMetadata struct {
	Name              string            `json:"name,omitempty"`
	UID               string            `json:"uid,omitempty"`
	Generation        int64             `json:"generation,omitempty"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	OwnerReferences   []struct {
		UID string `json:"uid,omitempty"`
	} `json:"ownerReferences,omitempty"`
}

type managedPostgresOrphanCluster struct {
	Metadata managedPostgresOrphanKubeMetadata `json:"metadata"`
}

type managedPostgresOrphanPVCList struct {
	Items []struct {
		Metadata managedPostgresOrphanKubeMetadata `json:"metadata"`
		Status   struct {
			Phase string `json:"phase,omitempty"`
		} `json:"status"`
	} `json:"items"`
}

type managedPostgresOrphanDeployment struct {
	Metadata managedPostgresOrphanKubeMetadata `json:"metadata"`
	Spec     struct {
		Replicas int `json:"replicas"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration  int64 `json:"observedGeneration"`
		Replicas            int   `json:"replicas"`
		UpdatedReplicas     int   `json:"updatedReplicas"`
		ReadyReplicas       int   `json:"readyReplicas"`
		AvailableReplicas   int   `json:"availableReplicas"`
		UnavailableReplicas int   `json:"unavailableReplicas"`
	} `json:"status"`
}

type managedPostgresOrphanPodList struct {
	Items []struct {
		Metadata managedPostgresOrphanKubeMetadata `json:"metadata"`
	} `json:"items"`
}

func (s *Server) verifyManagedPostgresOrphanStorage(ctx context.Context, managed runtime.ManagedAppObject, snapshot model.App) error {
	deployments := runtime.ManagedBackingServiceDeployments(snapshot, managed.Spec.Scheduling)
	if len(deployments) == 0 || len(deployments) != len(snapshot.BackingServices) {
		return errors.New("retained managed PostgreSQL snapshot does not map exactly to runtime clusters")
	}
	client, err := s.managedAppStatusClient()
	if err != nil {
		return fmt.Errorf("%w: %v", errManagedPostgresOrphanStorageUnavailable, err)
	}
	defer client.closeIdleConnections()
	refreshCtx, cancel := s.managedAppStatusRefreshContext(ctx)
	defer cancel()

	namespace := strings.TrimSpace(managed.Metadata.Namespace)
	managedUID := strings.TrimSpace(managed.Metadata.UID)
	managedPath := "/apis/" + runtime.ManagedAppAPIGroup + "/v1alpha1/namespaces/" + url.PathEscape(namespace) + "/" + runtime.ManagedAppPlural + "/" + url.PathEscape(strings.TrimSpace(managed.Metadata.Name))
	var currentManaged runtime.ManagedAppObject
	if err := client.doJSON(refreshCtx, managedPath, &currentManaged); err != nil {
		if isKubeNotFound(err) {
			return errors.New("managed app disappeared before storage evidence verification")
		}
		return fmt.Errorf("%w: re-read managed app identity: %v", errManagedPostgresOrphanStorageUnavailable, err)
	}
	if strings.TrimSpace(currentManaged.Metadata.UID) != managedUID ||
		strings.TrimSpace(currentManaged.Metadata.ResourceVersion) != strings.TrimSpace(managed.Metadata.ResourceVersion) ||
		currentManaged.Metadata.Generation != managed.Metadata.Generation ||
		strings.TrimSpace(currentManaged.Metadata.DeletionTimestamp) != "" ||
		runtime.ManagedAppSpecHash(currentManaged.Spec) != runtime.ManagedAppSpecHash(managed.Spec) {
		return errors.New("managed app changed or began deleting during storage evidence verification")
	}
	deploymentName := runtime.RuntimeAppResourceName(snapshot)
	deploymentPath := "/apis/apps/v1/namespaces/" + url.PathEscape(namespace) + "/deployments/" + url.PathEscape(deploymentName)
	var deployment managedPostgresOrphanDeployment
	if err := client.doJSON(refreshCtx, deploymentPath, &deployment); err != nil {
		if !isKubeNotFound(err) {
			return fmt.Errorf("%w: query app deployment %s: %v", errManagedPostgresOrphanStorageUnavailable, deploymentName, err)
		}
	} else {
		ownerMatches := false
		for _, owner := range deployment.Metadata.OwnerReferences {
			if strings.TrimSpace(owner.UID) == managedUID {
				ownerMatches = true
				break
			}
		}
		if strings.TrimSpace(deployment.Metadata.UID) == "" ||
			strings.TrimSpace(deployment.Metadata.DeletionTimestamp) != "" ||
			!ownerMatches ||
			deployment.Spec.Replicas != 0 ||
			deployment.Metadata.Generation <= 0 ||
			deployment.Status.ObservedGeneration < deployment.Metadata.Generation ||
			deployment.Status.Replicas != 0 ||
			deployment.Status.UpdatedReplicas != 0 ||
			deployment.Status.ReadyReplicas != 0 ||
			deployment.Status.AvailableReplicas != 0 ||
			deployment.Status.UnavailableReplicas != 0 {
			return fmt.Errorf("managed app deployment %s is not verified at zero replicas", deploymentName)
		}
	}

	podSelector := url.QueryEscape(runtime.FugueLabelAppID + "=" + snapshot.ID)
	podsPath := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods?labelSelector=" + podSelector
	var pods managedPostgresOrphanPodList
	if err := client.doJSON(refreshCtx, podsPath, &pods); err != nil {
		if !isKubeNotFound(err) {
			return fmt.Errorf("%w: query app pods for %s: %v", errManagedPostgresOrphanStorageUnavailable, snapshot.ID, err)
		}
	} else {
		for _, pod := range pods.Items {
			if strings.TrimSpace(pod.Metadata.Labels[runtime.FugueLabelAppID]) == snapshot.ID {
				return fmt.Errorf("managed app %s still has a matching pod, including terminating pods", snapshot.ID)
			}
		}
	}

	seenClusters := make(map[string]struct{}, len(deployments))
	for _, deployment := range deployments {
		clusterName := strings.TrimSpace(deployment.ResourceName)
		if deployment.ResourceKind != runtime.CloudNativePGClusterKind || clusterName == "" {
			return errors.New("retained managed PostgreSQL snapshot has an invalid runtime cluster identity")
		}
		if _, exists := seenClusters[clusterName]; exists {
			return errors.New("retained managed PostgreSQL snapshot maps multiple services to one runtime cluster")
		}
		seenClusters[clusterName] = struct{}{}

		clusterPath := "/apis/postgresql.cnpg.io/v1/namespaces/" + url.PathEscape(namespace) + "/clusters/" + url.PathEscape(clusterName)
		var cluster managedPostgresOrphanCluster
		if err := client.doJSON(refreshCtx, clusterPath, &cluster); err != nil {
			if isKubeNotFound(err) {
				return fmt.Errorf("retained managed PostgreSQL cluster %s was not found", clusterName)
			}
			return fmt.Errorf("%w: query cluster %s: %v", errManagedPostgresOrphanStorageUnavailable, clusterName, err)
		}
		if strings.TrimSpace(cluster.Metadata.UID) == "" || strings.TrimSpace(cluster.Metadata.DeletionTimestamp) != "" {
			return fmt.Errorf("retained managed PostgreSQL cluster %s is missing identity or is deleting", clusterName)
		}
		ownerMatches := false
		for _, owner := range cluster.Metadata.OwnerReferences {
			if strings.TrimSpace(owner.UID) == managedUID {
				ownerMatches = true
				break
			}
		}
		if !ownerMatches {
			return fmt.Errorf("retained managed PostgreSQL cluster %s is not owned by the managed app", clusterName)
		}

		selector := url.QueryEscape("cnpg.io/cluster=" + clusterName)
		pvcPath := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/persistentvolumeclaims?labelSelector=" + selector
		var pvcs managedPostgresOrphanPVCList
		if err := client.doJSON(refreshCtx, pvcPath, &pvcs); err != nil {
			if isKubeNotFound(err) {
				return fmt.Errorf("retained managed PostgreSQL cluster %s has no persistent volume claims", clusterName)
			}
			return fmt.Errorf("%w: query persistent volume claims for %s: %v", errManagedPostgresOrphanStorageUnavailable, clusterName, err)
		}
		boundPVCFound := false
		for _, pvc := range pvcs.Items {
			if strings.TrimSpace(pvc.Metadata.DeletionTimestamp) != "" ||
				!strings.EqualFold(strings.TrimSpace(pvc.Status.Phase), "Bound") ||
				strings.TrimSpace(pvc.Metadata.Labels["cnpg.io/cluster"]) != clusterName {
				continue
			}
			boundPVCFound = true
			break
		}
		if !boundPVCFound {
			return fmt.Errorf("retained managed PostgreSQL cluster %s has no non-deleting bound persistent volume claim", clusterName)
		}
	}
	return nil
}

func (s *Server) hydrateAdoptedOrphanBackingServices(app model.App) (model.App, error) {
	if len(app.BackingServices) > 0 {
		return app, nil
	}
	services, err := s.store.ListBackingServices(app.TenantID, false)
	if err != nil {
		return model.App{}, err
	}
	boundIDs := make(map[string]struct{}, len(app.Bindings))
	for _, binding := range app.Bindings {
		boundIDs[strings.TrimSpace(binding.ServiceID)] = struct{}{}
	}
	for _, service := range services {
		_, bound := boundIDs[strings.TrimSpace(service.ID)]
		if strings.TrimSpace(service.OwnerAppID) != strings.TrimSpace(app.ID) && !bound {
			continue
		}
		app.BackingServices = append(app.BackingServices, cloneBackingService(service))
	}
	return app, nil
}
