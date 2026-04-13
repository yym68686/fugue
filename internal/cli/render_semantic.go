package cli

import (
	"io"
	"strconv"
	"strings"

	"fugue/internal/model"
)

func (c *CLI) renderAppStatus(client *Client, app model.App) error {
	tenantNames := c.loadTenantNames(client)
	projectNames := c.loadProjectNames(client, app.TenantID)
	runtimeNames := c.loadRuntimeNames(client)
	return writeAppStatusWithContext(c.stdout, app, tenantNames, projectNames, runtimeNames, c.showIDs())
}

func (c *CLI) renderProjectDetail(client *Client, project model.Project) error {
	return renderProjectWithContext(c.stdout, project, c.loadTenantNames(client), c.showIDs())
}

func (c *CLI) renderRuntimeDetail(client *Client, runtimeObj model.Runtime) error {
	return renderRuntimeWithContext(c.stdout, runtimeObj, c.loadTenantNames(client), c.showIDs())
}

func (c *CLI) renderBackingServiceDetail(client *Client, service model.BackingService) error {
	projectNames := c.loadProjectNames(client, service.TenantID)
	var appNames map[string]string
	if strings.TrimSpace(service.OwnerAppID) != "" {
		appNames = c.loadAppNames(client)
	}
	var runtimeNames map[string]string
	if service.Spec.Postgres != nil && strings.TrimSpace(service.Spec.Postgres.RuntimeID) != "" {
		runtimeNames = c.loadRuntimeNames(client)
	}
	return renderBackingServiceWithContext(c.stdout, service, projectNames, appNames, runtimeNames, c.showIDs())
}

func (c *CLI) loadTenantNames(client *Client) map[string]string {
	tenantNames, err := c.visibleTenantNamesByID(client)
	if err != nil {
		c.progressf("warning=tenant inventory unavailable: %v", err)
		return nil
	}
	return tenantNames
}

func (c *CLI) loadProjectNames(client *Client, tenantID string) map[string]string {
	projects, err := client.ListProjects(strings.TrimSpace(tenantID))
	if err != nil {
		c.progressf("warning=project inventory unavailable: %v", err)
		return nil
	}
	return mapProjectNamesByID(projects)
}

func (c *CLI) loadRuntimeNames(client *Client) map[string]string {
	runtimes, err := client.ListRuntimes()
	if err != nil {
		c.progressf("warning=runtime inventory unavailable: %v", err)
		return nil
	}
	return mapRuntimeNames(runtimes)
}

func (c *CLI) loadAppNames(client *Client) map[string]string {
	apps, err := client.ListApps()
	if err != nil {
		c.progressf("warning=app inventory unavailable: %v", err)
		return nil
	}
	return mapAppNames(apps)
}

func renderProjectWithContext(w io.Writer, project model.Project, tenantNames map[string]string, showIDs bool) error {
	tenantName := firstNonEmptyTrimmed(tenantNames[project.TenantID], project.TenantID)
	return writeKeyValues(w,
		kvPair{Key: "project", Value: formatDisplayName(project.Name, project.ID, showIDs)},
		kvPair{Key: "tenant", Value: formatDisplayName(tenantName, project.TenantID, showIDs)},
		kvPair{Key: "slug", Value: project.Slug},
		kvPair{Key: "description", Value: project.Description},
		kvPair{Key: "updated_at", Value: formatTime(project.UpdatedAt)},
	)
}

func renderRuntimeWithContext(w io.Writer, runtimeObj model.Runtime, tenantNames map[string]string, showIDs bool) error {
	tenantName := firstNonEmptyTrimmed(tenantNames[runtimeObj.TenantID], runtimeObj.TenantID)
	publicOffer := "hidden"
	if runtimeObj.PublicOffer != nil {
		publicOffer = "published"
	}
	return writeKeyValues(w,
		kvPair{Key: "runtime", Value: formatDisplayName(runtimeObj.Name, runtimeObj.ID, showIDs)},
		kvPair{Key: "tenant", Value: formatDisplayName(tenantName, runtimeObj.TenantID, showIDs)},
		kvPair{Key: "type", Value: runtimeObj.Type},
		kvPair{Key: "access_mode", Value: runtimeObj.AccessMode},
		kvPair{Key: "pool_mode", Value: runtimeObj.PoolMode},
		kvPair{Key: "status", Value: runtimeObj.Status},
		kvPair{Key: "connection_mode", Value: runtimeObj.ConnectionMode},
		kvPair{Key: "public_offer", Value: publicOffer},
		kvPair{Key: "endpoint", Value: runtimeObj.Endpoint},
		kvPair{Key: "cluster_node", Value: runtimeObj.ClusterNodeName},
		kvPair{Key: "last_seen_at", Value: formatOptionalTimePtr(runtimeObj.LastSeenAt)},
		kvPair{Key: "updated_at", Value: formatTime(runtimeObj.UpdatedAt)},
	)
}

func renderBackingServiceWithContext(w io.Writer, service model.BackingService, projectNames, appNames, runtimeNames map[string]string, showIDs bool) error {
	projectName := firstNonEmptyTrimmed(projectNames[service.ProjectID], service.ProjectID)
	ownerAppName := firstNonEmptyTrimmed(appNames[service.OwnerAppID], service.OwnerAppID)
	runtimeID := ""
	if service.Spec.Postgres != nil {
		runtimeID = strings.TrimSpace(service.Spec.Postgres.RuntimeID)
	}
	runtimeName := firstNonEmptyTrimmed(runtimeNames[runtimeID], runtimeID)
	pairs := []kvPair{
		{Key: "service", Value: formatDisplayName(service.Name, service.ID, showIDs)},
		{Key: "project", Value: formatDisplayName(projectName, service.ProjectID, showIDs)},
		{Key: "owner_app", Value: formatDisplayName(ownerAppName, service.OwnerAppID, showIDs)},
		{Key: "type", Value: service.Type},
		{Key: "provisioner", Value: service.Provisioner},
		{Key: "status", Value: service.Status},
		{Key: "description", Value: service.Description},
		{Key: "runtime", Value: formatDisplayName(runtimeName, runtimeID, showIDs)},
		{Key: "current_resource_usage", Value: formatResourceUsageSummary(service.CurrentResourceUsage)},
		{Key: "updated_at", Value: formatTime(service.UpdatedAt)},
	}
	if service.Spec.Postgres != nil {
		pairs = append(pairs,
			kvPair{Key: "database", Value: service.Spec.Postgres.Database},
			kvPair{Key: "user", Value: service.Spec.Postgres.User},
			kvPair{Key: "service_name", Value: service.Spec.Postgres.ServiceName},
			kvPair{Key: "storage_size", Value: service.Spec.Postgres.StorageSize},
		)
		if service.Spec.Postgres.Instances > 0 {
			pairs = append(pairs, kvPair{Key: "instances", Value: formatInt(service.Spec.Postgres.Instances)})
		}
		if service.Spec.Postgres.SynchronousReplicas > 0 {
			pairs = append(pairs, kvPair{Key: "sync_replicas", Value: formatInt(service.Spec.Postgres.SynchronousReplicas)})
		}
	}
	return writeKeyValues(w, pairs...)
}

func formatInt(value int) string {
	return strconv.Itoa(value)
}
