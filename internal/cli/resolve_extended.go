package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

func filterProjects(projects []model.Project, tenantID string) []model.Project {
	if strings.TrimSpace(tenantID) == "" {
		return append([]model.Project(nil), projects...)
	}
	out := make([]model.Project, 0, len(projects))
	for _, project := range projects {
		if project.TenantID == tenantID {
			out = append(out, project)
		}
	}
	return out
}

func filterServices(services []model.BackingService, tenantID, projectID string) []model.BackingService {
	out := make([]model.BackingService, 0, len(services))
	for _, service := range services {
		if strings.TrimSpace(tenantID) != "" && service.TenantID != tenantID {
			continue
		}
		if strings.TrimSpace(projectID) != "" && service.ProjectID != projectID {
			continue
		}
		out = append(out, service)
	}
	return out
}

func filterAPIKeys(keys []model.APIKey, tenantID string) []model.APIKey {
	if strings.TrimSpace(tenantID) == "" {
		return append([]model.APIKey(nil), keys...)
	}
	out := make([]model.APIKey, 0, len(keys))
	for _, key := range keys {
		if key.TenantID == tenantID {
			out = append(out, key)
		}
	}
	return out
}

func filterNodeKeys(keys []model.NodeKey, tenantID string) []model.NodeKey {
	if strings.TrimSpace(tenantID) == "" {
		return append([]model.NodeKey(nil), keys...)
	}
	out := make([]model.NodeKey, 0, len(keys))
	for _, key := range keys {
		if key.TenantID == tenantID {
			out = append(out, key)
		}
	}
	return out
}

func matchVisibleProjects(projects []model.Project, ref string) []model.Project {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	slug := model.Slugify(ref)
	matches := make([]model.Project, 0, 1)
	for _, project := range projects {
		switch {
		case strings.EqualFold(project.ID, ref):
			matches = append(matches, project)
		case strings.EqualFold(project.Name, ref):
			matches = append(matches, project)
		case slug != "" && strings.EqualFold(project.Slug, slug):
			matches = append(matches, project)
		}
	}
	return matches
}

func matchVisibleServices(services []model.BackingService, ref string) []model.BackingService {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	slug := model.Slugify(ref)
	matches := make([]model.BackingService, 0, 1)
	for _, service := range services {
		switch {
		case strings.EqualFold(service.ID, ref):
			matches = append(matches, service)
		case strings.EqualFold(service.Name, ref):
			matches = append(matches, service)
		case slug != "" && strings.EqualFold(model.Slugify(service.Name), slug):
			matches = append(matches, service)
		}
	}
	return matches
}

func matchVisibleAPIKeys(keys []model.APIKey, ref string) []model.APIKey {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	matches := make([]model.APIKey, 0, 1)
	for _, key := range keys {
		switch {
		case strings.EqualFold(key.ID, ref):
			matches = append(matches, key)
		case strings.EqualFold(key.Label, ref):
			matches = append(matches, key)
		case strings.EqualFold(key.Prefix, ref):
			matches = append(matches, key)
		}
	}
	return matches
}

func matchVisibleNodeKeys(keys []model.NodeKey, ref string) []model.NodeKey {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	matches := make([]model.NodeKey, 0, 1)
	for _, key := range keys {
		switch {
		case strings.EqualFold(key.ID, ref):
			matches = append(matches, key)
		case strings.EqualFold(key.Label, ref):
			matches = append(matches, key)
		case strings.EqualFold(key.Prefix, ref):
			matches = append(matches, key)
		}
	}
	return matches
}

func resolveSingleMatch[T any](ref string, matches []T, kind string) (T, error) {
	var zero T
	switch len(matches) {
	case 0:
		return zero, fmt.Errorf("%s %q not found", kind, strings.TrimSpace(ref))
	case 1:
		return matches[0], nil
	default:
		return zero, fmt.Errorf("multiple %ss match %q", kind, strings.TrimSpace(ref))
	}
}

func (c *CLI) resolveNamedProject(client *Client, ref string) (model.Project, error) {
	tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
	if err != nil {
		return model.Project{}, err
	}
	projects, err := client.ListProjects(tenantID)
	if err != nil {
		return model.Project{}, err
	}
	return resolveSingleMatch(ref, matchVisibleProjects(projects, ref), "project")
}

func (c *CLI) resolveNamedService(client *Client, ref string) (model.BackingService, error) {
	tenantID, projectID, err := c.resolveFilterSelections(client)
	if err != nil {
		return model.BackingService{}, err
	}
	services, err := client.ListBackingServices()
	if err != nil {
		return model.BackingService{}, err
	}
	return resolveSingleMatch(ref, matchVisibleServices(filterServices(services, tenantID, projectID), ref), "service")
}

func (c *CLI) resolveNamedRuntime(client *Client, ref string) (model.Runtime, error) {
	runtimeID, err := resolveRuntimeSelection(client, "", ref)
	if err != nil {
		return model.Runtime{}, err
	}
	if strings.TrimSpace(runtimeID) == "" {
		return model.Runtime{}, fmt.Errorf("runtime is required")
	}
	return client.GetRuntime(runtimeID)
}

func (c *CLI) resolveNamedAPIKey(client *Client, ref string) (model.APIKey, error) {
	tenantID := c.effectiveTenantID()
	if strings.TrimSpace(tenantID) == "" && strings.TrimSpace(c.effectiveTenantName()) != "" {
		var err error
		tenantID, err = resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
		if err != nil {
			return model.APIKey{}, err
		}
	}
	keys, err := client.ListAPIKeys()
	if err != nil {
		return model.APIKey{}, err
	}
	return resolveSingleMatch(ref, matchVisibleAPIKeys(filterAPIKeys(keys, tenantID), ref), "api key")
}

func (c *CLI) resolveNamedNodeKey(client *Client, ref string) (model.NodeKey, error) {
	tenantID := c.effectiveTenantID()
	if strings.TrimSpace(tenantID) == "" && strings.TrimSpace(c.effectiveTenantName()) != "" {
		var err error
		tenantID, err = resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
		if err != nil {
			return model.NodeKey{}, err
		}
	}
	keys, err := client.ListNodeKeys()
	if err != nil {
		return model.NodeKey{}, err
	}
	return resolveSingleMatch(ref, matchVisibleNodeKeys(filterNodeKeys(keys, tenantID), ref), "node key")
}

func (c *CLI) resolveNamedTenant(client *Client, ref string) (model.Tenant, error) {
	tenants, err := client.ListTenants()
	if err != nil {
		return model.Tenant{}, err
	}
	slug := model.Slugify(ref)
	matches := make([]model.Tenant, 0, 1)
	for _, tenant := range tenants {
		switch {
		case strings.EqualFold(tenant.ID, ref):
			matches = append(matches, tenant)
		case strings.EqualFold(tenant.Name, ref):
			matches = append(matches, tenant)
		case slug != "" && strings.EqualFold(tenant.Slug, slug):
			matches = append(matches, tenant)
		}
	}
	return resolveSingleMatch(ref, matches, "tenant")
}

func (c *CLI) visibleTenantNamesByID(client *Client) (map[string]string, error) {
	tenants, err := client.ListTenants()
	if err != nil {
		return nil, err
	}
	names := make(map[string]string, len(tenants))
	for _, tenant := range tenants {
		name := strings.TrimSpace(tenant.Name)
		if name == "" {
			name = strings.TrimSpace(tenant.Slug)
		}
		if name == "" {
			name = strings.TrimSpace(tenant.ID)
		}
		names[strings.TrimSpace(tenant.ID)] = name
	}
	return names, nil
}

func resolveDefaultProjectForCreate(client *Client, tenantID string) (model.Project, bool, error) {
	projects, err := client.ListProjects(tenantID)
	if err != nil {
		return model.Project{}, false, err
	}
	if len(projects) == 1 {
		return projects[0], true, nil
	}
	for _, project := range projects {
		if strings.EqualFold(project.Name, "default") || strings.EqualFold(project.Slug, "default") {
			return project, true, nil
		}
	}
	return model.Project{}, false, nil
}
