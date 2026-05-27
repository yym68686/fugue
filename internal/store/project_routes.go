package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) GetProjectRouteTable(projectID string) (model.ProjectRouteTable, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return model.ProjectRouteTable{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetProjectRouteTable(projectID)
	}
	var table model.ProjectRouteTable
	err := s.withLockedState(false, func(state *model.State) error {
		for _, candidate := range state.ProjectRouteTables {
			if strings.TrimSpace(candidate.ProjectID) == projectID {
				table = cloneProjectRouteTable(candidate)
				return nil
			}
		}
		return ErrNotFound
	})
	return table, err
}

func (s *Store) ListProjectRouteTables(tenantID string, platformAdmin bool) ([]model.ProjectRouteTable, error) {
	tenantID = strings.TrimSpace(tenantID)
	if s.usingDatabase() {
		return s.pgListProjectRouteTables(tenantID, platformAdmin)
	}
	var tables []model.ProjectRouteTable
	err := s.withLockedState(false, func(state *model.State) error {
		for _, table := range state.ProjectRouteTables {
			if !platformAdmin && strings.TrimSpace(table.TenantID) != tenantID {
				continue
			}
			if platformAdmin || tenantID == "" || strings.TrimSpace(table.TenantID) == tenantID {
				tables = append(tables, cloneProjectRouteTable(table))
			}
		}
		sortProjectRouteTables(tables)
		return nil
	})
	return tables, err
}

func (s *Store) PutProjectRouteTable(projectID string, table model.ProjectRouteTable) (model.ProjectRouteTable, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return model.ProjectRouteTable{}, ErrInvalidInput
	}
	table.ProjectID = projectID
	table = model.NormalizeProjectRouteTable(table)
	if s.usingDatabase() {
		return s.pgPutProjectRouteTable(projectID, table)
	}

	var saved model.ProjectRouteTable
	err := s.withLockedState(true, func(state *model.State) error {
		projectIndex := findProject(state, projectID)
		if projectIndex < 0 {
			return ErrNotFound
		}
		project := state.Projects[projectIndex]
		if strings.TrimSpace(table.TenantID) != "" && strings.TrimSpace(table.TenantID) != project.TenantID {
			return ErrInvalidInput
		}
		table.TenantID = project.TenantID
		apps := projectRouteAppsForProject(state.Apps, projectID)
		bindings, err := CompileProjectRouteTableBindings(table, apps)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		index := findProjectRouteTable(state.ProjectRouteTables, projectID)
		if index >= 0 {
			table.CreatedAt = state.ProjectRouteTables[index].CreatedAt
		} else {
			table.CreatedAt = now
		}
		table.UpdatedAt = now
		if index >= 0 {
			state.ProjectRouteTables[index] = cloneProjectRouteTable(table)
		} else {
			state.ProjectRouteTables = append(state.ProjectRouteTables, cloneProjectRouteTable(table))
		}
		saved = cloneProjectRouteTable(table)
		saved.Bindings = bindings
		return nil
	})
	return saved, err
}

func (s *Store) DeleteProjectRouteTable(projectID string) (model.ProjectRouteTable, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return model.ProjectRouteTable{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteProjectRouteTable(projectID)
	}
	var deleted model.ProjectRouteTable
	err := s.withLockedState(true, func(state *model.State) error {
		index := findProjectRouteTable(state.ProjectRouteTables, projectID)
		if index < 0 {
			return ErrNotFound
		}
		deleted = cloneProjectRouteTable(state.ProjectRouteTables[index])
		state.ProjectRouteTables = append(state.ProjectRouteTables[:index], state.ProjectRouteTables[index+1:]...)
		return nil
	})
	return deleted, err
}

func CompileProjectRouteTableBindings(table model.ProjectRouteTable, apps []model.App) ([]model.ProjectRouteBinding, error) {
	return compileProjectRouteTableBindings(table, apps, true)
}

func TryCompileProjectRouteTableBindings(table model.ProjectRouteTable, apps []model.App) []model.ProjectRouteBinding {
	bindings, _ := compileProjectRouteTableBindings(table, apps, false)
	return bindings
}

func compileProjectRouteTableBindings(table model.ProjectRouteTable, apps []model.App, strict bool) ([]model.ProjectRouteBinding, error) {
	table = model.NormalizeProjectRouteTable(table)
	appByID := make(map[string]model.App, len(apps))
	appByService := make(map[string]model.App, len(apps)*2)
	for _, app := range apps {
		if strings.TrimSpace(app.ProjectID) != strings.TrimSpace(table.ProjectID) {
			continue
		}
		appByID[strings.TrimSpace(app.ID)] = app
		for _, key := range projectRouteAppServiceKeys(app) {
			if _, exists := appByService[key]; !exists {
				appByService[key] = app
			}
		}
	}

	domainsByName := make(map[string]model.ProjectRouteDomain, len(table.Domains))
	domainsByHost := make(map[string]model.ProjectRouteDomain, len(table.Domains))
	for _, domain := range table.Domains {
		normalized := model.NormalizeProjectRouteDomain(domain)
		if normalized.Name != "" {
			key := strings.ToLower(normalized.Name)
			if _, exists := domainsByName[key]; exists && strict {
				return nil, ErrConflict
			}
			domainsByName[key] = normalized
		}
		if normalized.Hostname != "" {
			key := strings.ToLower(normalized.Hostname)
			if _, exists := domainsByHost[key]; exists && strict {
				return nil, ErrConflict
			}
			domainsByHost[key] = normalized
		}
	}

	bindings := make([]model.ProjectRouteBinding, 0)
	seenRoutes := make(map[string]struct{})
	for _, entrypoint := range table.Entrypoints {
		entrypoint = model.NormalizeProjectRouteEntrypoint(entrypoint)
		domain, ok := resolveProjectRouteEntrypointDomain(entrypoint.Domain, table.Domains, domainsByName, domainsByHost)
		if !ok {
			if strict {
				return nil, ErrInvalidInput
			}
			continue
		}
		for _, route := range entrypoint.Routes {
			app, ok := resolveProjectRouteApp(route, appByID, appByService)
			if !ok {
				if strict {
					return nil, ErrNotFound
				}
				continue
			}
			servicePort := model.AppPublicServicePort(app.Spec)
			if servicePort <= 0 && app.Route != nil {
				servicePort = app.Route.ServicePort
			}
			if servicePort <= 0 {
				if strict {
					return nil, ErrInvalidInput
				}
				continue
			}
			pathPrefix := model.NormalizeAppRoutePathPrefix(firstNonEmpty(route.PathPrefix, route.Path))
			routeKey := strings.ToLower(domain.Hostname) + "\x00" + strings.ToLower(pathPrefix)
			if _, exists := seenRoutes[routeKey]; exists {
				if strict {
					return nil, ErrConflict
				}
				continue
			}
			seenRoutes[routeKey] = struct{}{}
			serviceName := strings.TrimSpace(route.Service)
			if serviceName == "" {
				serviceName = app.Name
			}
			bindings = append(bindings, model.ProjectRouteBinding{
				Hostname:       domain.Hostname,
				PathPrefix:     pathPrefix,
				PublicURL:      model.AppRoutePublicURL(domain.Hostname, pathPrefix),
				DomainName:     domain.Name,
				EntrypointName: entrypoint.Name,
				Service:        serviceName,
				AppID:          app.ID,
				AppName:        app.Name,
				ServicePort:    servicePort,
				TLS:            domain.TLS,
				StripPrefix:    route.StripPrefix,
				Rewrite:        strings.TrimSpace(route.Rewrite),
			})
		}
	}
	sort.SliceStable(bindings, func(i, j int) bool {
		if bindings[i].Hostname != bindings[j].Hostname {
			return bindings[i].Hostname < bindings[j].Hostname
		}
		if bindings[i].PathPrefix != bindings[j].PathPrefix {
			return bindings[i].PathPrefix < bindings[j].PathPrefix
		}
		return bindings[i].AppName < bindings[j].AppName
	})
	return bindings, nil
}

func resolveProjectRouteEntrypointDomain(raw string, domains []model.ProjectRouteDomain, byName, byHost map[string]model.ProjectRouteDomain) (model.ProjectRouteDomain, bool) {
	raw = strings.TrimSpace(raw)
	if raw != "" {
		if domain, ok := byName[strings.ToLower(raw)]; ok && domain.Hostname != "" {
			return domain, true
		}
		if domain, ok := byHost[strings.ToLower(raw)]; ok && domain.Hostname != "" {
			return domain, true
		}
		normalized := model.NormalizeProjectRouteDomain(model.ProjectRouteDomain{Hostname: raw})
		if normalized.Hostname != "" {
			return normalized, true
		}
	}
	if len(domains) == 1 {
		domain := model.NormalizeProjectRouteDomain(domains[0])
		return domain, domain.Hostname != ""
	}
	return model.ProjectRouteDomain{}, false
}

func resolveProjectRouteApp(route model.ProjectRouteEntrypointRoute, byID, byService map[string]model.App) (model.App, bool) {
	if appID := strings.TrimSpace(route.AppID); appID != "" {
		app, ok := byID[appID]
		return app, ok
	}
	if service := strings.TrimSpace(route.Service); service != "" {
		app, ok := byService[strings.ToLower(service)]
		return app, ok
	}
	return model.App{}, false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func projectRouteAppServiceKeys(app model.App) []string {
	keys := []string{strings.ToLower(strings.TrimSpace(app.Name))}
	if source := model.AppOriginSource(app); source != nil {
		if service := strings.TrimSpace(source.ComposeService); service != "" {
			keys = append(keys, strings.ToLower(service))
		}
	}
	return keys
}

func projectRouteAppsForProject(apps []model.App, projectID string) []model.App {
	out := make([]model.App, 0)
	for _, app := range apps {
		if isDeletedApp(app) || strings.TrimSpace(app.ProjectID) != strings.TrimSpace(projectID) {
			continue
		}
		out = append(out, app)
	}
	return out
}

func findAppByProjectRouteTables(apps []model.App, tables []model.ProjectRouteTable, hostname, requestPath string, exact bool) (model.App, bool) {
	hostname = normalizeAppDomainHostname(hostname)
	requestPath = model.NormalizeAppRoutePathPrefix(requestPath)
	appByID := make(map[string]model.App, len(apps))
	appsByProject := make(map[string][]model.App)
	for _, app := range apps {
		if isDeletedApp(app) {
			continue
		}
		appByID[strings.TrimSpace(app.ID)] = app
		appsByProject[strings.TrimSpace(app.ProjectID)] = append(appsByProject[strings.TrimSpace(app.ProjectID)], app)
	}
	var (
		best    model.ProjectRouteBinding
		bestLen int
		matched bool
	)
	for _, table := range tables {
		for _, binding := range TryCompileProjectRouteTableBindings(table, appsByProject[strings.TrimSpace(table.ProjectID)]) {
			if !strings.EqualFold(normalizeAppDomainHostname(binding.Hostname), hostname) {
				continue
			}
			prefix := model.NormalizeAppRoutePathPrefix(binding.PathPrefix)
			if exact {
				if prefix != requestPath {
					continue
				}
			} else if !routePathPrefixMatches(prefix, requestPath) {
				continue
			}
			if !matched || len(prefix) > bestLen {
				best = binding
				bestLen = len(prefix)
				matched = true
			}
		}
	}
	if !matched {
		return model.App{}, false
	}
	app, ok := appByID[strings.TrimSpace(best.AppID)]
	return app, ok
}

func (s *Store) pgGetAppByProjectRoute(hostname, requestPath string, exact bool) (model.App, error) {
	apps, err := s.pgListAppsMetadata("", true)
	if err != nil {
		return model.App{}, err
	}
	tables, err := s.pgListProjectRouteTables("", true)
	if err != nil {
		return model.App{}, err
	}
	app, ok := findAppByProjectRouteTables(apps, tables, hostname, requestPath, exact)
	if !ok {
		return model.App{}, ErrNotFound
	}
	return s.pgGetApp(app.ID)
}

func cloneProjectRouteTable(in model.ProjectRouteTable) model.ProjectRouteTable {
	out := in
	if len(in.Domains) > 0 {
		out.Domains = append([]model.ProjectRouteDomain(nil), in.Domains...)
	}
	if len(in.Entrypoints) > 0 {
		out.Entrypoints = make([]model.ProjectRouteEntrypoint, len(in.Entrypoints))
		for index, entrypoint := range in.Entrypoints {
			out.Entrypoints[index] = entrypoint
			if len(entrypoint.Routes) > 0 {
				out.Entrypoints[index].Routes = append([]model.ProjectRouteEntrypointRoute(nil), entrypoint.Routes...)
			}
		}
	}
	if len(in.Bindings) > 0 {
		out.Bindings = append([]model.ProjectRouteBinding(nil), in.Bindings...)
	}
	return out
}

func findProjectRouteTable(tables []model.ProjectRouteTable, projectID string) int {
	for index, table := range tables {
		if strings.TrimSpace(table.ProjectID) == strings.TrimSpace(projectID) {
			return index
		}
	}
	return -1
}

func sortProjectRouteTables(tables []model.ProjectRouteTable) {
	sort.SliceStable(tables, func(i, j int) bool {
		if tables[i].CreatedAt.Equal(tables[j].CreatedAt) {
			return tables[i].ProjectID < tables[j].ProjectID
		}
		return tables[i].CreatedAt.Before(tables[j].CreatedAt)
	})
}

func (s *Store) pgGetProjectRouteTable(projectID string) (model.ProjectRouteTable, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	table, err := scanProjectRouteTable(s.db.QueryRowContext(ctx, `
SELECT project_id, tenant_id, domains_json, entrypoints_json, created_at, updated_at
FROM fugue_project_route_tables
WHERE project_id = $1
`, projectID))
	if err != nil {
		return model.ProjectRouteTable{}, mapDBErr(err)
	}
	return table, nil
}

func (s *Store) pgListProjectRouteTables(tenantID string, platformAdmin bool) ([]model.ProjectRouteTable, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT project_id, tenant_id, domains_json, entrypoints_json, created_at, updated_at
FROM fugue_project_route_tables
`
	args := make([]any, 0, 1)
	if !platformAdmin || strings.TrimSpace(tenantID) != "" {
		query += `WHERE tenant_id = $1
`
		args = append(args, strings.TrimSpace(tenantID))
	}
	query += `ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project route tables: %w", err)
	}
	defer rows.Close()
	tables := make([]model.ProjectRouteTable, 0)
	for rows.Next() {
		table, err := scanProjectRouteTable(rows)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate project route tables: %w", err)
	}
	return tables, nil
}

func (s *Store) pgPutProjectRouteTable(projectID string, table model.ProjectRouteTable) (model.ProjectRouteTable, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ProjectRouteTable{}, fmt.Errorf("begin put project route table transaction: %w", err)
	}
	defer tx.Rollback()

	project, err := scanProject(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, slug, description, default_runtime_id, created_at, updated_at
FROM fugue_projects
WHERE id = $1
FOR UPDATE
`, projectID))
	if err != nil {
		return model.ProjectRouteTable{}, mapDBErr(err)
	}
	if strings.TrimSpace(table.TenantID) != "" && strings.TrimSpace(table.TenantID) != project.TenantID {
		return model.ProjectRouteTable{}, ErrInvalidInput
	}
	table.ProjectID = project.ID
	table.TenantID = project.TenantID
	table = model.NormalizeProjectRouteTable(table)

	apps, err := s.pgListAppsByProjectIDs([]string{project.ID})
	if err != nil {
		return model.ProjectRouteTable{}, err
	}
	bindings, err := CompileProjectRouteTableBindings(table, apps)
	if err != nil {
		return model.ProjectRouteTable{}, err
	}

	now := time.Now().UTC()
	existing, err := scanProjectRouteTable(tx.QueryRowContext(ctx, `
SELECT project_id, tenant_id, domains_json, entrypoints_json, created_at, updated_at
FROM fugue_project_route_tables
WHERE project_id = $1
`, project.ID))
	switch {
	case err == nil:
		table.CreatedAt = existing.CreatedAt
	case errorsIsNotFound(err):
		table.CreatedAt = now
	default:
		return model.ProjectRouteTable{}, mapDBErr(err)
	}
	table.UpdatedAt = now

	domainsJSON, err := marshalJSON(table.Domains)
	if err != nil {
		return model.ProjectRouteTable{}, err
	}
	entrypointsJSON, err := marshalJSON(table.Entrypoints)
	if err != nil {
		return model.ProjectRouteTable{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_project_route_tables (project_id, tenant_id, domains_json, entrypoints_json, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (project_id) DO UPDATE
SET tenant_id = EXCLUDED.tenant_id,
	domains_json = EXCLUDED.domains_json,
	entrypoints_json = EXCLUDED.entrypoints_json,
	updated_at = EXCLUDED.updated_at
`, table.ProjectID, table.TenantID, domainsJSON, entrypointsJSON, table.CreatedAt, table.UpdatedAt); err != nil {
		return model.ProjectRouteTable{}, fmt.Errorf("put project route table %s: %w", project.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return model.ProjectRouteTable{}, fmt.Errorf("commit put project route table transaction: %w", err)
	}
	table.Bindings = bindings
	return table, nil
}

func (s *Store) pgDeleteProjectRouteTable(projectID string) (model.ProjectRouteTable, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ProjectRouteTable{}, fmt.Errorf("begin delete project route table transaction: %w", err)
	}
	defer tx.Rollback()
	table, err := scanProjectRouteTable(tx.QueryRowContext(ctx, `
SELECT project_id, tenant_id, domains_json, entrypoints_json, created_at, updated_at
FROM fugue_project_route_tables
WHERE project_id = $1
FOR UPDATE
`, projectID))
	if err != nil {
		return model.ProjectRouteTable{}, mapDBErr(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_project_route_tables WHERE project_id = $1`, projectID); err != nil {
		return model.ProjectRouteTable{}, fmt.Errorf("delete project route table %s: %w", projectID, err)
	}
	if err := tx.Commit(); err != nil {
		return model.ProjectRouteTable{}, fmt.Errorf("commit delete project route table transaction: %w", err)
	}
	return table, nil
}

func scanProjectRouteTable(scanner sqlScanner) (model.ProjectRouteTable, error) {
	var table model.ProjectRouteTable
	var domainsRaw []byte
	var entrypointsRaw []byte
	if err := scanner.Scan(&table.ProjectID, &table.TenantID, &domainsRaw, &entrypointsRaw, &table.CreatedAt, &table.UpdatedAt); err != nil {
		return model.ProjectRouteTable{}, err
	}
	domains, err := decodeJSONValue[[]model.ProjectRouteDomain](domainsRaw)
	if err != nil {
		return model.ProjectRouteTable{}, err
	}
	entrypoints, err := decodeJSONValue[[]model.ProjectRouteEntrypoint](entrypointsRaw)
	if err != nil {
		return model.ProjectRouteTable{}, err
	}
	table.Domains = domains
	table.Entrypoints = entrypoints
	return model.NormalizeProjectRouteTable(table), nil
}

func errorsIsNotFound(err error) bool {
	return err == sql.ErrNoRows || mapDBErr(err) == ErrNotFound
}
