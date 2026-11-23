package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

// ProjectHostRouteInputs is read from one snapshot, or under write locks for apply.
// Table is the actual stored table, not the synthetic legacy API view.
type ProjectHostRouteInputs struct {
	Project model.Project
	Domain  model.AppDomain
	DNS     model.DNSRecord
	Zone    model.HostedZone
	Apps    []model.App
	Table   model.ProjectRouteTable
}

// Isolate a hostname for a read-only comparison against its actual saved
// intent. Resolve aliases and the implicit single-domain case BEFORE
// filtering: deleting the other domains first would misassign an unnamed
// entrypoint. Never persist this projected view as the entire project table.
func ProjectRoutesForHost(table model.ProjectRouteTable, host string) (model.ProjectRouteTable, error) {
	table = model.NormalizeProjectRouteTable(table)
	if host == "" || host != strings.ToLower(strings.TrimSpace(host)) {
		return model.ProjectRouteTable{}, ErrInvalidInput
	}
	byName, byHost := map[string]model.ProjectRouteDomain{}, map[string]model.ProjectRouteDomain{}
	out := model.ProjectRouteTable{ProjectID: table.ProjectID, TenantID: table.TenantID}
	for _, d := range table.Domains {
		name := strings.ToLower(d.Name)
		if name != "" {
			if prev, exists := byName[name]; exists && (prev.Hostname == host || d.Hostname == host) {
				return out, ErrConflict
			}
			byName[name] = d
		}
		if d.Hostname != "" {
			if _, exists := byHost[d.Hostname]; exists && d.Hostname == host {
				return out, ErrConflict
			}
			byHost[d.Hostname] = d
		}
	}
	for _, ep := range table.Entrypoints {
		d, ok := resolveProjectRouteEntrypointDomain(ep.Domain, table.Domains, byName, byHost)
		if !ok || d.Hostname != host {
			continue
		}
		// Freeze the already-resolved hostname, not its mutable alias.
		ep.Domain = host
		out.Entrypoints = append(out.Entrypoints, ep)
	}
	if d, ok := byHost[host]; ok {
		d.Name = host
		out.Domains = []model.ProjectRouteDomain{d}
	}
	return out, nil
}

// EditProjectHostRoutes serializes with whole-table writes. The callback must
// only compute over the supplied inputs, and can return nil for plan/no-op.
// Domain identity and application ownership cannot change within the write.
func (s *Store) EditProjectHostRoutes(ctx context.Context, projectID, hostname string, write bool, edit func(ProjectHostRouteInputs) (*model.ProjectRouteTable, error)) error {
	if projectID == "" || hostname == "" || edit == nil {
		return ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgEditProjectHostRoutes(ctx, projectID, hostname, write, edit)
	}
	return s.withLockedState(write, func(state *model.State) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		pi, di := findProject(state, projectID), findAppDomain(state, hostname)
		if pi < 0 || di < 0 {
			return ErrNotFound
		}
		in := ProjectHostRouteInputs{Project: state.Projects[pi], Domain: cloneAppDomain(state.AppDomains[di]), Apps: projectRouteAppsForProject(state.Apps, projectID)}
		if in.Domain.DNSMode == model.AppDomainDNSModeManaged {
			if ri := findDNSRecord(state, in.Domain.DNSZoneID, in.Domain.DNSRecordID); ri >= 0 {
				in.DNS = state.DNSRecords[ri]
			}
			for _, zone := range state.HostedZones {
				if zone.ID == in.Domain.DNSZoneID {
					in.Zone = zone
					break
				}
			}
		}
		i := findProjectRouteTable(state.ProjectRouteTables, projectID)
		if i >= 0 {
			in.Table = cloneProjectRouteTable(state.ProjectRouteTables[i])
		}
		next, err := edit(in)
		if err != nil || next == nil || !write {
			return err
		}
		table := stampProjectHostRouteTable(in, *next)
		if i < 0 {
			state.ProjectRouteTables = append(state.ProjectRouteTables, table)
		} else {
			state.ProjectRouteTables[i] = table
		}
		return nil
	})
}

func stampProjectHostRouteTable(in ProjectHostRouteInputs, next model.ProjectRouteTable) model.ProjectRouteTable {
	next.ProjectID, next.TenantID = in.Project.ID, in.Project.TenantID
	next.UpdatedAt = time.Now().UTC()
	next.CreatedAt = in.Table.CreatedAt
	if next.CreatedAt.IsZero() {
		next.CreatedAt = next.UpdatedAt
	}
	return model.NormalizeProjectRouteTable(next)
}

func (s *Store) pgEditProjectHostRoutes(parent context.Context, projectID, hostname string, write bool, edit func(ProjectHostRouteInputs) (*model.ProjectRouteTable, error)) error {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	// Planning and receipt readers cannot apply the callback's result. Use
	// one MVCC snapshot instead of blocking writes to every app in a project.
	// Real writes still serialize on the project and recheck the entire plan.
	update, share := " FOR UPDATE", " FOR SHARE"
	var opts *sql.TxOptions
	if !write {
		opts = &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}
		update, share = "", ""
	}
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var in ProjectHostRouteInputs
	in.Project, err = scanProject(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, slug, description, default_runtime_id, created_at, updated_at
FROM fugue_projects WHERE id = $1`+update, projectID))
	if err != nil {
		return mapDBErr(err)
	}
	in.Apps, err = readRouteBusinessRows(ctx, tx, `SELECT `+routeBusinessAppSelectColumns+`
FROM fugue_apps WHERE project_id = $1 AND `+appVisiblePhasePredicate+` ORDER BY id`+share, scanApp, projectID)
	if err != nil {
		return err
	}
	in.Domain, err = scanAppDomain(tx.QueryRowContext(ctx, `
SELECT hostname, tenant_id, app_id, status, dns_mode, dns_zone_id, dns_record_id, dns_record_source, dns_status, dns_record_kind, tls_status, verification_txt_name, verification_txt_value, route_target, last_message, dns_last_message, tls_last_message, last_checked_at, dns_last_checked_at, verified_at, tls_last_checked_at, tls_ready_at, created_at, updated_at
FROM fugue_app_domains WHERE lower(hostname) = $1`+share, hostname))
	if err != nil {
		return mapDBErr(err)
	}
	if in.Domain.DNSMode == model.AppDomainDNSModeManaged && in.Domain.DNSZoneID != "" && in.Domain.DNSRecordID != "" {
		in.Zone, err = scanHostedZone(tx.QueryRowContext(ctx, hostedZoneSelect()+` WHERE id = $1`+share, in.Domain.DNSZoneID))
		if err != nil && err != sql.ErrNoRows {
			return mapDBErr(err)
		}
		in.DNS, err = scanDNSRecord(tx.QueryRowContext(ctx, dnsRecordSelect()+` WHERE zone_id = $1 AND id = $2`+share, in.Domain.DNSZoneID, in.Domain.DNSRecordID))
		if err != nil && err != sql.ErrNoRows {
			return mapDBErr(err)
		}
	}
	in.Table, err = scanProjectRouteTable(tx.QueryRowContext(ctx, `
SELECT project_id, tenant_id, domains_json, entrypoints_json, created_at, updated_at
FROM fugue_project_route_tables WHERE project_id = $1`+update, projectID))
	if err != nil && err != sql.ErrNoRows {
		return mapDBErr(err)
	}
	next, err := edit(in)
	if err != nil || next == nil || !write {
		return err
	}
	table := stampProjectHostRouteTable(in, *next)
	domains, err := marshalJSON(table.Domains)
	if err != nil {
		return err
	}
	entrypoints, err := marshalJSON(table.Entrypoints)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_project_route_tables (project_id, tenant_id, domains_json, entrypoints_json, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (project_id) DO UPDATE SET domains_json = EXCLUDED.domains_json, entrypoints_json = EXCLUDED.entrypoints_json, updated_at = EXCLUDED.updated_at
`, table.ProjectID, table.TenantID, domains, entrypoints, table.CreatedAt, table.UpdatedAt); err != nil {
		return fmt.Errorf("save scoped project routes: %w", err)
	}
	return tx.Commit()
}
