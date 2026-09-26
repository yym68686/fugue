package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type StaticEdgeRegistrationFilter struct {
	TenantID      string
	ProjectID     string
	PlatformAdmin bool
}

func (s *Store) ListStaticEdgeRegistrations(filter StaticEdgeRegistrationFilter) ([]model.StaticEdgeRegistration, error) {
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.ProjectID = strings.TrimSpace(filter.ProjectID)
	if !filter.PlatformAdmin && filter.TenantID == "" {
		return nil, fmt.Errorf("%w: tenant ID is required", ErrInvalidInput)
	}
	if s.usingDatabase() {
		return s.pgListStaticEdgeRegistrations(filter)
	}
	out := []model.StaticEdgeRegistration{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, registration := range state.StaticEdgeRegistrations {
			if !staticEdgeVisible(registration, filter) {
				continue
			}
			out = append(out, cloneStaticEdgeRegistration(registration))
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].ProjectID == out[j].ProjectID {
				return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name)
			}
			return out[i].ProjectID < out[j].ProjectID
		})
		return nil
	})
	return out, err
}

func (s *Store) GetStaticEdgeRegistration(id, tenantID string, platformAdmin bool) (model.StaticEdgeRegistration, error) {
	id = strings.TrimSpace(id)
	tenantID = strings.TrimSpace(tenantID)
	if id == "" || (!platformAdmin && tenantID == "") {
		return model.StaticEdgeRegistration{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetStaticEdgeRegistration(id, tenantID, platformAdmin)
	}
	var out model.StaticEdgeRegistration
	err := s.withLockedState(false, func(state *model.State) error {
		for _, registration := range state.StaticEdgeRegistrations {
			if registration.ID != id || (!platformAdmin && registration.TenantID != tenantID) {
				continue
			}
			out = cloneStaticEdgeRegistration(registration)
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

func (s *Store) CreateStaticEdgeRegistration(registration model.StaticEdgeRegistration) (model.StaticEdgeRegistration, error) {
	registration, err := normalizeStaticEdgeRegistration(registration)
	if err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	if s.usingDatabase() {
		return s.pgCreateStaticEdgeRegistration(registration)
	}
	var out model.StaticEdgeRegistration
	err = s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, registration.TenantID) < 0 || findProject(state, registration.ProjectID) < 0 {
			return ErrNotFound
		}
		project := state.Projects[findProject(state, registration.ProjectID)]
		if project.TenantID != registration.TenantID {
			return ErrNotFound
		}
		for _, existing := range state.StaticEdgeRegistrations {
			if existing.TenantID == registration.TenantID && existing.ProjectID == registration.ProjectID &&
				(strings.EqualFold(existing.Name, registration.Name) || existing.EdgeID == registration.EdgeID) {
				return ErrConflict
			}
		}
		now := time.Now().UTC()
		registration.ID = model.NewID("static_edge")
		registration.CreatedAt, registration.UpdatedAt = now, now
		state.StaticEdgeRegistrations = append(state.StaticEdgeRegistrations, cloneStaticEdgeRegistration(registration))
		out = cloneStaticEdgeRegistration(registration)
		return nil
	})
	return out, err
}

func (s *Store) UpdateStaticEdgeRegistrationProof(id, tenantID, digest, signingKeyID string, ready bool) (model.StaticEdgeRegistration, error) {
	id, tenantID, digest, signingKeyID = strings.TrimSpace(id), strings.TrimSpace(tenantID), strings.TrimSpace(digest), strings.TrimSpace(signingKeyID)
	if id == "" || tenantID == "" {
		return model.StaticEdgeRegistration{}, ErrInvalidInput
	}
	if err := validateStaticEdgeProof(digest, signingKeyID); err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	if s.usingDatabase() {
		return s.pgUpdateStaticEdgeRegistrationProof(id, tenantID, digest, signingKeyID, ready)
	}
	var out model.StaticEdgeRegistration
	err := s.withLockedState(true, func(state *model.State) error {
		for index := range state.StaticEdgeRegistrations {
			if state.StaticEdgeRegistrations[index].ID != id || state.StaticEdgeRegistrations[index].TenantID != tenantID {
				continue
			}
			registration := state.StaticEdgeRegistrations[index]
			if registration.Status == model.StaticEdgeStatusRevoked {
				return ErrConflict
			}
			registration.PossessionProofDigest = digest
			registration.SigningKeyID = signingKeyID
			registration.Status = model.StaticEdgeStatusPending
			if ready {
				registration.Status = model.StaticEdgeStatusReady
			}
			now := time.Now().UTC()
			registration.LastProofAt, registration.UpdatedAt = &now, now
			state.StaticEdgeRegistrations[index] = cloneStaticEdgeRegistration(registration)
			out = cloneStaticEdgeRegistration(registration)
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

func (s *Store) RevokeStaticEdgeRegistration(id, tenantID string, platformAdmin bool) (model.StaticEdgeRegistration, error) {
	registration, err := s.GetStaticEdgeRegistration(id, tenantID, platformAdmin)
	if err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	if s.usingDatabase() {
		return s.pgRevokeStaticEdgeRegistration(registration.ID, tenantID, platformAdmin)
	}
	err = s.withLockedState(true, func(state *model.State) error {
		for index := range state.StaticEdgeRegistrations {
			if state.StaticEdgeRegistrations[index].ID != registration.ID {
				continue
			}
			registration = cloneStaticEdgeRegistration(state.StaticEdgeRegistrations[index])
			registration.Status = model.StaticEdgeStatusRevoked
			registration.UpdatedAt = time.Now().UTC()
			state.StaticEdgeRegistrations[index] = cloneStaticEdgeRegistration(registration)
			return nil
		}
		return ErrNotFound
	})
	return registration, err
}

func staticEdgeVisible(registration model.StaticEdgeRegistration, filter StaticEdgeRegistrationFilter) bool {
	return (filter.TenantID == "" || registration.TenantID == filter.TenantID) &&
		(filter.ProjectID == "" || registration.ProjectID == filter.ProjectID)
}

func normalizeStaticEdgeRegistration(in model.StaticEdgeRegistration) (model.StaticEdgeRegistration, error) {
	in.ID = strings.TrimSpace(in.ID)
	in.TenantID, in.ProjectID = strings.TrimSpace(in.TenantID), strings.TrimSpace(in.ProjectID)
	in.Name, in.EdgeID = strings.TrimSpace(in.Name), strings.TrimSpace(in.EdgeID)
	in.Transport, in.ManagerURL = strings.ToLower(strings.TrimSpace(in.Transport)), strings.TrimSpace(in.ManagerURL)
	in.CertificateFingerprint = strings.ToLower(strings.TrimSpace(in.CertificateFingerprint))
	in.SigningKeyID, in.PossessionProofDigest = strings.TrimSpace(in.SigningKeyID), strings.TrimSpace(in.PossessionProofDigest)
	in.Status = strings.ToLower(strings.TrimSpace(in.Status))
	if in.TenantID == "" || in.ProjectID == "" || in.Name == "" || in.EdgeID == "" {
		return model.StaticEdgeRegistration{}, fmt.Errorf("%w: tenant, project, name, and edge ID are required", ErrInvalidInput)
	}
	if in.Transport != model.StaticEdgeTransportMTLS && in.Transport != model.StaticEdgeTransportSSH {
		return model.StaticEdgeRegistration{}, fmt.Errorf("%w: unsupported static edge transport", ErrInvalidInput)
	}
	if in.ManagerURL != "" {
		u, err := url.Parse(in.ManagerURL)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return model.StaticEdgeRegistration{}, fmt.Errorf("%w: manager URL must be an HTTPS authority", ErrInvalidInput)
		}
	}
	if in.CertificateFingerprint != "" && !validHexFingerprint(in.CertificateFingerprint) {
		return model.StaticEdgeRegistration{}, fmt.Errorf("%w: certificate fingerprint must be 32-byte hex", ErrInvalidInput)
	}
	if in.Status == "" {
		in.Status = model.StaticEdgeStatusPending
	}
	if in.Status != model.StaticEdgeStatusPending {
		return model.StaticEdgeRegistration{}, fmt.Errorf("%w: new registrations must be pending", ErrInvalidInput)
	}
	if err := validateStaticEdgeProof(in.PossessionProofDigest, in.SigningKeyID); err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	return in, nil
}

func validateStaticEdgeProof(digest, signingKeyID string) error {
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 || !validHexFingerprint(digest[len("sha256:"):]) || signingKeyID == "" {
		return fmt.Errorf("%w: possession proof digest and signing key ID are required", ErrInvalidInput)
	}
	return nil
}

func validHexFingerprint(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func cloneStaticEdgeRegistration(in model.StaticEdgeRegistration) model.StaticEdgeRegistration {
	out := in
	if in.LastProofAt != nil {
		proofAt := in.LastProofAt.UTC()
		out.LastProofAt = &proofAt
	}
	return out
}

const staticEdgeRegistrationSelectColumns = `id, tenant_id, project_id, name, edge_id, transport, manager_url,
certificate_fingerprint, signing_key_id, possession_proof_digest, status, last_proof_at, created_at, updated_at`

func (s *Store) pgListStaticEdgeRegistrations(filter StaticEdgeRegistrationFilter) ([]model.StaticEdgeRegistration, error) {
	if err := s.ensureDatabaseReady(); err != nil {
		return nil, err
	}
	args := []any{}
	clauses := []string{"1=1"}
	if filter.TenantID != "" {
		args = append(args, filter.TenantID)
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.ProjectID != "" {
		args = append(args, filter.ProjectID)
		clauses = append(clauses, fmt.Sprintf("project_id = $%d", len(args)))
	}
	rows, err := s.db.QueryContext(context.Background(), `SELECT `+staticEdgeRegistrationSelectColumns+` FROM fugue_static_edge_registrations WHERE `+strings.Join(clauses, " AND ")+` ORDER BY project_id, lower(name), id`, args...)
	if err != nil {
		return nil, fmt.Errorf("list static edge registrations: %w", err)
	}
	defer rows.Close()
	out := []model.StaticEdgeRegistration{}
	for rows.Next() {
		registration, err := scanStaticEdgeRegistration(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, registration)
	}
	return out, rows.Err()
}

func (s *Store) pgGetStaticEdgeRegistration(id, tenantID string, platformAdmin bool) (model.StaticEdgeRegistration, error) {
	if err := s.ensureDatabaseReady(); err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	query := `SELECT ` + staticEdgeRegistrationSelectColumns + ` FROM fugue_static_edge_registrations WHERE id = $1`
	args := []any{id}
	if !platformAdmin {
		query += " AND tenant_id = $2"
		args = append(args, tenantID)
	}
	return scanStaticEdgeRegistration(s.db.QueryRowContext(context.Background(), query, args...))
}

func (s *Store) pgCreateStaticEdgeRegistration(registration model.StaticEdgeRegistration) (model.StaticEdgeRegistration, error) {
	if err := s.ensureDatabaseReady(); err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	now := time.Now().UTC()
	registration.ID = model.NewID("static_edge")
	registration.CreatedAt, registration.UpdatedAt = now, now
	row := s.db.QueryRowContext(context.Background(), `INSERT INTO fugue_static_edge_registrations (`+staticEdgeRegistrationSelectColumns+`) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14 FROM fugue_projects WHERE id=$3 AND tenant_id=$2 RETURNING `+staticEdgeRegistrationSelectColumns,
		registration.ID, registration.TenantID, registration.ProjectID, registration.Name, registration.EdgeID, registration.Transport, registration.ManagerURL,
		registration.CertificateFingerprint, registration.SigningKeyID, registration.PossessionProofDigest, registration.Status, registration.LastProofAt, registration.CreatedAt, registration.UpdatedAt)
	return scanStaticEdgeRegistration(row)
}

func (s *Store) pgUpdateStaticEdgeRegistrationProof(id, tenantID, digest, signingKeyID string, ready bool) (model.StaticEdgeRegistration, error) {
	if err := s.ensureDatabaseReady(); err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	defer tx.Rollback()
	current, err := scanStaticEdgeRegistration(tx.QueryRowContext(context.Background(), `SELECT `+staticEdgeRegistrationSelectColumns+` FROM fugue_static_edge_registrations WHERE id=$1 AND tenant_id=$2 FOR UPDATE`, id, tenantID))
	if err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	if current.Status == model.StaticEdgeStatusRevoked {
		return model.StaticEdgeRegistration{}, ErrConflict
	}
	status := model.StaticEdgeStatusPending
	if ready {
		status = model.StaticEdgeStatusReady
	}
	now := time.Now().UTC()
	updated, err := scanStaticEdgeRegistration(tx.QueryRowContext(context.Background(), `UPDATE fugue_static_edge_registrations SET possession_proof_digest=$3, signing_key_id=$4, status=$5, last_proof_at=$6, updated_at=$6 WHERE id=$1 AND tenant_id=$2 RETURNING `+staticEdgeRegistrationSelectColumns, id, tenantID, digest, signingKeyID, status, now))
	if err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	return updated, tx.Commit()
}

func (s *Store) pgRevokeStaticEdgeRegistration(id, tenantID string, platformAdmin bool) (model.StaticEdgeRegistration, error) {
	if err := s.ensureDatabaseReady(); err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	query := `UPDATE fugue_static_edge_registrations SET status=$2, updated_at=$3 WHERE id=$1`
	args := []any{id, model.StaticEdgeStatusRevoked, time.Now().UTC()}
	if !platformAdmin {
		query += " AND tenant_id=$4"
		args = append(args, tenantID)
	}
	query += " RETURNING " + staticEdgeRegistrationSelectColumns
	return scanStaticEdgeRegistration(s.db.QueryRowContext(context.Background(), query, args...))
}

type staticEdgeScanner interface{ Scan(...any) error }

func scanStaticEdgeRegistration(scanner staticEdgeScanner) (model.StaticEdgeRegistration, error) {
	var out model.StaticEdgeRegistration
	var proofAt sql.NullTime
	err := scanner.Scan(&out.ID, &out.TenantID, &out.ProjectID, &out.Name, &out.EdgeID, &out.Transport, &out.ManagerURL,
		&out.CertificateFingerprint, &out.SigningKeyID, &out.PossessionProofDigest, &out.Status, &proofAt, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return model.StaticEdgeRegistration{}, mapDBErr(err)
	}
	if proofAt.Valid {
		value := proofAt.Time.UTC()
		out.LastProofAt = &value
	}
	return cloneStaticEdgeRegistration(out), nil
}
