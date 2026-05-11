package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrConflict            = errors.New("conflict")
	ErrInvalidInput        = errors.New("invalid input")
	ErrIdempotencyMismatch = errors.New("idempotency key mismatch")
)

type Store struct {
	path        string
	databaseURL string
	db          *sql.DB
	dbInitMu    sync.Mutex
	dbReady     bool
}

type ProjectUpdate struct {
	Name                  *string
	Description           *string
	DefaultRuntimeID      *string
	ClearDefaultRuntimeID bool
}

func New(path string, databaseURL ...string) *Store {
	var dsn string
	if len(databaseURL) > 0 {
		dsn = strings.TrimSpace(databaseURL[0])
	}
	return &Store{
		path:        path,
		databaseURL: dsn,
	}
}

func (s *Store) Init() error {
	if s.usingDatabase() {
		if err := s.ensureDatabaseReady(); err != nil {
			return err
		}
		if err := s.pgRepairAppStatuses(); err != nil {
			return err
		}
		return nil
	}
	return s.withFileLockedState(true, func(state *model.State) error {
		ensureDefaults(state)
		repairAllAPIKeyStatuses(state)
		repairAllAppStatuses(state)
		return nil
	})
}

func (s *Store) CheckReadiness(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s.usingDatabase() {
		if s.db == nil || !s.dbReady {
			if err := s.ensureDatabaseReady(); err != nil {
				return err
			}
		}
		pingCtx := ctx
		cancel := func() {}
		if _, ok := ctx.Deadline(); !ok {
			pingCtx, cancel = context.WithTimeout(ctx, 3*time.Second)
		}
		defer cancel()
		if err := s.db.PingContext(pingCtx); err != nil {
			return fmt.Errorf("ping postgres: %w", err)
		}
		return nil
	}
	return s.withFileLockedState(false, func(state *model.State) error {
		return nil
	})
}

func (s *Store) usingDatabase() bool {
	return strings.TrimSpace(s.databaseURL) != ""
}

func (s *Store) withLockedState(write bool, fn func(*model.State) error) error {
	if s.usingDatabase() {
		return fmt.Errorf("internal error: database mode requires SQL store methods")
	}
	return s.withFileLockedState(write, fn)
}

func (s *Store) withFileLockedState(write bool, fn func(*model.State) error) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create store directory: %w", err)
	}

	lockPath := s.path + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open lock file: %w", err)
	}
	defer lockFile.Close()

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock store file: %w", err)
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)

	state, err := s.readState()
	if err != nil {
		return err
	}
	ensureDefaults(&state)

	if err := fn(&state); err != nil {
		return err
	}

	if !write {
		return nil
	}
	return s.writeState(state)
}

func (s *Store) ListTenants() ([]model.Tenant, error) {
	if s.usingDatabase() {
		return s.pgListTenants()
	}
	var tenants []model.Tenant
	err := s.withLockedState(false, func(state *model.State) error {
		tenants = append(tenants, state.Tenants...)
		sort.Slice(tenants, func(i, j int) bool {
			return tenants[i].CreatedAt.Before(tenants[j].CreatedAt)
		})
		return nil
	})
	return tenants, err
}

func (s *Store) GetTenant(id string) (model.Tenant, error) {
	if s.usingDatabase() {
		return s.pgGetTenant(id)
	}
	var tenant model.Tenant
	err := s.withLockedState(false, func(state *model.State) error {
		index := findTenant(state, id)
		if index < 0 {
			return ErrNotFound
		}
		tenant = state.Tenants[index]
		return nil
	})
	return tenant, err
}

func (s *Store) CreateTenant(name string) (model.Tenant, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return model.Tenant{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateTenant(name)
	}

	var tenant model.Tenant
	err := s.withLockedState(true, func(state *model.State) error {
		slug := model.Slugify(name)
		for _, existing := range state.Tenants {
			if existing.Slug == slug {
				return ErrConflict
			}
		}
		now := time.Now().UTC()
		tenant = model.Tenant{
			ID:        model.NewID("tenant"),
			Name:      name,
			Slug:      slug,
			Status:    "active",
			CreatedAt: now,
			UpdatedAt: now,
		}
		state.Tenants = append(state.Tenants, tenant)
		ensureTenantBillingRecord(state, tenant.ID, now)
		return nil
	})
	return tenant, err
}

func (s *Store) DeleteTenant(id string) (model.Tenant, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Tenant{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteTenant(id)
	}

	var tenant model.Tenant
	err := s.withLockedState(true, func(state *model.State) error {
		index := findTenant(state, id)
		if index < 0 {
			return ErrNotFound
		}
		tenant = state.Tenants[index]
		state.Tenants = append(state.Tenants[:index], state.Tenants[index+1:]...)

		state.Projects = deleteProjectsByTenant(state.Projects, id)
		state.ProjectRuntimeReservations = deleteProjectRuntimeReservationsByTenant(state.ProjectRuntimeReservations, id)
		state.APIKeys = deleteAPIKeysByTenant(state.APIKeys, id)
		state.EnrollmentTokens = deleteEnrollmentTokensByTenant(state.EnrollmentTokens, id)

		deletedNodeKeyIDs := make(map[string]struct{})
		deletedRuntimeIDs := make(map[string]struct{})
		state.NodeKeys = deleteNodeKeysByTenant(state.NodeKeys, id, deletedNodeKeyIDs)
		state.Machines = deleteLegacyMachinesByTenant(state.Machines, id, deletedNodeKeyIDs)
		state.Runtimes = deleteRuntimesByTenant(state.Runtimes, id, deletedNodeKeyIDs, deletedRuntimeIDs)
		state.RuntimeGrants = deleteRuntimeAccessGrants(state.RuntimeGrants, id, deletedRuntimeIDs)
		state.Apps = deleteAppsByTenant(state.Apps, id)
		state.BackingServices = deleteBackingServicesByTenant(state.BackingServices, id)
		state.ServiceBindings = deleteServiceBindingsByTenant(state.ServiceBindings, id)
		state.Operations = deleteOperationsByTenant(state.Operations, id)
		state.AuditEvents = deleteAuditEventsByTenant(state.AuditEvents, id)
		state.TenantBilling = deleteTenantBillingRecords(state.TenantBilling, id)
		state.BillingEvents = deleteTenantBillingEvents(state.BillingEvents, id)
		state.Idempotency = deleteIdempotencyRecordsByTenant(state.Idempotency, id)
		return nil
	})
	return tenant, err
}

func (s *Store) ListProjects(tenantID string) ([]model.Project, error) {
	if s.usingDatabase() {
		return s.pgListProjects(tenantID)
	}
	var projects []model.Project
	err := s.withLockedState(false, func(state *model.State) error {
		for _, project := range state.Projects {
			if project.TenantID == tenantID {
				projects = append(projects, project)
			}
		}
		sort.Slice(projects, func(i, j int) bool {
			return projects[i].CreatedAt.Before(projects[j].CreatedAt)
		})
		return nil
	})
	return projects, err
}

func (s *Store) ListAllProjects() ([]model.Project, error) {
	if s.usingDatabase() {
		return s.pgListAllProjects()
	}

	var projects []model.Project
	err := s.withLockedState(false, func(state *model.State) error {
		projects = append(projects, state.Projects...)
		sort.Slice(projects, func(i, j int) bool {
			return projects[i].CreatedAt.Before(projects[j].CreatedAt)
		})
		return nil
	})
	return projects, err
}

func (s *Store) GetProject(id string) (model.Project, error) {
	if s.usingDatabase() {
		return s.pgGetProject(id)
	}
	var project model.Project
	err := s.withLockedState(false, func(state *model.State) error {
		index := findProject(state, id)
		if index < 0 {
			return ErrNotFound
		}
		project = state.Projects[index]
		return nil
	})
	return project, err
}

func (s *Store) UpdateProject(id string, name, description *string) (model.Project, error) {
	return s.UpdateProjectFields(id, ProjectUpdate{Name: name, Description: description})
}

func (s *Store) UpdateProjectFields(id string, update ProjectUpdate) (model.Project, error) {
	id = strings.TrimSpace(id)
	if id == "" || (update.Name == nil && update.Description == nil && update.DefaultRuntimeID == nil && !update.ClearDefaultRuntimeID) {
		return model.Project{}, ErrInvalidInput
	}
	if update.ClearDefaultRuntimeID && update.DefaultRuntimeID != nil && strings.TrimSpace(*update.DefaultRuntimeID) != "" {
		return model.Project{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateProjectFields(id, update)
	}

	var project model.Project
	err := s.withLockedState(true, func(state *model.State) error {
		index := findProject(state, id)
		if index < 0 {
			return ErrNotFound
		}
		if projectDeleteRequested(state, id) {
			return ErrConflict
		}
		project = state.Projects[index]
		changed := false

		if update.Name != nil {
			trimmedName := strings.TrimSpace(*update.Name)
			if trimmedName == "" {
				return ErrInvalidInput
			}
			slug := model.Slugify(trimmedName)
			for _, existing := range state.Projects {
				if existing.ID == project.ID {
					continue
				}
				if existing.TenantID == project.TenantID && existing.Slug == slug {
					return ErrConflict
				}
			}
			if project.Name != trimmedName || project.Slug != slug {
				project.Name = trimmedName
				project.Slug = slug
				changed = true
			}
		}

		if update.Description != nil {
			trimmedDescription := strings.TrimSpace(*update.Description)
			if project.Description != trimmedDescription {
				project.Description = trimmedDescription
				changed = true
			}
		}

		if update.DefaultRuntimeID != nil {
			runtimeID := strings.TrimSpace(*update.DefaultRuntimeID)
			if runtimeID != "" && !runtimeVisibleToTenant(state, runtimeID, project.TenantID) {
				return ErrNotFound
			}
			if err := validateRuntimeReservedForProjectState(state, project.ID, runtimeID); err != nil {
				return err
			}
			if project.DefaultRuntimeID != runtimeID {
				project.DefaultRuntimeID = runtimeID
				changed = true
			}
		} else if update.ClearDefaultRuntimeID {
			if project.DefaultRuntimeID != "" {
				project.DefaultRuntimeID = ""
				changed = true
			}
		}

		if changed {
			project.UpdatedAt = time.Now().UTC()
			state.Projects[index] = project
		}
		return nil
	})
	return project, err
}

func (s *Store) CreateProject(tenantID, name, description string, defaultRuntimeID ...string) (model.Project, error) {
	name = strings.TrimSpace(name)
	runtimeID := ""
	if len(defaultRuntimeID) > 0 {
		runtimeID = strings.TrimSpace(defaultRuntimeID[0])
	}
	if tenantID == "" || name == "" {
		return model.Project{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateProject(tenantID, name, description, runtimeID)
	}

	var project model.Project
	err := s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		if runtimeID != "" && !runtimeVisibleToTenant(state, runtimeID, tenantID) {
			return ErrNotFound
		}
		if err := validateRuntimeReservedForProjectState(state, "", runtimeID); err != nil {
			return err
		}
		slug := model.Slugify(name)
		for _, existing := range state.Projects {
			if existing.TenantID == tenantID && existing.Slug == slug {
				return ErrConflict
			}
		}
		now := time.Now().UTC()
		project = model.Project{
			ID:               model.NewID("project"),
			TenantID:         tenantID,
			Name:             name,
			Slug:             slug,
			Description:      strings.TrimSpace(description),
			DefaultRuntimeID: runtimeID,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		state.Projects = append(state.Projects, project)
		return nil
	})
	return project, err
}

func (s *Store) MarkProjectDeleteRequested(id string) (model.Project, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Project{}, false, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgMarkProjectDeleteRequested(id)
	}

	var (
		alreadyRequested bool
		project          model.Project
	)
	err := s.withLockedState(true, func(state *model.State) error {
		index := findProject(state, id)
		if index < 0 {
			return ErrNotFound
		}
		project = state.Projects[index]
		alreadyRequested = projectDeleteRequested(state, id)
		if !alreadyRequested {
			now := time.Now().UTC()
			markProjectDeleteRequested(state, id, now)
			project.UpdatedAt = now
			state.Projects[index] = project
		}
		return nil
	})
	return project, alreadyRequested, err
}

func (s *Store) DeleteProject(id string) (model.Project, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Project{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteProject(id)
	}

	var project model.Project
	err := s.withLockedState(true, func(state *model.State) error {
		if projectHasLiveResources(state, id) {
			return ErrConflict
		}
		deletedProject, err := deleteProjectFromState(state, id)
		if err != nil {
			return err
		}
		project = deletedProject
		return nil
	})
	return project, err
}

func (s *Store) EnsureDefaultProject(tenantID string) (model.Project, error) {
	project, _, err := s.EnsureDefaultProjectWithStatus(tenantID)
	return project, err
}

func (s *Store) EnsureDefaultProjectWithStatus(tenantID string) (model.Project, bool, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return model.Project{}, false, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgEnsureDefaultProject(tenantID)
	}

	var project model.Project
	var created bool
	err := s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		for _, existing := range state.Projects {
			if existing.TenantID == tenantID && existing.Slug == "default" {
				project = existing
				return nil
			}
		}
		now := time.Now().UTC()
		project = model.Project{
			ID:          model.NewID("project"),
			TenantID:    tenantID,
			Name:        "default",
			Slug:        "default",
			Description: "default project",
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		state.Projects = append(state.Projects, project)
		created = true
		return nil
	})
	return project, created, err
}

func (s *Store) ListAPIKeys(tenantID string, platformAdmin bool) ([]model.APIKey, error) {
	if s.usingDatabase() {
		return s.pgListAPIKeys(tenantID, platformAdmin)
	}
	var keys []model.APIKey
	err := s.withLockedState(false, func(state *model.State) error {
		for _, key := range state.APIKeys {
			normalizeAPIKeyForRead(&key)
			if platformAdmin || key.TenantID == tenantID {
				keys = append(keys, redactAPIKey(key))
			}
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].CreatedAt.Before(keys[j].CreatedAt)
		})
		return nil
	})
	return keys, err
}

func (s *Store) GetAPIKey(id string) (model.APIKey, error) {
	if s.usingDatabase() {
		return s.pgGetAPIKey(id)
	}
	var key model.APIKey
	err := s.withLockedState(false, func(state *model.State) error {
		index := findAPIKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		key = state.APIKeys[index]
		normalizeAPIKeyForRead(&key)
		return nil
	})
	return key, err
}

func (s *Store) CreateAPIKey(tenantID, label string, scopes []string) (model.APIKey, string, error) {
	label = strings.TrimSpace(label)
	if label == "" {
		return model.APIKey{}, "", ErrInvalidInput
	}
	if len(scopes) == 0 {
		return model.APIKey{}, "", ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateAPIKey(tenantID, label, scopes)
	}

	secret := model.NewSecret("fugue_pk")
	key := model.APIKey{
		ID:        model.NewID("apikey"),
		TenantID:  tenantID,
		Label:     label,
		Prefix:    model.SecretPrefix(secret),
		Hash:      model.HashSecret(secret),
		Status:    model.APIKeyStatusActive,
		Scopes:    model.NormalizeScopes(scopes),
		CreatedAt: time.Now().UTC(),
	}

	err := s.withLockedState(true, func(state *model.State) error {
		if tenantID != "" && findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		state.APIKeys = append(state.APIKeys, key)
		return nil
	})
	if err != nil {
		return model.APIKey{}, "", err
	}

	return redactAPIKey(key), secret, nil
}

func (s *Store) UpdateAPIKey(id string, label *string, scopes *[]string) (model.APIKey, error) {
	id = strings.TrimSpace(id)
	if id == "" || (label == nil && scopes == nil) {
		return model.APIKey{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateAPIKey(id, label, scopes)
	}

	var key model.APIKey
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAPIKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		updated, err := applyAPIKeyUpdates(state.APIKeys[index], label, scopes)
		if err != nil {
			return err
		}
		state.APIKeys[index] = updated
		key = updated
		return nil
	})
	if err != nil {
		return model.APIKey{}, err
	}
	return redactAPIKey(key), nil
}

func (s *Store) RotateAPIKey(id string, label *string, scopes *[]string) (model.APIKey, string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.APIKey{}, "", ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRotateAPIKey(id, label, scopes)
	}

	secret := model.NewSecret("fugue_pk")
	var key model.APIKey
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAPIKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		updated, err := applyAPIKeyUpdates(state.APIKeys[index], label, scopes)
		if err != nil {
			return err
		}
		updated.Prefix = model.SecretPrefix(secret)
		updated.Hash = model.HashSecret(secret)
		state.APIKeys[index] = updated
		key = updated
		return nil
	})
	if err != nil {
		return model.APIKey{}, "", err
	}
	return redactAPIKey(key), secret, nil
}

func (s *Store) DisableAPIKey(id string) (model.APIKey, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.APIKey{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDisableAPIKey(id)
	}

	var key model.APIKey
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAPIKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		if normalizeAPIKeyStatus(state.APIKeys[index].Status) != model.APIKeyStatusDisabled || state.APIKeys[index].DisabledAt == nil {
			now := time.Now().UTC()
			state.APIKeys[index].Status = model.APIKeyStatusDisabled
			state.APIKeys[index].DisabledAt = &now
		}
		key = state.APIKeys[index]
		return nil
	})
	if err != nil {
		return model.APIKey{}, err
	}
	return redactAPIKey(key), nil
}

func (s *Store) EnableAPIKey(id string) (model.APIKey, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.APIKey{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgEnableAPIKey(id)
	}

	var key model.APIKey
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAPIKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		state.APIKeys[index].Status = model.APIKeyStatusActive
		state.APIKeys[index].DisabledAt = nil
		key = state.APIKeys[index]
		return nil
	})
	if err != nil {
		return model.APIKey{}, err
	}
	return redactAPIKey(key), nil
}

func (s *Store) DeleteAPIKey(id string) (model.APIKey, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.APIKey{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteAPIKey(id)
	}

	var key model.APIKey
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAPIKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		key = state.APIKeys[index]
		state.APIKeys = append(state.APIKeys[:index], state.APIKeys[index+1:]...)
		return nil
	})
	if err != nil {
		return model.APIKey{}, err
	}
	return redactAPIKey(key), nil
}

func (s *Store) AuthenticateAPIKey(secret string) (model.Principal, error) {
	if strings.TrimSpace(secret) == "" {
		return model.Principal{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgAuthenticateAPIKey(secret)
	}

	var principal model.Principal
	err := s.withLockedState(true, func(state *model.State) error {
		hash := model.HashSecret(secret)
		now := time.Now().UTC()
		for idx := range state.APIKeys {
			if state.APIKeys[idx].Hash != hash {
				continue
			}
			if normalizeAPIKeyStatus(state.APIKeys[idx].Status) != model.APIKeyStatusActive {
				return ErrNotFound
			}
			state.APIKeys[idx].LastUsedAt = &now
			scopes := make(map[string]struct{}, len(state.APIKeys[idx].Scopes))
			for _, scope := range state.APIKeys[idx].Scopes {
				scopes[scope] = struct{}{}
			}
			principal = model.Principal{
				ActorType: model.ActorTypeAPIKey,
				ActorID:   state.APIKeys[idx].ID,
				TenantID:  state.APIKeys[idx].TenantID,
				Scopes:    scopes,
			}
			return nil
		}
		return ErrNotFound
	})
	return principal, err
}

func (s *Store) ListEnrollmentTokens(tenantID string) ([]model.EnrollmentToken, error) {
	if s.usingDatabase() {
		return s.pgListEnrollmentTokens(tenantID)
	}
	var tokens []model.EnrollmentToken
	err := s.withLockedState(false, func(state *model.State) error {
		for _, token := range state.EnrollmentTokens {
			if token.TenantID == tenantID {
				tokens = append(tokens, redactEnrollmentToken(token))
			}
		}
		sort.Slice(tokens, func(i, j int) bool {
			return tokens[i].CreatedAt.Before(tokens[j].CreatedAt)
		})
		return nil
	})
	return tokens, err
}

func (s *Store) CreateEnrollmentToken(tenantID, label string, ttl time.Duration) (model.EnrollmentToken, string, error) {
	label = strings.TrimSpace(label)
	if tenantID == "" || label == "" || ttl <= 0 {
		return model.EnrollmentToken{}, "", ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateEnrollmentToken(tenantID, label, ttl)
	}

	secret := model.NewSecret("fugue_enroll")
	token := model.EnrollmentToken{
		ID:        model.NewID("enroll"),
		TenantID:  tenantID,
		Label:     label,
		Prefix:    model.SecretPrefix(secret),
		Hash:      model.HashSecret(secret),
		ExpiresAt: time.Now().UTC().Add(ttl),
		CreatedAt: time.Now().UTC(),
	}

	err := s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		state.EnrollmentTokens = append(state.EnrollmentTokens, token)
		return nil
	})
	if err != nil {
		return model.EnrollmentToken{}, "", err
	}

	return redactEnrollmentToken(token), secret, nil
}

func (s *Store) ListNodeKeys(tenantID string, platformAdmin bool) ([]model.NodeKey, error) {
	if s.usingDatabase() {
		return s.pgListNodeKeys(tenantID, platformAdmin)
	}
	var keys []model.NodeKey
	err := s.withLockedState(false, func(state *model.State) error {
		for _, key := range state.NodeKeys {
			if platformAdmin || key.TenantID == tenantID {
				keys = append(keys, redactNodeKey(key))
			}
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].CreatedAt.Before(keys[j].CreatedAt)
		})
		return nil
	})
	return keys, err
}

func (s *Store) GetNodeKey(id string) (model.NodeKey, error) {
	if s.usingDatabase() {
		return s.pgGetNodeKey(id)
	}
	var key model.NodeKey
	err := s.withLockedState(false, func(state *model.State) error {
		index := findNodeKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		key = state.NodeKeys[index]
		return nil
	})
	return key, err
}

func (s *Store) AuthenticateNodeKey(secret string) (model.NodeKey, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return model.NodeKey{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgAuthenticateNodeKey(secret)
	}

	var key model.NodeKey
	err := s.withLockedState(true, func(state *model.State) error {
		hash := model.HashSecret(secret)
		now := time.Now().UTC()
		keyIndex := -1
		for idx := range state.NodeKeys {
			if state.NodeKeys[idx].Hash != hash {
				continue
			}
			keyIndex = idx
			break
		}
		if keyIndex < 0 {
			return ErrNotFound
		}
		if state.NodeKeys[keyIndex].RevokedAt != nil || state.NodeKeys[keyIndex].Status == model.NodeKeyStatusRevoked {
			return ErrConflict
		}

		state.NodeKeys[keyIndex].LastUsedAt = &now
		state.NodeKeys[keyIndex].UpdatedAt = now
		key = state.NodeKeys[keyIndex]
		return nil
	})
	if err != nil {
		return model.NodeKey{}, err
	}
	return redactNodeKey(key), nil
}

func (s *Store) CreateNodeKey(tenantID, label string) (model.NodeKey, string, error) {
	return s.CreateScopedNodeKey(tenantID, label, model.NodeKeyScopeTenantRuntime)
}

func (s *Store) CreateScopedNodeKey(tenantID, label, scope string) (model.NodeKey, string, error) {
	label = defaultNodeKeyLabel(label)
	scope = model.NormalizeNodeKeyScope(scope)
	if scope == "" {
		return model.NodeKey{}, "", ErrInvalidInput
	}
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" && scope != model.NodeKeyScopePlatformNode {
		return model.NodeKey{}, "", ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateScopedNodeKey(tenantID, label, scope)
	}

	secret := model.NewSecret("fugue_nk")
	now := time.Now().UTC()
	key := model.NodeKey{
		ID:        model.NewID("nodekey"),
		TenantID:  tenantID,
		Label:     label,
		Prefix:    model.SecretPrefix(secret),
		Hash:      model.HashSecret(secret),
		Scope:     scope,
		Status:    model.NodeKeyStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}

	err := s.withLockedState(true, func(state *model.State) error {
		if tenantID != "" && findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		state.NodeKeys = append(state.NodeKeys, key)
		return nil
	})
	if err != nil {
		return model.NodeKey{}, "", err
	}

	return redactNodeKey(key), secret, nil
}

func (s *Store) RevokeNodeKey(id string) (model.NodeKey, error) {
	if s.usingDatabase() {
		return s.pgRevokeNodeKey(id)
	}
	var key model.NodeKey
	err := s.withLockedState(true, func(state *model.State) error {
		index := findNodeKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		if state.NodeKeys[index].RevokedAt == nil {
			state.NodeKeys[index].Status = model.NodeKeyStatusRevoked
			state.NodeKeys[index].RevokedAt = &now
			state.NodeKeys[index].UpdatedAt = now
		}
		key = state.NodeKeys[index]
		return nil
	})
	return redactNodeKey(key), err
}

func (s *Store) BootstrapNode(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.NodeKey, model.Runtime, string, error) {
	secret = strings.TrimSpace(secret)
	runtimeName = strings.TrimSpace(runtimeName)
	if secret == "" {
		return model.NodeKey{}, model.Runtime{}, "", ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgBootstrapNode(secret, runtimeName, endpoint, labels, machineName, machineFingerprint)
	}

	var key model.NodeKey
	var runtime model.Runtime
	var runtimeSecret string
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		hash := model.HashSecret(secret)
		now := time.Now().UTC()
		keyIndex := -1
		for idx := range state.NodeKeys {
			if state.NodeKeys[idx].Hash != hash {
				continue
			}
			keyIndex = idx
			break
		}
		if keyIndex < 0 {
			return ErrNotFound
		}
		if state.NodeKeys[keyIndex].RevokedAt != nil || state.NodeKeys[keyIndex].Status == model.NodeKeyStatusRevoked {
			return ErrConflict
		}

		state.NodeKeys[keyIndex].LastUsedAt = &now
		state.NodeKeys[keyIndex].UpdatedAt = now
		key = state.NodeKeys[keyIndex]

		explicitFingerprint := strings.TrimSpace(machineFingerprint) != ""
		machineName = normalizedMachineName(machineName, runtimeName, endpoint)
		machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, runtimeName, endpoint)
		fingerprintHash := model.HashSecret(machineFingerprint)

		sameTenantFingerprintIndex := findRuntimeByFingerprintHash(state, key.TenantID, fingerprintHash)
		runtimeIndex := sameTenantFingerprintIndex
		if runtimeIndex < 0 && explicitFingerprint {
			runtimeIndex = findRuntimeCandidate(state, key.TenantID, key.ID, model.RuntimeTypeExternalOwned, machineName, runtimeName, endpoint)
		}
		transferByFingerprint := false
		if explicitFingerprint {
			matchingIndexes := findRuntimeIndexesByFingerprintHash(state, fingerprintHash)
			keepFingerprintIndex := sameTenantFingerprintIndex
			if sameTenantFingerprintIndex < 0 && len(matchingIndexes) > 0 {
				transferByFingerprint = true
			}
			for _, matchIndex := range matchingIndexes {
				if matchIndex == keepFingerprintIndex {
					continue
				}
				detachRuntimeOwnership(state, matchIndex, now)
			}
		}

		runtimeSecret = model.NewSecret("fugue_rt")
		if runtimeIndex >= 0 {
			if transferByFingerprint {
				resetRuntimeSharing(state, state.Runtimes[runtimeIndex].ID)
				state.Runtimes[runtimeIndex].AccessMode = model.RuntimeAccessModePrivate
			}
			state.Runtimes[runtimeIndex].Type = model.RuntimeTypeExternalOwned
			state.Runtimes[runtimeIndex].Status = model.RuntimeStatusActive
			state.Runtimes[runtimeIndex].TenantID = key.TenantID
			state.Runtimes[runtimeIndex].Endpoint = strings.TrimSpace(endpoint)
			state.Runtimes[runtimeIndex].Labels = cloneMap(labels)
			state.Runtimes[runtimeIndex].NodeKeyID = key.ID
			state.Runtimes[runtimeIndex].AgentKeyPrefix = model.SecretPrefix(runtimeSecret)
			state.Runtimes[runtimeIndex].AgentKeyHash = model.HashSecret(runtimeSecret)
			state.Runtimes[runtimeIndex].LastHeartbeatAt = &now
			state.Runtimes[runtimeIndex].UpdatedAt = now
			applyRuntimeIdentity(&state.Runtimes[runtimeIndex], machineName, machineFingerprint, model.RuntimeTypeExternalOwned, strings.TrimSpace(endpoint), labels, key.ID, now)
			runtime = state.Runtimes[runtimeIndex]
			_ = upsertMachineForRuntimeState(state, runtime, now)
			return nil
		}

		runtime = model.Runtime{
			ID:              model.NewID("runtime"),
			TenantID:        key.TenantID,
			Name:            nextAvailableRuntimeName(state, key.TenantID, runtimeName),
			Type:            model.RuntimeTypeExternalOwned,
			AccessMode:      model.RuntimeAccessModePrivate,
			PoolMode:        model.RuntimePoolModeDedicated,
			Status:          model.RuntimeStatusActive,
			Endpoint:        strings.TrimSpace(endpoint),
			Labels:          cloneMap(labels),
			NodeKeyID:       key.ID,
			AgentKeyPrefix:  model.SecretPrefix(runtimeSecret),
			AgentKeyHash:    model.HashSecret(runtimeSecret),
			LastHeartbeatAt: &now,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		applyRuntimeIdentity(&runtime, machineName, machineFingerprint, model.RuntimeTypeExternalOwned, strings.TrimSpace(endpoint), labels, key.ID, now)
		state.Runtimes = append(state.Runtimes, runtime)
		_ = upsertMachineForRuntimeState(state, runtime, now)
		return nil
	})
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", err
	}

	return redactNodeKey(key), runtime, runtimeSecret, nil
}

func (s *Store) BootstrapClusterNode(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.NodeKey, model.Runtime, error) {
	secret = strings.TrimSpace(secret)
	runtimeName = strings.TrimSpace(runtimeName)
	if secret == "" {
		return model.NodeKey{}, model.Runtime{}, ErrInvalidInput
	}
	normalizedRuntimeName, err := normalizeClusterNodeName(runtimeName)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, err
	}
	runtimeName = normalizedRuntimeName
	if s.usingDatabase() {
		return s.pgBootstrapClusterNode(secret, runtimeName, endpoint, labels, machineName, machineFingerprint)
	}

	var key model.NodeKey
	var runtime model.Runtime
	err = s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		hash := model.HashSecret(secret)
		now := time.Now().UTC()
		keyIndex := -1
		for idx := range state.NodeKeys {
			if state.NodeKeys[idx].Hash != hash {
				continue
			}
			keyIndex = idx
			break
		}
		if keyIndex < 0 {
			return ErrNotFound
		}
		if state.NodeKeys[keyIndex].RevokedAt != nil || state.NodeKeys[keyIndex].Status == model.NodeKeyStatusRevoked {
			return ErrConflict
		}

		state.NodeKeys[keyIndex].LastUsedAt = &now
		state.NodeKeys[keyIndex].UpdatedAt = now
		key = state.NodeKeys[keyIndex]

		explicitFingerprint := strings.TrimSpace(machineFingerprint) != ""
		machineName = normalizedMachineName(machineName, runtimeName, endpoint)
		machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, runtimeName, endpoint)
		fingerprintHash := model.HashSecret(machineFingerprint)

		sameTenantFingerprintIndex := findRuntimeByFingerprintHash(state, key.TenantID, fingerprintHash)
		runtimeIndex := sameTenantFingerprintIndex
		if runtimeIndex < 0 && explicitFingerprint {
			runtimeIndex = findRuntimeCandidate(state, key.TenantID, key.ID, model.RuntimeTypeManagedOwned, machineName, runtimeName, endpoint)
		}
		transferByFingerprint := false
		if explicitFingerprint {
			matchingIndexes := findRuntimeIndexesByFingerprintHash(state, fingerprintHash)
			keepFingerprintIndex := sameTenantFingerprintIndex
			if sameTenantFingerprintIndex < 0 && len(matchingIndexes) > 0 {
				transferByFingerprint = true
			}
			for _, matchIndex := range matchingIndexes {
				if matchIndex == keepFingerprintIndex {
					continue
				}
				detachRuntimeOwnership(state, matchIndex, now)
			}
		}

		if runtimeIndex >= 0 {
			if transferByFingerprint {
				resetRuntimeSharing(state, state.Runtimes[runtimeIndex].ID)
				state.Runtimes[runtimeIndex].AccessMode = model.RuntimeAccessModePrivate
			}
			if runtimeName != "" {
				state.Runtimes[runtimeIndex].Name = runtimeName
			}
			state.Runtimes[runtimeIndex].Type = model.RuntimeTypeManagedOwned
			state.Runtimes[runtimeIndex].Status = model.RuntimeStatusActive
			state.Runtimes[runtimeIndex].TenantID = key.TenantID
			state.Runtimes[runtimeIndex].Endpoint = strings.TrimSpace(endpoint)
			state.Runtimes[runtimeIndex].Labels = cloneMap(labels)
			state.Runtimes[runtimeIndex].NodeKeyID = key.ID
			state.Runtimes[runtimeIndex].AgentKeyPrefix = ""
			state.Runtimes[runtimeIndex].AgentKeyHash = ""
			state.Runtimes[runtimeIndex].UpdatedAt = now
			state.Runtimes[runtimeIndex].LastHeartbeatAt = &now
			applyRuntimeIdentity(&state.Runtimes[runtimeIndex], machineName, machineFingerprint, model.RuntimeTypeManagedOwned, strings.TrimSpace(endpoint), labels, key.ID, now)
			runtime = state.Runtimes[runtimeIndex]
			_ = upsertMachineForRuntimeState(state, runtime, now)
			return nil
		}

		runtime = model.Runtime{
			ID:              model.NewID("runtime"),
			TenantID:        key.TenantID,
			Name:            nextAvailableRuntimeName(state, key.TenantID, runtimeName),
			Type:            model.RuntimeTypeManagedOwned,
			AccessMode:      model.RuntimeAccessModePrivate,
			PoolMode:        model.RuntimePoolModeDedicated,
			Status:          model.RuntimeStatusActive,
			Endpoint:        strings.TrimSpace(endpoint),
			Labels:          cloneMap(labels),
			NodeKeyID:       key.ID,
			LastHeartbeatAt: &now,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		applyRuntimeIdentity(&runtime, machineName, machineFingerprint, model.RuntimeTypeManagedOwned, strings.TrimSpace(endpoint), labels, key.ID, now)
		state.Runtimes = append(state.Runtimes, runtime)
		_ = upsertMachineForRuntimeState(state, runtime, now)
		return nil
	})
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, err
	}

	return redactNodeKey(key), runtime, nil
}

func (s *Store) CreateRuntime(tenantID, name, runtimeType, endpoint string, labels map[string]string) (model.Runtime, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return model.Runtime{}, "", ErrInvalidInput
	}
	if runtimeType == "" {
		runtimeType = model.RuntimeTypeExternalOwned
	}
	if runtimeType != model.RuntimeTypeManagedShared && runtimeType != model.RuntimeTypeManagedOwned && runtimeType != model.RuntimeTypeExternalOwned {
		return model.Runtime{}, "", ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateRuntime(tenantID, name, runtimeType, endpoint, labels)
	}

	secret := model.NewSecret("fugue_rt")
	now := time.Now().UTC()
	runtime := model.Runtime{
		ID:             model.NewID("runtime"),
		TenantID:       tenantID,
		Name:           name,
		Type:           runtimeType,
		AccessMode:     normalizeRuntimeAccessMode(runtimeType, ""),
		PoolMode:       model.NormalizeRuntimePoolMode(runtimeType, ""),
		Status:         model.RuntimeStatusPending,
		Endpoint:       strings.TrimSpace(endpoint),
		Labels:         cloneMap(labels),
		AgentKeyPrefix: model.SecretPrefix(secret),
		AgentKeyHash:   model.HashSecret(secret),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if runtimeType == model.RuntimeTypeManagedShared || runtimeType == model.RuntimeTypeManagedOwned {
		runtime.Status = model.RuntimeStatusActive
	}

	err := s.withLockedState(true, func(state *model.State) error {
		if tenantID != "" && findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		for _, existing := range state.Runtimes {
			if existing.TenantID == tenantID && strings.EqualFold(existing.Name, name) {
				return ErrConflict
			}
		}
		state.Runtimes = append(state.Runtimes, runtime)
		return nil
	})
	if err != nil {
		return model.Runtime{}, "", err
	}

	return runtime, secret, nil
}

func (s *Store) ConsumeEnrollmentToken(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Runtime, string, error) {
	secret = strings.TrimSpace(secret)
	runtimeName = strings.TrimSpace(runtimeName)
	if secret == "" {
		return model.Runtime{}, "", ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgConsumeEnrollmentToken(secret, runtimeName, endpoint, labels, machineName, machineFingerprint)
	}

	var runtime model.Runtime
	var runtimeSecret string
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		hash := model.HashSecret(secret)
		now := time.Now().UTC()
		tokenIndex := -1
		for idx := range state.EnrollmentTokens {
			if state.EnrollmentTokens[idx].Hash != hash {
				continue
			}
			tokenIndex = idx
			break
		}
		if tokenIndex < 0 {
			return ErrNotFound
		}
		token := &state.EnrollmentTokens[tokenIndex]
		if token.UsedAt != nil || token.ExpiresAt.Before(now) {
			return ErrConflict
		}
		token.UsedAt = &now
		token.LastUsedAt = &now

		explicitFingerprint := strings.TrimSpace(machineFingerprint) != ""
		machineName = normalizedMachineName(machineName, runtimeName, endpoint)
		machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, runtimeName, endpoint)
		fingerprintHash := model.HashSecret(machineFingerprint)

		runtimeIndex := findRuntimeByFingerprintHash(state, token.TenantID, fingerprintHash)
		if runtimeIndex < 0 && explicitFingerprint {
			runtimeIndex = findRuntimeCandidate(state, token.TenantID, "", model.RuntimeTypeExternalOwned, machineName, runtimeName, endpoint)
		}

		runtimeSecret = model.NewSecret("fugue_rt")
		if runtimeIndex >= 0 {
			state.Runtimes[runtimeIndex].Type = model.RuntimeTypeExternalOwned
			state.Runtimes[runtimeIndex].Status = model.RuntimeStatusActive
			state.Runtimes[runtimeIndex].Endpoint = strings.TrimSpace(endpoint)
			state.Runtimes[runtimeIndex].Labels = cloneMap(labels)
			state.Runtimes[runtimeIndex].AgentKeyPrefix = model.SecretPrefix(runtimeSecret)
			state.Runtimes[runtimeIndex].AgentKeyHash = model.HashSecret(runtimeSecret)
			state.Runtimes[runtimeIndex].LastHeartbeatAt = &now
			state.Runtimes[runtimeIndex].UpdatedAt = now
			applyRuntimeIdentity(&state.Runtimes[runtimeIndex], machineName, machineFingerprint, model.RuntimeTypeExternalOwned, strings.TrimSpace(endpoint), labels, state.Runtimes[runtimeIndex].NodeKeyID, now)
			runtime = state.Runtimes[runtimeIndex]
			_ = upsertMachineForRuntimeState(state, runtime, now)
			return nil
		}

		runtime = model.Runtime{
			ID:              model.NewID("runtime"),
			TenantID:        token.TenantID,
			Name:            nextAvailableRuntimeName(state, token.TenantID, runtimeName),
			Type:            model.RuntimeTypeExternalOwned,
			AccessMode:      model.RuntimeAccessModePrivate,
			PoolMode:        model.RuntimePoolModeDedicated,
			Status:          model.RuntimeStatusActive,
			Endpoint:        strings.TrimSpace(endpoint),
			Labels:          cloneMap(labels),
			AgentKeyPrefix:  model.SecretPrefix(runtimeSecret),
			AgentKeyHash:    model.HashSecret(runtimeSecret),
			LastHeartbeatAt: &now,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		applyRuntimeIdentity(&runtime, machineName, machineFingerprint, model.RuntimeTypeExternalOwned, strings.TrimSpace(endpoint), labels, "", now)
		state.Runtimes = append(state.Runtimes, runtime)
		_ = upsertMachineForRuntimeState(state, runtime, now)
		return nil
	})
	if err != nil {
		return model.Runtime{}, "", err
	}

	return runtime, runtimeSecret, nil
}

func (s *Store) AuthenticateRuntimeKey(secret string) (model.Runtime, model.Principal, error) {
	if strings.TrimSpace(secret) == "" {
		return model.Runtime{}, model.Principal{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgAuthenticateRuntimeKey(secret)
	}

	var runtime model.Runtime
	var principal model.Principal
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		hash := model.HashSecret(secret)
		now := time.Now().UTC()
		for idx := range state.Runtimes {
			if state.Runtimes[idx].AgentKeyHash != hash {
				continue
			}
			state.Runtimes[idx].LastHeartbeatAt = &now
			state.Runtimes[idx].LastSeenAt = &now
			state.Runtimes[idx].Status = model.RuntimeStatusActive
			state.Runtimes[idx].UpdatedAt = now
			runtime = state.Runtimes[idx]
			principal = model.Principal{
				ActorType: model.ActorTypeRuntime,
				ActorID:   runtime.ID,
				TenantID:  runtime.TenantID,
				Scopes: map[string]struct{}{
					"runtime.agent": {},
				},
			}
			return nil
		}
		return ErrNotFound
	})
	return runtime, principal, err
}

func (s *Store) UpdateRuntimeHeartbeat(runtimeID, endpoint string) (model.Runtime, error) {
	if s.usingDatabase() {
		return s.pgUpdateRuntimeHeartbeat(runtimeID, endpoint)
	}
	var runtime model.Runtime
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		index := findRuntime(state, runtimeID)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		state.Runtimes[index].LastHeartbeatAt = &now
		state.Runtimes[index].LastSeenAt = &now
		state.Runtimes[index].UpdatedAt = now
		state.Runtimes[index].Status = model.RuntimeStatusActive
		if strings.TrimSpace(endpoint) != "" {
			state.Runtimes[index].Endpoint = strings.TrimSpace(endpoint)
		}
		runtime = state.Runtimes[index]
		return nil
	})
	return runtime, err
}

func (s *Store) UpdateRuntimeHeartbeatWithLabels(runtimeID, endpoint string, labels map[string]string) (model.Runtime, error) {
	if len(labels) == 0 {
		return s.UpdateRuntimeHeartbeat(runtimeID, endpoint)
	}
	if s.usingDatabase() {
		return s.pgUpdateRuntimeHeartbeatWithLabels(runtimeID, endpoint, labels)
	}
	var runtime model.Runtime
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		index := findRuntime(state, runtimeID)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		state.Runtimes[index].LastHeartbeatAt = &now
		state.Runtimes[index].LastSeenAt = &now
		state.Runtimes[index].UpdatedAt = now
		state.Runtimes[index].Status = model.RuntimeStatusActive
		if strings.TrimSpace(endpoint) != "" {
			state.Runtimes[index].Endpoint = strings.TrimSpace(endpoint)
		}
		state.Runtimes[index].Labels = mergeRuntimeHeartbeatLabels(state.Runtimes[index].Labels, labels)
		runtime = state.Runtimes[index]
		return nil
	})
	return runtime, err
}

func mergeRuntimeHeartbeatLabels(current, incoming map[string]string) map[string]string {
	merged := cloneMap(current)
	if merged == nil {
		merged = map[string]string{}
	}
	for key := range merged {
		if strings.HasPrefix(key, runtimepkg.CellRuntimeLabelPrefix) {
			delete(merged, key)
		}
	}
	for key, value := range incoming {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		if !strings.HasPrefix(key, runtimepkg.CellRuntimeLabelPrefix) {
			continue
		}
		merged[key] = value
	}
	return merged
}

func (s *Store) MarkRuntimeOfflineStale(after time.Duration) (int, error) {
	if s.usingDatabase() {
		return s.pgMarkRuntimeOfflineStale(after)
	}
	var count int
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		if after <= 0 {
			return nil
		}
		threshold := time.Now().UTC().Add(-after)
		for idx := range state.Runtimes {
			runtime := &state.Runtimes[idx]
			if runtime.Type == model.RuntimeTypeManagedShared || runtime.Type == model.RuntimeTypeManagedOwned {
				continue
			}
			if runtime.LastHeartbeatAt == nil || runtime.LastHeartbeatAt.Before(threshold) {
				if runtime.Status != model.RuntimeStatusOffline {
					runtime.Status = model.RuntimeStatusOffline
					runtime.UpdatedAt = time.Now().UTC()
					count++
				}
			}
		}
		return nil
	})
	return count, err
}

func (s *Store) ListNodes(tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	if s.usingDatabase() {
		return s.pgListNodes(tenantID, platformAdmin)
	}
	var nodes []model.Runtime
	err := s.withLockedState(false, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		for _, runtime := range state.Runtimes {
			if runtime.Type != model.RuntimeTypeExternalOwned && runtime.Type != model.RuntimeTypeManagedOwned {
				continue
			}
			if platformAdmin || runtimeVisibleToTenant(state, runtime.ID, tenantID) {
				nodes = append(nodes, runtime)
			}
		}
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].CreatedAt.Before(nodes[j].CreatedAt)
		})
		return nil
	})
	return nodes, err
}

func (s *Store) ListRuntimesByNodeKey(nodeKeyID, tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	nodeKeyID = strings.TrimSpace(nodeKeyID)
	if nodeKeyID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListRuntimesByNodeKey(nodeKeyID, tenantID, platformAdmin)
	}

	var runtimes []model.Runtime
	err := s.withLockedState(false, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		for _, runtime := range state.Runtimes {
			if runtime.NodeKeyID != nodeKeyID {
				continue
			}
			if platformAdmin || runtime.TenantID == tenantID {
				runtimes = append(runtimes, runtime)
			}
		}
		sort.Slice(runtimes, func(i, j int) bool {
			return runtimes[i].CreatedAt.Before(runtimes[j].CreatedAt)
		})
		return nil
	})
	return runtimes, err
}

func (s *Store) ListRuntimes(tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	if s.usingDatabase() {
		return s.pgListRuntimes(tenantID, platformAdmin)
	}
	var runtimes []model.Runtime
	err := s.withLockedState(false, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		for _, runtime := range state.Runtimes {
			if platformAdmin || runtimeVisibleToTenant(state, runtime.ID, tenantID) {
				runtimes = append(runtimes, runtime)
			}
		}
		sort.Slice(runtimes, func(i, j int) bool {
			return runtimes[i].CreatedAt.Before(runtimes[j].CreatedAt)
		})
		return nil
	})
	return runtimes, err
}

func (s *Store) GetRuntime(id string) (model.Runtime, error) {
	if s.usingDatabase() {
		return s.pgGetRuntime(id)
	}
	var runtime model.Runtime
	err := s.withLockedState(false, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		index := findRuntime(state, id)
		if index < 0 {
			return ErrNotFound
		}
		runtime = state.Runtimes[index]
		return nil
	})
	return runtime, err
}

func (s *Store) DetachRuntimeOwnership(runtimeID string) (model.Runtime, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return model.Runtime{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDetachRuntimeOwnership(runtimeID)
	}

	var runtime model.Runtime
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		index := findRuntime(state, runtimeID)
		if index < 0 {
			return ErrNotFound
		}
		detachRuntimeOwnership(state, index, time.Now().UTC())
		runtime = state.Runtimes[index]
		return nil
	})
	return runtime, err
}

func (s *Store) DeleteRuntime(runtimeID string) (model.Runtime, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return model.Runtime{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteRuntime(runtimeID)
	}

	var runtime model.Runtime
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		index := findRuntime(state, runtimeID)
		if index < 0 {
			return ErrNotFound
		}

		candidate := state.Runtimes[index]
		if err := validateRuntimeDeletion(candidate); err != nil {
			return err
		}
		if runtimeHasDeleteBlockersState(state, runtimeID) {
			return fmt.Errorf("%w: runtime still has apps, services, or active operations", ErrConflict)
		}

		state.RuntimeGrants = deleteRuntimeAccessGrantsByRuntime(
			state.RuntimeGrants,
			runtimeID,
		)
		state.ProjectRuntimeReservations = deleteProjectRuntimeReservationsByRuntime(
			state.ProjectRuntimeReservations,
			runtimeID,
		)
		runtime = candidate
		state.Runtimes = append(state.Runtimes[:index], state.Runtimes[index+1:]...)
		return nil
	})
	return runtime, err
}

func (s *Store) RuntimeVisibleToTenant(runtimeID, tenantID string, platformAdmin bool) (bool, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return false, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRuntimeVisibleToTenant(runtimeID, tenantID, platformAdmin)
	}

	var visible bool
	err := s.withLockedState(false, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		if platformAdmin {
			visible = findRuntime(state, runtimeID) >= 0
			return nil
		}
		visible = runtimeVisibleToTenant(state, runtimeID, tenantID)
		return nil
	})
	return visible, err
}

func (s *Store) ListRuntimeAccessGrants(runtimeID string) ([]model.RuntimeAccessGrant, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListRuntimeAccessGrants(runtimeID)
	}

	var grants []model.RuntimeAccessGrant
	err := s.withLockedState(false, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		if findRuntime(state, runtimeID) < 0 {
			return ErrNotFound
		}
		for _, grant := range state.RuntimeGrants {
			if grant.RuntimeID != runtimeID {
				continue
			}
			grants = append(grants, grant)
		}
		sort.Slice(grants, func(i, j int) bool {
			return grants[i].CreatedAt.Before(grants[j].CreatedAt)
		})
		return nil
	})
	return grants, err
}

func (s *Store) GrantRuntimeAccess(runtimeID, ownerTenantID, granteeTenantID string) (model.RuntimeAccessGrant, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	ownerTenantID = strings.TrimSpace(ownerTenantID)
	granteeTenantID = strings.TrimSpace(granteeTenantID)
	if runtimeID == "" || ownerTenantID == "" || granteeTenantID == "" {
		return model.RuntimeAccessGrant{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGrantRuntimeAccess(runtimeID, ownerTenantID, granteeTenantID)
	}

	var grant model.RuntimeAccessGrant
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		runtimeIndex := findRuntime(state, runtimeID)
		if runtimeIndex < 0 {
			return ErrNotFound
		}
		runtime := state.Runtimes[runtimeIndex]
		if runtime.TenantID != ownerTenantID || runtime.TenantID == "" {
			return ErrNotFound
		}
		if runtime.Type == model.RuntimeTypeManagedShared {
			return ErrInvalidInput
		}
		if granteeTenantID == runtime.TenantID {
			return ErrInvalidInput
		}
		if findTenant(state, granteeTenantID) < 0 {
			return ErrNotFound
		}
		if index := findRuntimeAccessGrant(state, runtimeID, granteeTenantID); index >= 0 {
			state.RuntimeGrants[index].UpdatedAt = time.Now().UTC()
			grant = state.RuntimeGrants[index]
			return nil
		}
		now := time.Now().UTC()
		grant = model.RuntimeAccessGrant{
			RuntimeID: runtimeID,
			TenantID:  granteeTenantID,
			CreatedAt: now,
			UpdatedAt: now,
		}
		state.RuntimeGrants = append(state.RuntimeGrants, grant)
		return nil
	})
	return grant, err
}

func (s *Store) RevokeRuntimeAccess(runtimeID, ownerTenantID, granteeTenantID string) (bool, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	ownerTenantID = strings.TrimSpace(ownerTenantID)
	granteeTenantID = strings.TrimSpace(granteeTenantID)
	if runtimeID == "" || ownerTenantID == "" || granteeTenantID == "" {
		return false, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRevokeRuntimeAccess(runtimeID, ownerTenantID, granteeTenantID)
	}

	var removed bool
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		runtimeIndex := findRuntime(state, runtimeID)
		if runtimeIndex < 0 {
			return ErrNotFound
		}
		runtime := state.Runtimes[runtimeIndex]
		if runtime.TenantID != ownerTenantID || runtime.TenantID == "" {
			return ErrNotFound
		}
		index := findRuntimeAccessGrant(state, runtimeID, granteeTenantID)
		if index < 0 {
			return nil
		}
		state.RuntimeGrants = append(state.RuntimeGrants[:index], state.RuntimeGrants[index+1:]...)
		removed = true
		return nil
	})
	return removed, err
}

func (s *Store) SetRuntimeAccessMode(runtimeID, ownerTenantID, accessMode string) (model.Runtime, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	ownerTenantID = strings.TrimSpace(ownerTenantID)
	accessMode = strings.TrimSpace(accessMode)
	if runtimeID == "" || ownerTenantID == "" {
		return model.Runtime{}, ErrInvalidInput
	}
	switch accessMode {
	case model.RuntimeAccessModePrivate, model.RuntimeAccessModePublic, model.RuntimeAccessModePlatformShared:
	default:
		return model.Runtime{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgSetRuntimeAccessMode(runtimeID, ownerTenantID, accessMode)
	}

	var runtime model.Runtime
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		index := findRuntime(state, runtimeID)
		if index < 0 {
			return ErrNotFound
		}
		if state.Runtimes[index].TenantID != ownerTenantID || state.Runtimes[index].TenantID == "" {
			return ErrNotFound
		}
		if state.Runtimes[index].Type == model.RuntimeTypeManagedShared {
			return ErrInvalidInput
		}
		state.Runtimes[index].AccessMode = normalizeRuntimeAccessMode(state.Runtimes[index].Type, accessMode)
		state.Runtimes[index].UpdatedAt = time.Now().UTC()
		runtime = state.Runtimes[index]
		return nil
	})
	return runtime, err
}

func (s *Store) SetRuntimePublicOffer(runtimeID, ownerTenantID string, offer model.RuntimePublicOffer) (model.Runtime, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	ownerTenantID = strings.TrimSpace(ownerTenantID)
	if runtimeID == "" || ownerTenantID == "" {
		return model.Runtime{}, ErrInvalidInput
	}
	normalizedOffer, err := normalizeRuntimePublicOffer(offer)
	if err != nil {
		return model.Runtime{}, err
	}
	if s.usingDatabase() {
		return s.pgSetRuntimePublicOffer(runtimeID, ownerTenantID, normalizedOffer)
	}

	var runtime model.Runtime
	err = s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		index := findRuntime(state, runtimeID)
		if index < 0 {
			return ErrNotFound
		}
		if state.Runtimes[index].TenantID != ownerTenantID || state.Runtimes[index].TenantID == "" {
			return ErrNotFound
		}
		if state.Runtimes[index].Type == model.RuntimeTypeManagedShared {
			return ErrInvalidInput
		}
		state.Runtimes[index].PublicOffer = cloneRuntimePublicOffer(&normalizedOffer)
		state.Runtimes[index].UpdatedAt = time.Now().UTC()
		runtime = state.Runtimes[index]
		return nil
	})
	return runtime, err
}

func (s *Store) SetRuntimePoolMode(runtimeID, poolMode string) (model.Runtime, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return model.Runtime{}, ErrInvalidInput
	}
	switch strings.TrimSpace(poolMode) {
	case model.RuntimePoolModeDedicated, model.RuntimePoolModeInternalShared:
	default:
		return model.Runtime{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgSetRuntimePoolMode(runtimeID, poolMode)
	}

	var runtime model.Runtime
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		now := time.Now().UTC()

		index := findRuntime(state, runtimeID)
		if index < 0 {
			return ErrNotFound
		}
		if state.Runtimes[index].Type != model.RuntimeTypeManagedOwned || state.Runtimes[index].TenantID == "" {
			return ErrInvalidInput
		}
		state.Runtimes[index].PoolMode = model.NormalizeRuntimePoolMode(state.Runtimes[index].Type, poolMode)
		state.Runtimes[index].UpdatedAt = now
		runtime = state.Runtimes[index]
		_ = upsertMachineForRuntimeState(state, runtime, now)
		return nil
	})
	return runtime, err
}

func (s *Store) EnsureManagedSharedLocationLabels(labels map[string]string) (model.Runtime, bool, error) {
	labels = normalizeManagedSharedLocationLabels(labels)
	if s.usingDatabase() {
		return s.pgEnsureManagedSharedLocationLabels(labels)
	}

	var runtimeObj model.Runtime
	var changed bool
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		index := findRuntime(state, "runtime_managed_shared")
		if index < 0 {
			return ErrNotFound
		}
		runtimeObj = state.Runtimes[index]
		if len(labels) == 0 || len(runtimepkg.PlacementNodeSelector(runtimeObj)) > 0 {
			return nil
		}

		runtimeLabels := cloneMap(runtimeObj.Labels)
		if runtimeLabels == nil {
			runtimeLabels = map[string]string{}
		}
		for key, value := range labels {
			runtimeLabels[key] = value
		}
		state.Runtimes[index].Labels = runtimeLabels
		state.Runtimes[index].UpdatedAt = time.Now().UTC()
		runtimeObj = state.Runtimes[index]
		changed = true
		return nil
	})
	return runtimeObj, changed, err
}

func normalizeManagedSharedLocationLabels(labels map[string]string) map[string]string {
	if value := strings.TrimSpace(labels[runtimepkg.LocationCountryCodeLabelKey]); value != "" {
		return map[string]string{
			runtimepkg.LocationCountryCodeLabelKey: strings.ToLower(value),
		}
	}
	if value := strings.TrimSpace(labels[runtimepkg.RegionLabelKey]); value != "" {
		return map[string]string{
			runtimepkg.RegionLabelKey: value,
		}
	}
	return nil
}

func (s *Store) ListApps(tenantID string, platformAdmin bool) ([]model.App, error) {
	if s.usingDatabase() {
		return s.pgListApps(tenantID, platformAdmin)
	}
	return s.listAppsView(tenantID, platformAdmin, true)
}

// ListAppsMetadata returns app records without hydrating backing services or bindings.
// Callers that only need routing, source, spec, and status should prefer this lighter view.
func (s *Store) ListAppsMetadata(tenantID string, platformAdmin bool) ([]model.App, error) {
	if s.usingDatabase() {
		return s.pgListAppsMetadata(tenantID, platformAdmin)
	}
	return s.listAppsView(tenantID, platformAdmin, false)
}

func (s *Store) ListAppsMetadataByIDs(appIDs []string) ([]model.App, error) {
	appIDSet := trimmedStringSet(appIDs)
	if len(appIDSet) == 0 {
		return nil, nil
	}
	if s.usingDatabase() {
		return s.pgListAppsMetadataByIDs(sortedTrimmedStringKeys(appIDSet))
	}
	return s.listAppsViewFiltered(false, func(app model.App) bool {
		_, ok := appIDSet[strings.TrimSpace(app.ID)]
		return ok
	})
}

func (s *Store) ListAppsMetadataByProjectIDs(projectIDs []string) ([]model.App, error) {
	projectIDSet := trimmedStringSet(projectIDs)
	if len(projectIDSet) == 0 {
		return nil, nil
	}
	if s.usingDatabase() {
		return s.pgListAppsMetadataByProjectIDs(sortedTrimmedStringKeys(projectIDSet))
	}
	return s.listAppsViewFiltered(false, func(app model.App) bool {
		_, ok := projectIDSet[strings.TrimSpace(app.ProjectID)]
		return ok
	})
}

func (s *Store) ListAppsByProjectIDs(projectIDs []string) ([]model.App, error) {
	projectIDSet := trimmedStringSet(projectIDs)
	if len(projectIDSet) == 0 {
		return nil, nil
	}
	if s.usingDatabase() {
		return s.pgListAppsByProjectIDs(sortedTrimmedStringKeys(projectIDSet))
	}
	return s.listAppsViewFiltered(true, func(app model.App) bool {
		_, ok := projectIDSet[strings.TrimSpace(app.ProjectID)]
		return ok
	})
}

func (s *Store) listAppsView(tenantID string, platformAdmin bool, hydrateBackingServices bool) ([]model.App, error) {
	return s.listAppsViewFiltered(hydrateBackingServices, func(app model.App) bool {
		return platformAdmin || app.TenantID == tenantID
	})
}

func (s *Store) listAppsViewFiltered(hydrateBackingServices bool, include func(model.App) bool) ([]model.App, error) {
	var apps []model.App
	err := s.withLockedState(false, func(state *model.State) error {
		for _, app := range state.Apps {
			if isDeletedApp(app) {
				continue
			}
			normalizeAppStatusForRead(&app)
			if hydrateBackingServices {
				hydrateAppBackingServices(state, &app)
			} else {
				app.Bindings = nil
				app.BackingServices = nil
			}
			if include == nil || include(app) {
				apps = append(apps, app)
			}
		}
		sort.Slice(apps, func(i, j int) bool {
			return apps[i].CreatedAt.Before(apps[j].CreatedAt)
		})
		return nil
	})
	return apps, err
}

func (s *Store) GetApp(id string) (model.App, error) {
	if s.usingDatabase() {
		return s.pgGetApp(id)
	}
	var app model.App
	err := s.withLockedState(false, func(state *model.State) error {
		index := findApp(state, id)
		if index < 0 {
			return ErrNotFound
		}
		app = state.Apps[index]
		normalizeAppStatusForRead(&app)
		hydrateAppBackingServices(state, &app)
		if isDeletedApp(app) {
			return ErrNotFound
		}
		return nil
	})
	return app, err
}

func (s *Store) GetAppMetadata(id string) (model.App, error) {
	if s.usingDatabase() {
		return s.pgGetAppMetadata(id)
	}
	var app model.App
	err := s.withLockedState(false, func(state *model.State) error {
		index := findApp(state, id)
		if index < 0 {
			return ErrNotFound
		}
		app = state.Apps[index]
		normalizeAppStatusForRead(&app)
		app.Bindings = nil
		app.BackingServices = nil
		if isDeletedApp(app) {
			return ErrNotFound
		}
		return nil
	})
	return app, err
}

func (s *Store) GetAppByHostname(hostname string) (model.App, error) {
	hostname = normalizeAppDomainHostname(hostname)
	if s.usingDatabase() {
		return s.pgGetAppByHostname(hostname)
	}
	var app model.App
	err := s.withLockedState(false, func(state *model.State) error {
		for _, candidate := range state.Apps {
			if isDeletedApp(candidate) {
				continue
			}
			if candidate.Route == nil {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(candidate.Route.Hostname), hostname) {
				app = candidate
				normalizeAppStatusForRead(&app)
				hydrateAppBackingServices(state, &app)
				return nil
			}
		}
		if domain, found := activeAppDomainByHostname(state, hostname); found {
			index := findApp(state, domain.AppID)
			if index >= 0 {
				app = state.Apps[index]
				normalizeAppStatusForRead(&app)
				hydrateAppBackingServices(state, &app)
				if !isDeletedApp(app) {
					return nil
				}
			}
		}
		return ErrNotFound
	})
	return app, err
}

func (s *Store) CreateApp(tenantID, projectID, name, description string, spec model.AppSpec) (model.App, error) {
	return s.createApp(tenantID, projectID, name, description, spec, nil, nil)
}

func (s *Store) CreateAppWithRoute(tenantID, projectID, name, description string, spec model.AppSpec, route model.AppRoute) (model.App, error) {
	return s.createApp(tenantID, projectID, name, description, spec, nil, &route)
}

func (s *Store) CreateImportedApp(tenantID, projectID, name, description string, spec model.AppSpec, source model.AppSource, route model.AppRoute) (model.App, error) {
	return s.createApp(tenantID, projectID, name, description, spec, &source, &route)
}

func (s *Store) CreateImportedAppWithoutRoute(tenantID, projectID, name, description string, spec model.AppSpec, source model.AppSource) (model.App, error) {
	return s.createApp(tenantID, projectID, name, description, spec, &source, nil)
}

func (s *Store) UpdateAppRoute(id string, route model.AppRoute) (model.App, error) {
	id = strings.TrimSpace(id)
	route.Hostname = strings.TrimSpace(strings.ToLower(route.Hostname))
	route.BaseDomain = strings.TrimSpace(strings.ToLower(route.BaseDomain))
	route.PublicURL = strings.TrimSpace(route.PublicURL)
	if id == "" || route.Hostname == "" {
		return model.App{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateAppRoute(id, route)
	}

	var app model.App
	err := s.withLockedState(true, func(state *model.State) error {
		index := findApp(state, id)
		if index < 0 {
			return ErrNotFound
		}
		app = state.Apps[index]
		if isDeletedApp(app) {
			return ErrNotFound
		}
		for _, existing := range state.Apps {
			if existing.ID == app.ID || isDeletedApp(existing) || existing.Route == nil {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(existing.Route.Hostname), route.Hostname) {
				return ErrConflict
			}
		}
		if route.BaseDomain == "" && app.Route != nil {
			route.BaseDomain = strings.TrimSpace(strings.ToLower(app.Route.BaseDomain))
		}
		if route.PublicURL == "" {
			route.PublicURL = "https://" + route.Hostname
		}
		if route.ServicePort <= 0 {
			if app.Route != nil && app.Route.ServicePort > 0 {
				route.ServicePort = app.Route.ServicePort
			}
			if route.ServicePort <= 0 {
				route.ServicePort = model.AppPublicServicePort(app.Spec)
			}
		}
		app.Route = cloneAppRoute(&route)
		app.UpdatedAt = time.Now().UTC()
		state.Apps[index] = app
		normalizeAppStatusForRead(&app)
		hydrateAppBackingServices(state, &app)
		return nil
	})
	return app, err
}

func (s *Store) UpdateAppImageMirrorLimit(id string, limit int) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" || limit < 0 {
		return model.App{}, ErrInvalidInput
	}
	limit = model.EffectiveAppImageMirrorLimit(limit)
	if s.usingDatabase() {
		return s.pgUpdateAppImageMirrorLimit(id, limit)
	}

	var app model.App
	err := s.withLockedState(true, func(state *model.State) error {
		index := findApp(state, id)
		if index < 0 {
			return ErrNotFound
		}
		app = state.Apps[index]
		if isDeletedApp(app) {
			return ErrNotFound
		}
		app.Spec.ImageMirrorLimit = limit
		app.UpdatedAt = time.Now().UTC()
		state.Apps[index] = app
		normalizeAppStatusForRead(&app)
		hydrateAppBackingServices(state, &app)
		return nil
	})
	return app, err
}

func (s *Store) UpdateAppOriginSource(id string, source model.AppSource) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.App{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateAppOriginSource(id, source)
	}

	var app model.App
	err := s.withLockedState(true, func(state *model.State) error {
		index := findApp(state, id)
		if index < 0 {
			return ErrNotFound
		}
		app = state.Apps[index]
		if isDeletedApp(app) {
			return ErrNotFound
		}
		model.SetAppSourceState(&app, &source, model.AppBuildSource(app))
		app.UpdatedAt = time.Now().UTC()
		state.Apps[index] = app
		normalizeAppStatusForRead(&app)
		hydrateAppBackingServices(state, &app)
		return nil
	})
	return app, err
}

func (s *Store) SyncObservedManagedPostgresSpec(id string, desiredSpec model.AppSpec) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.App{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgSyncObservedManagedPostgresSpec(id, desiredSpec)
	}

	var app model.App
	err := s.withLockedState(true, func(state *model.State) error {
		index := findApp(state, id)
		if index < 0 {
			return ErrNotFound
		}
		app = state.Apps[index]
		if isDeletedApp(app) {
			return ErrNotFound
		}

		spec := cloneAppSpec(&desiredSpec)
		if spec == nil {
			return ErrInvalidInput
		}
		if err := applyGeneratedEnvSpec(spec, &app.Spec); err != nil {
			return err
		}
		if err := applyDesiredSpecBackingServicesState(state, &app, spec); err != nil {
			return err
		}
		app.Spec = *spec
		app.UpdatedAt = time.Now().UTC()
		state.Apps[index] = app
		normalizeAppStatusForRead(&app)
		hydrateAppBackingServices(state, &app)
		return nil
	})
	return app, err
}

func (s *Store) SyncObservedManagedAppBaseline(id string, desiredSpec model.AppSpec, desiredSource *model.AppSource) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.App{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgSyncObservedManagedAppBaseline(id, desiredSpec, desiredSource)
	}

	var app model.App
	err := s.withLockedState(true, func(state *model.State) error {
		index := findApp(state, id)
		if index < 0 {
			return ErrNotFound
		}
		app = state.Apps[index]
		if isDeletedApp(app) {
			return ErrNotFound
		}

		spec := cloneAppSpec(&desiredSpec)
		if spec == nil {
			return ErrInvalidInput
		}
		if err := applyGeneratedEnvSpec(spec, &app.Spec); err != nil {
			return err
		}
		if err := applyDesiredSpecBackingServicesState(state, &app, spec); err != nil {
			return err
		}
		app.Spec = *spec
		buildSource := model.AppBuildSource(app)
		if desiredSource != nil {
			buildSource = cloneAppSource(desiredSource)
		}
		model.SetAppSourceState(&app, model.AppOriginSource(app), buildSource)
		app.UpdatedAt = time.Now().UTC()
		state.Apps[index] = app
		normalizeAppStatusForRead(&app)
		hydrateAppBackingServices(state, &app)
		return nil
	})
	return app, err
}

func (s *Store) PurgeApp(id string) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.App{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgPurgeApp(id)
	}

	var app model.App
	err := s.withLockedState(true, func(state *model.State) error {
		index := findApp(state, id)
		if index < 0 {
			return ErrNotFound
		}

		app = state.Apps[index]
		normalizeAppStatusForRead(&app)
		if app.Status.CurrentReplicas > 0 || strings.TrimSpace(app.Status.CurrentRuntimeID) != "" {
			return ErrConflict
		}
		if !isDeletedApp(app) {
			phase := strings.TrimSpace(strings.ToLower(app.Status.Phase))
			if strings.TrimSpace(app.Spec.Image) != "" || (phase != "importing" && phase != "failed") {
				return ErrConflict
			}
		}

		state.Apps = append(state.Apps[:index], state.Apps[index+1:]...)
		deleteAppDomainsByApp(state, id)
		state.ServiceBindings = deleteServiceBindingsByApp(state.ServiceBindings, id)
		state.BackingServices = deleteOwnedBackingServicesByApp(state.BackingServices, id)
		state.Operations = deleteOperationsByApp(state.Operations, id)
		return maybeFinalizeRequestedProjectDelete(state, app.ProjectID)
	})
	return app, err
}

func (s *Store) createApp(tenantID, projectID, name, description string, spec model.AppSpec, source *model.AppSource, route *model.AppRoute) (model.App, error) {
	name = strings.TrimSpace(name)
	allowPendingImport := source != nil && isQueuedImportSourceType(source.Type) && strings.TrimSpace(spec.Image) == ""
	if tenantID == "" || projectID == "" || name == "" || (!allowPendingImport && spec.Image == "") || spec.Replicas < 1 {
		return model.App{}, ErrInvalidInput
	}
	spec, _ = model.StripFugueInjectedAppEnvFromSpec(spec)
	if err := normalizeAppSpecResources(&spec); err != nil {
		return model.App{}, err
	}
	if err := applyGeneratedEnvSpec(&spec, nil); err != nil {
		return model.App{}, err
	}
	if err := validateAppNetworkMode(spec); err != nil {
		return model.App{}, err
	}
	if err := ensureManagedPostgresPassword(spec.Postgres); err != nil {
		return model.App{}, err
	}
	if err := validateManagedPostgresSpecForAppName(name, spec.Postgres); err != nil {
		return model.App{}, err
	}
	if s.usingDatabase() {
		return s.pgCreateApp(tenantID, projectID, name, description, spec, source, route)
	}

	var app model.App
	err := s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		projectIndex := findProject(state, projectID)
		if projectIndex < 0 || state.Projects[projectIndex].TenantID != tenantID {
			return ErrNotFound
		}
		if projectDeleteRequested(state, projectID) {
			return ErrConflict
		}
		spec.RuntimeID = resolveProjectRuntimeID(state.Projects[projectIndex], spec.RuntimeID)
		if !runtimeVisibleToTenant(state, spec.RuntimeID, tenantID) {
			return ErrNotFound
		}
		if err := validateWorkspaceRuntimeState(state, spec.RuntimeID, spec); err != nil {
			return err
		}
		if err := validateFailoverRuntimeState(state, tenantID, spec); err != nil {
			return err
		}
		if err := validateManagedPostgresRuntimeState(state, tenantID, spec.RuntimeID, spec.Postgres); err != nil {
			return err
		}
		if err := validateAppSpecRuntimeReservationsState(state, projectID, spec); err != nil {
			return err
		}
		for _, existing := range state.Apps {
			if existing.TenantID == tenantID && existing.ProjectID == projectID && strings.EqualFold(existing.Name, name) {
				return ErrConflict
			}
			if route != nil && route.Hostname != "" && existing.Route != nil && strings.EqualFold(existing.Route.Hostname, route.Hostname) {
				return ErrConflict
			}
		}
		now := time.Now().UTC()
		billing := accrueTenantBillingLedger(state, tenantID, now)
		if billing == nil {
			return ErrNotFound
		}
		phase := "created"
		if allowPendingImport {
			phase = "importing"
		}
		app = model.App{
			ID:          model.NewID("app"),
			TenantID:    tenantID,
			ProjectID:   projectID,
			Name:        name,
			Description: strings.TrimSpace(description),
			Route:       cloneAppRoute(route),
			Spec:        spec,
			Status: model.AppStatus{
				Phase:           phase,
				CurrentReplicas: 0,
				UpdatedAt:       now,
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
		model.SetAppSourceState(&app, source, source)
		var ownedService *model.BackingService
		var ownedBinding *model.ServiceBinding
		if app.Spec.Postgres != nil {
			service, binding := ownedManagedPostgresResources(app)
			service.Name = nextAvailableBackingServiceName(state, tenantID, projectID, service.Name)
			ownedService = &service
			ownedBinding = &binding
			app.Spec.Postgres = nil
		}
		if err := validateTenantManagedCapacityProjection(state, *billing, func(projection *model.State) {
			projection.Apps = append(projection.Apps, cloneAppForBilling(app))
			if ownedService != nil {
				projection.BackingServices = append(projection.BackingServices, cloneBackingService(*ownedService))
			}
			if ownedBinding != nil {
				projection.ServiceBindings = append(projection.ServiceBindings, cloneServiceBinding(*ownedBinding))
			}
		}); err != nil {
			return err
		}
		if ownedService != nil {
			state.BackingServices = append(state.BackingServices, *ownedService)
			state.ServiceBindings = append(state.ServiceBindings, *ownedBinding)
		}
		hydrateAppBackingServices(state, &app)
		state.Apps = append(state.Apps, app)
		return nil
	})
	return app, err
}

func isGitHubSyncImportOperation(op model.Operation) bool {
	return op.Type == model.OperationTypeImport && strings.TrimSpace(op.RequestedByID) == model.OperationRequestedByGitHubSyncController
}

func isForegroundPendingOperation(op model.Operation) bool {
	return !isGitHubSyncImportOperation(op)
}

func (s *Store) CreateOperation(op model.Operation) (model.Operation, error) {
	if op.TenantID == "" || op.Type == "" || op.AppID == "" {
		return model.Operation{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateOperation(op)
	}

	err := s.withLockedState(true, func(state *model.State) error {
		appIndex := findApp(state, op.AppID)
		if appIndex < 0 {
			return ErrNotFound
		}
		app := state.Apps[appIndex]
		if app.TenantID != op.TenantID {
			return ErrNotFound
		}
		hydrateAppBackingServices(state, &app)
		if op.DesiredSpec != nil {
			if err := applyGeneratedEnvSpec(op.DesiredSpec, &app.Spec); err != nil {
				return err
			}
			if err := ensureManagedPostgresPasswordWithExisting(op.DesiredSpec.Postgres, OwnedManagedPostgresSpec(app)); err != nil {
				return err
			}
		}

		switch op.Type {
		case model.OperationTypeImport:
			if op.DesiredSpec == nil || op.DesiredSource == nil {
				return ErrInvalidInput
			}
			if !isQueuedImportSourceType(op.DesiredSource.Type) {
				return ErrInvalidInput
			}
			if err := normalizeAppSpecResources(op.DesiredSpec); err != nil {
				return err
			}
			if err := validateAppNetworkMode(*op.DesiredSpec); err != nil {
				return err
			}
			if err := validateManagedPostgresSpecForAppName(app.Name, op.DesiredSpec.Postgres); err != nil {
				return err
			}
			if !runtimeVisibleToTenant(state, op.DesiredSpec.RuntimeID, op.TenantID) {
				return ErrNotFound
			}
			if err := validateWorkspaceRuntimeState(state, op.DesiredSpec.RuntimeID, *op.DesiredSpec); err != nil {
				return err
			}
			if err := validateFailoverRuntimeState(state, op.TenantID, *op.DesiredSpec); err != nil {
				return err
			}
			if err := validateManagedPostgresRuntimeState(state, op.TenantID, op.DesiredSpec.RuntimeID, op.DesiredSpec.Postgres); err != nil {
				return err
			}
			op.TargetRuntimeID = op.DesiredSpec.RuntimeID
		case model.OperationTypeDeploy:
			if op.DesiredSpec == nil {
				return ErrInvalidInput
			}
			if err := normalizeAppSpecResources(op.DesiredSpec); err != nil {
				return err
			}
			if err := validateAppNetworkMode(*op.DesiredSpec); err != nil {
				return err
			}
			if err := validateManagedPostgresSpecForAppName(app.Name, op.DesiredSpec.Postgres); err != nil {
				return err
			}
			if !runtimeVisibleToTenant(state, op.DesiredSpec.RuntimeID, op.TenantID) {
				return ErrNotFound
			}
			if err := validateWorkspaceRuntimeState(state, op.DesiredSpec.RuntimeID, *op.DesiredSpec); err != nil {
				return err
			}
			if err := validateFailoverRuntimeState(state, op.TenantID, *op.DesiredSpec); err != nil {
				return err
			}
			if err := validateManagedPostgresRuntimeState(state, op.TenantID, op.DesiredSpec.RuntimeID, op.DesiredSpec.Postgres); err != nil {
				return err
			}
			op.TargetRuntimeID = op.DesiredSpec.RuntimeID
		case model.OperationTypeScale:
			if op.DesiredReplicas == nil || *op.DesiredReplicas < 0 {
				return ErrInvalidInput
			}
			op.TargetRuntimeID = app.Spec.RuntimeID
		case model.OperationTypeDelete:
			if strings.TrimSpace(app.Spec.RuntimeID) == "" {
				return ErrInvalidInput
			}
			op.SourceRuntimeID = app.Spec.RuntimeID
			op.TargetRuntimeID = app.Spec.RuntimeID
		case model.OperationTypeMigrate:
			targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
			if op.DesiredSpec != nil {
				if err := normalizeAppSpecResources(op.DesiredSpec); err != nil {
					return err
				}
				if err := validateAppNetworkMode(*op.DesiredSpec); err != nil {
					return err
				}
				if err := validateManagedPostgresSpecForAppName(app.Name, op.DesiredSpec.Postgres); err != nil {
					return err
				}
				if strings.TrimSpace(op.DesiredSpec.RuntimeID) == "" {
					op.DesiredSpec.RuntimeID = targetRuntimeID
				}
				if strings.TrimSpace(op.DesiredSpec.RuntimeID) != targetRuntimeID {
					return ErrInvalidInput
				}
				if !runtimeVisibleToTenant(state, op.DesiredSpec.RuntimeID, op.TenantID) {
					return ErrNotFound
				}
				if err := validateWorkspaceRuntimeState(state, op.DesiredSpec.RuntimeID, *op.DesiredSpec); err != nil {
					return err
				}
				if err := validateFailoverRuntimeState(state, op.TenantID, *op.DesiredSpec); err != nil {
					return err
				}
				if err := validateManagedPostgresRuntimeState(state, op.TenantID, op.DesiredSpec.RuntimeID, op.DesiredSpec.Postgres); err != nil {
					return err
				}
				targetRuntimeID = op.DesiredSpec.RuntimeID
			}
			if targetRuntimeID == "" || !runtimeVisibleToTenant(state, targetRuntimeID, op.TenantID) {
				return ErrNotFound
			}
			if hasMigrationBlockingPersistentWorkspace(app) || (op.DesiredSpec != nil && appSpecHasMigrationBlockingPersistentWorkspace(*op.DesiredSpec)) {
				return ErrInvalidInput
			}
			if appHasManagedPostgresService(app) {
				targetRuntimeIndex := findRuntime(state, targetRuntimeID)
				if targetRuntimeIndex < 0 {
					return ErrNotFound
				}
				if err := validateFailoverTargetRuntimeType(state.Runtimes[targetRuntimeIndex].Type); err != nil {
					return err
				}
			}
			op.TargetRuntimeID = targetRuntimeID
			op.SourceRuntimeID = app.Spec.RuntimeID
		case model.OperationTypeFailover:
			if hasInFlightOperationForApp(state.Operations, app.ID) {
				return ErrConflict
			}
			targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
			if targetRuntimeID == "" && app.Spec.Failover != nil {
				targetRuntimeID = strings.TrimSpace(app.Spec.Failover.TargetRuntimeID)
			}
			if targetRuntimeID == "" {
				return ErrInvalidInput
			}
			if !runtimeVisibleToTenant(state, targetRuntimeID, op.TenantID) {
				return ErrNotFound
			}
			targetRuntimeIndex := findRuntime(state, targetRuntimeID)
			if targetRuntimeIndex < 0 {
				return ErrNotFound
			}
			if err := validateFailoverTargetRuntimeType(state.Runtimes[targetRuntimeIndex].Type); err != nil {
				return err
			}
			sourceRuntimeIndex := findRuntime(state, app.Spec.RuntimeID)
			if sourceRuntimeIndex < 0 {
				return ErrNotFound
			}
			if err := validateFailoverTargetRuntimeType(state.Runtimes[sourceRuntimeIndex].Type); err != nil {
				return err
			}
			if strings.TrimSpace(app.Spec.RuntimeID) == targetRuntimeID {
				return ErrInvalidInput
			}
			if err := validateFailoverVolumeReplication(app); err != nil {
				return err
			}
			op.SourceRuntimeID = app.Spec.RuntimeID
			op.TargetRuntimeID = targetRuntimeID
		case model.OperationTypeDatabaseSwitchover:
			if hasInFlightOperationForApp(state.Operations, app.ID) {
				return ErrConflict
			}
			targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
			if targetRuntimeID == "" {
				return ErrInvalidInput
			}
			if !runtimeVisibleToTenant(state, targetRuntimeID, op.TenantID) {
				return ErrNotFound
			}
			postgresSpec := OwnedManagedPostgresSpec(app)
			if postgresSpec == nil {
				return ErrInvalidInput
			}
			sourceRuntimeID := strings.TrimSpace(postgresSpec.RuntimeID)
			if sourceRuntimeID == "" {
				sourceRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
			}
			if sourceRuntimeID == "" {
				return ErrInvalidInput
			}
			targetRuntimeIndex := findRuntime(state, targetRuntimeID)
			if targetRuntimeIndex < 0 {
				return ErrNotFound
			}
			if err := validateFailoverTargetRuntimeType(state.Runtimes[targetRuntimeIndex].Type); err != nil {
				return err
			}
			sourceRuntimeIndex := findRuntime(state, sourceRuntimeID)
			if sourceRuntimeIndex < 0 {
				return ErrNotFound
			}
			if err := validateFailoverTargetRuntimeType(state.Runtimes[sourceRuntimeIndex].Type); err != nil {
				return err
			}
			if sourceRuntimeID == targetRuntimeID {
				return ErrInvalidInput
			}
			op.SourceRuntimeID = sourceRuntimeID
			op.TargetRuntimeID = targetRuntimeID
			if op.DesiredSpec == nil {
				op.DesiredSpec = cloneAppSpec(&app.Spec)
			}
		case model.OperationTypeDatabaseLocalize:
			if hasInFlightOperationForApp(state.Operations, app.ID) {
				return ErrConflict
			}
			postgresSpec := OwnedManagedPostgresSpec(app)
			if postgresSpec == nil {
				return ErrInvalidInput
			}
			sourceRuntimeID := strings.TrimSpace(postgresSpec.RuntimeID)
			if sourceRuntimeID == "" {
				sourceRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
			}
			targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
			if targetRuntimeID == "" && op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
				targetRuntimeID = strings.TrimSpace(op.DesiredSpec.Postgres.RuntimeID)
			}
			if targetRuntimeID == "" {
				targetRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
			}
			if sourceRuntimeID == "" || targetRuntimeID == "" {
				return ErrInvalidInput
			}
			if !runtimeVisibleToTenant(state, targetRuntimeID, op.TenantID) {
				return ErrNotFound
			}
			targetRuntimeIndex := findRuntime(state, targetRuntimeID)
			if targetRuntimeIndex < 0 {
				return ErrNotFound
			}
			if err := validateFailoverTargetRuntimeType(state.Runtimes[targetRuntimeIndex].Type); err != nil {
				return err
			}
			sourceRuntimeIndex := findRuntime(state, sourceRuntimeID)
			if sourceRuntimeIndex < 0 {
				return ErrNotFound
			}
			if err := validateFailoverTargetRuntimeType(state.Runtimes[sourceRuntimeIndex].Type); err != nil {
				return err
			}
			if op.DesiredSpec == nil {
				op.DesiredSpec = cloneAppSpec(&app.Spec)
			}
			if op.DesiredSpec == nil {
				return ErrInvalidInput
			}
			if err := normalizeAppSpecResources(op.DesiredSpec); err != nil {
				return err
			}
			if err := validateAppNetworkMode(*op.DesiredSpec); err != nil {
				return err
			}
			if op.DesiredSpec.Postgres == nil {
				postgresCopy := *postgresSpec
				if postgresSpec.Resources != nil {
					resources := *postgresSpec.Resources
					postgresCopy.Resources = &resources
				}
				op.DesiredSpec.Postgres = &postgresCopy
			}
			op.DesiredSpec.Postgres.RuntimeID = targetRuntimeID
			op.DesiredSpec.Postgres.FailoverTargetRuntimeID = ""
			op.DesiredSpec.Postgres.Instances = 1
			op.DesiredSpec.Postgres.SynchronousReplicas = 0
			op.DesiredSpec.Postgres.PrimaryPlacementPendingRebalance = false
			if err := validateManagedPostgresSpecForAppName(app.Name, op.DesiredSpec.Postgres); err != nil {
				return err
			}
			if err := validateWorkspaceRuntimeState(state, op.DesiredSpec.RuntimeID, *op.DesiredSpec); err != nil {
				return err
			}
			if err := validateFailoverRuntimeState(state, op.TenantID, *op.DesiredSpec); err != nil {
				return err
			}
			if err := validateManagedPostgresRuntimeState(state, op.TenantID, op.DesiredSpec.RuntimeID, op.DesiredSpec.Postgres); err != nil {
				return err
			}
			op.SourceRuntimeID = sourceRuntimeID
			op.TargetRuntimeID = targetRuntimeID
		default:
			return ErrInvalidInput
		}
		if err := validateOperationRuntimeReservationsState(state, app.ProjectID, op); err != nil {
			return err
		}

		now := time.Now().UTC()
		op.DesiredSpec = cloneAppSpec(op.DesiredSpec)
		op.DesiredSource = cloneAppSource(op.DesiredSource)
		op.DesiredOriginSource = cloneAppSource(op.DesiredOriginSource)
		billing := accrueTenantBillingLedger(state, app.TenantID, now)
		if billing == nil {
			return ErrNotFound
		}
		currentTotal, nextTotal, err := projectedTenantManagedTotalsWithBilling(state, app, op, *billing)
		if err != nil {
			return err
		}
		currentPublicHourlyRateMicroCents, nextPublicHourlyRateMicroCents, err := projectedTenantPublicRuntimeHourlyRates(state, app, op)
		if err != nil {
			return err
		}
		if err := validateTenantOperationBilling(
			*billing,
			currentTotal,
			nextTotal,
			currentPublicHourlyRateMicroCents,
			nextPublicHourlyRateMicroCents,
		); err != nil {
			return err
		}
		op.ID = model.NewID("op")
		op.Status = model.OperationStatusPending
		op.ExecutionMode = model.ExecutionModeManaged
		op.ResultMessage = defaultInFlightOperationMessage(op)
		op.CreatedAt = now
		op.UpdatedAt = now
		state.Operations = append(state.Operations, op)
		if err := applyInFlightOperationToApp(state, &state.Operations[len(state.Operations)-1]); err != nil {
			return err
		}
		op = state.Operations[len(state.Operations)-1]
		return nil
	})
	return op, err
}

func (s *Store) ListOperations(tenantID string, platformAdmin bool) ([]model.Operation, error) {
	if s.usingDatabase() {
		return s.pgListOperations(tenantID, platformAdmin)
	}
	var ops []model.Operation
	err := s.withLockedState(false, func(state *model.State) error {
		for _, op := range state.Operations {
			if platformAdmin || op.TenantID == tenantID {
				ops = append(ops, op)
			}
		}
		sort.Slice(ops, func(i, j int) bool {
			return ops[i].CreatedAt.Before(ops[j].CreatedAt)
		})
		return nil
	})
	return ops, err
}

func (s *Store) ListOperationSummaries(tenantID string, platformAdmin bool) ([]model.Operation, error) {
	if s.usingDatabase() {
		return s.pgListOperationSummaries(tenantID, platformAdmin)
	}
	var ops []model.Operation
	err := s.withLockedState(false, func(state *model.State) error {
		for _, op := range state.Operations {
			if platformAdmin || op.TenantID == tenantID {
				ops = append(ops, operationSummary(op))
			}
		}
		sort.Slice(ops, func(i, j int) bool {
			return ops[i].CreatedAt.Before(ops[j].CreatedAt)
		})
		return nil
	})
	return ops, err
}

func (s *Store) ListOperationsByApp(tenantID string, platformAdmin bool, appID string) ([]model.Operation, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return s.ListOperations(tenantID, platformAdmin)
	}
	if s.usingDatabase() {
		return s.pgListOperationsByApp(tenantID, platformAdmin, appID)
	}

	var ops []model.Operation
	err := s.withLockedState(false, func(state *model.State) error {
		for _, op := range state.Operations {
			if !platformAdmin && op.TenantID != tenantID {
				continue
			}
			if strings.TrimSpace(op.AppID) != appID {
				continue
			}
			ops = append(ops, op)
		}
		sort.Slice(ops, func(i, j int) bool {
			return ops[i].CreatedAt.Before(ops[j].CreatedAt)
		})
		return nil
	})
	return ops, err
}

func (s *Store) ListOperationSummariesByApp(tenantID string, platformAdmin bool, appID string) ([]model.Operation, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return s.ListOperationSummaries(tenantID, platformAdmin)
	}
	if s.usingDatabase() {
		return s.pgListOperationSummariesByApp(tenantID, platformAdmin, appID)
	}

	var ops []model.Operation
	err := s.withLockedState(false, func(state *model.State) error {
		for _, op := range state.Operations {
			if !platformAdmin && op.TenantID != tenantID {
				continue
			}
			if strings.TrimSpace(op.AppID) != appID {
				continue
			}
			ops = append(ops, operationSummary(op))
		}
		sort.Slice(ops, func(i, j int) bool {
			return ops[i].CreatedAt.Before(ops[j].CreatedAt)
		})
		return nil
	})
	return ops, err
}

func operationSummary(op model.Operation) model.Operation {
	op.DesiredSpec = nil
	op.DesiredSource = nil
	op.DesiredOriginSource = nil
	return op
}

func (s *Store) HasActiveOperationByApp(tenantID string, platformAdmin bool, appID string) (bool, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return false, nil
	}
	if s.usingDatabase() {
		return s.pgHasActiveOperationByApp(tenantID, platformAdmin, appID)
	}

	var found bool
	err := s.withLockedState(false, func(state *model.State) error {
		for _, op := range state.Operations {
			if !platformAdmin && op.TenantID != tenantID {
				continue
			}
			if strings.TrimSpace(op.AppID) != appID {
				continue
			}
			if isActiveOperationStatus(op.Status) {
				found = true
				return nil
			}
		}
		return nil
	})
	return found, err
}

func (s *Store) ListConsoleOperationsByApp(tenantID string, platformAdmin bool, appID string, recentLimit int) ([]model.Operation, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return []model.Operation{}, nil
	}
	if recentLimit < 1 {
		recentLimit = 1
	}
	if s.usingDatabase() {
		return s.pgListConsoleOperationsByApp(tenantID, platformAdmin, appID, recentLimit)
	}

	var ops []model.Operation
	err := s.withLockedState(false, func(state *model.State) error {
		candidates := make([]model.Operation, 0)
		for _, op := range state.Operations {
			if !platformAdmin && op.TenantID != tenantID {
				continue
			}
			if strings.TrimSpace(op.AppID) != appID {
				continue
			}
			candidates = append(candidates, op)
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
				return candidates[i].ID > candidates[j].ID
			}
			return candidates[i].CreatedAt.After(candidates[j].CreatedAt)
		})

		seen := make(map[string]struct{}, len(candidates))
		for _, op := range candidates {
			if isActiveOperationStatus(op.Status) {
				ops = append(ops, op)
				seen[op.ID] = struct{}{}
			}
		}
		recentCount := 0
		for _, op := range candidates {
			if recentCount >= recentLimit {
				break
			}
			recentCount++
			if _, ok := seen[op.ID]; ok {
				continue
			}
			ops = append(ops, op)
			seen[op.ID] = struct{}{}
		}
		sort.Slice(ops, func(i, j int) bool {
			if ops[i].CreatedAt.Equal(ops[j].CreatedAt) {
				return ops[i].ID < ops[j].ID
			}
			return ops[i].CreatedAt.Before(ops[j].CreatedAt)
		})
		return nil
	})
	return ops, err
}

func (s *Store) ListOperationsWithDesiredSourceByApp(tenantID string, platformAdmin bool, appID string) ([]model.Operation, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return []model.Operation{}, nil
	}
	if s.usingDatabase() {
		return s.pgListOperationsWithDesiredSourceByApp(tenantID, platformAdmin, appID)
	}

	var ops []model.Operation
	err := s.withLockedState(false, func(state *model.State) error {
		for _, op := range state.Operations {
			if !platformAdmin && op.TenantID != tenantID {
				continue
			}
			if strings.TrimSpace(op.AppID) != appID || op.DesiredSource == nil {
				continue
			}
			ops = append(ops, op)
		}
		sort.Slice(ops, func(i, j int) bool {
			return ops[i].CreatedAt.Before(ops[j].CreatedAt)
		})
		return nil
	})
	return ops, err
}

func (s *Store) ListOperationsWithDesiredSourceByApps(tenantID string, platformAdmin bool, appIDs []string) (map[string][]model.Operation, error) {
	appIDSet := trimmedStringSet(appIDs)
	if len(appIDSet) == 0 {
		return map[string][]model.Operation{}, nil
	}
	if s.usingDatabase() {
		return s.pgListOperationsWithDesiredSourceByApps(tenantID, platformAdmin, sortedTrimmedStringKeys(appIDSet))
	}

	opsByAppID := make(map[string][]model.Operation, len(appIDSet))
	err := s.withLockedState(false, func(state *model.State) error {
		for _, op := range state.Operations {
			if !platformAdmin && op.TenantID != tenantID {
				continue
			}
			appID := strings.TrimSpace(op.AppID)
			if _, ok := appIDSet[appID]; !ok || op.DesiredSource == nil {
				continue
			}
			opsByAppID[appID] = append(opsByAppID[appID], op)
		}
		for appID := range opsByAppID {
			sort.Slice(opsByAppID[appID], func(i, j int) bool {
				return opsByAppID[appID][i].CreatedAt.Before(opsByAppID[appID][j].CreatedAt)
			})
		}
		return nil
	})
	return opsByAppID, err
}

func (s *Store) ListActiveOperations() ([]model.Operation, error) {
	if s.usingDatabase() {
		return s.pgListActiveOperations()
	}

	var ops []model.Operation
	err := s.withLockedState(false, func(state *model.State) error {
		for _, op := range state.Operations {
			if !isActiveOperationStatus(op.Status) {
				continue
			}
			ops = append(ops, op)
		}
		sortActiveOperations(ops)
		return nil
	})
	return ops, err
}

func (s *Store) GetOperation(id string) (model.Operation, error) {
	if s.usingDatabase() {
		return s.pgGetOperation(id)
	}
	var op model.Operation
	err := s.withLockedState(false, func(state *model.State) error {
		index := findOperation(state, id)
		if index < 0 {
			return ErrNotFound
		}
		op = state.Operations[index]
		return nil
	})
	return op, err
}

func (s *Store) TryClaimPendingOperation(id string) (model.Operation, bool, error) {
	if s.usingDatabase() {
		return s.pgTryClaimPendingOperation(id)
	}

	var (
		op      model.Operation
		claimed bool
	)
	err := s.withLockedState(true, func(state *model.State) error {
		index := findOperation(state, id)
		if index < 0 {
			return nil
		}
		if state.Operations[index].Status != model.OperationStatusPending {
			return nil
		}
		claimedOp, err := claimPendingOperationLocked(state, index)
		if err != nil {
			return err
		}
		op = claimedOp
		claimed = true
		return nil
	})
	return op, claimed, err
}

func (s *Store) ClaimNextPendingOperation() (model.Operation, bool, error) {
	if s.usingDatabase() {
		return s.pgClaimNextPendingOperation()
	}
	return s.claimNextPendingOperationMatching(nil)
}

func (s *Store) ClaimNextPendingForegroundOperation() (model.Operation, bool, error) {
	if s.usingDatabase() {
		return s.pgClaimNextPendingForegroundOperation()
	}
	return s.claimNextPendingOperationMatching(isForegroundPendingOperation)
}

func (s *Store) ClaimNextPendingGitHubSyncImportOperation() (model.Operation, bool, error) {
	if s.usingDatabase() {
		return s.pgClaimNextPendingGitHubSyncImportOperation()
	}
	return s.claimNextPendingOperationMatching(isGitHubSyncImportOperation)
}

func (s *Store) claimNextPendingOperationMatching(match func(model.Operation) bool) (model.Operation, bool, error) {
	var op model.Operation
	var found bool
	err := s.withLockedState(true, func(state *model.State) error {
		pending := make([]int, 0)
		for idx := range state.Operations {
			if state.Operations[idx].Status != model.OperationStatusPending {
				continue
			}
			if match != nil && !match(state.Operations[idx]) {
				continue
			}
			pending = append(pending, idx)
		}
		if len(pending) == 0 {
			return nil
		}
		sort.Slice(pending, func(i, j int) bool {
			return state.Operations[pending[i]].CreatedAt.Before(state.Operations[pending[j]].CreatedAt)
		})
		claimed, err := claimPendingOperationLocked(state, pending[0])
		if err != nil {
			return err
		}
		op = claimed
		found = true
		return nil
	})
	return op, found, err
}

func claimPendingOperationLocked(state *model.State, index int) (model.Operation, error) {
	now := time.Now().UTC()
	if state.Operations[index].Type == model.OperationTypeImport {
		state.Operations[index].Status = model.OperationStatusRunning
		state.Operations[index].ExecutionMode = model.ExecutionModeManaged
		state.Operations[index].StartedAt = &now
		state.Operations[index].ResultMessage = defaultInFlightOperationMessage(state.Operations[index])
	} else {
		runtimeIndex := findRuntime(state, state.Operations[index].TargetRuntimeID)
		if runtimeIndex >= 0 && state.Runtimes[runtimeIndex].Type == model.RuntimeTypeExternalOwned {
			state.Operations[index].Status = model.OperationStatusWaitingAgent
			state.Operations[index].ExecutionMode = model.ExecutionModeAgent
			state.Operations[index].AssignedRuntimeID = state.Operations[index].TargetRuntimeID
			state.Operations[index].ResultMessage = "task dispatched to external runtime agent"
		} else {
			state.Operations[index].Status = model.OperationStatusRunning
			state.Operations[index].ExecutionMode = model.ExecutionModeManaged
			state.Operations[index].StartedAt = &now
			state.Operations[index].ResultMessage = defaultInFlightOperationMessage(state.Operations[index])
		}
	}
	state.Operations[index].UpdatedAt = now
	if err := applyInFlightOperationToApp(state, &state.Operations[index]); err != nil {
		return model.Operation{}, err
	}
	return state.Operations[index], nil
}

func (s *Store) CompleteManagedOperationWithResult(id, manifestPath, message string, desiredSpec *model.AppSpec, desiredSource *model.AppSource) (model.Operation, error) {
	return s.completeOperation(id, "", manifestPath, message, desiredSpec, desiredSource)
}

func (s *Store) CompleteManagedOperation(id, manifestPath, message string) (model.Operation, error) {
	return s.completeOperation(id, "", manifestPath, message, nil, nil)
}

func (s *Store) CompleteAgentOperation(id, runtimeID, manifestPath, message string) (model.Operation, error) {
	return s.completeOperation(id, runtimeID, manifestPath, message, nil, nil)
}

func (s *Store) UpdateOperationProgress(id, message string) (model.Operation, error) {
	message = strings.TrimSpace(message)
	if message == "" {
		return model.Operation{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateOperationProgress(id, message)
	}
	var op model.Operation
	err := s.withLockedState(true, func(state *model.State) error {
		index := findOperation(state, id)
		if index < 0 {
			return ErrNotFound
		}
		if !operationCanUpdateProgress(state.Operations[index]) {
			return ErrConflict
		}
		now := time.Now().UTC()
		state.Operations[index].ResultMessage = message
		state.Operations[index].UpdatedAt = now
		if err := applyInFlightOperationToApp(state, &state.Operations[index]); err != nil {
			return err
		}
		op = state.Operations[index]
		return nil
	})
	return op, err
}

func (s *Store) completeOperation(id, runtimeID, manifestPath, message string, desiredSpec *model.AppSpec, desiredSource *model.AppSource) (model.Operation, error) {
	if s.usingDatabase() {
		return s.pgCompleteOperation(id, runtimeID, manifestPath, message, desiredSpec, desiredSource)
	}
	var op model.Operation
	err := s.withLockedState(true, func(state *model.State) error {
		index := findOperation(state, id)
		if index < 0 {
			return ErrNotFound
		}
		if runtimeID != "" && state.Operations[index].AssignedRuntimeID != runtimeID {
			return ErrNotFound
		}
		if !operationCanTransitionToCompleted(state.Operations[index]) {
			return ErrConflict
		}
		if desiredSpec != nil {
			state.Operations[index].DesiredSpec = cloneAppSpec(desiredSpec)
		}
		if desiredSource != nil {
			state.Operations[index].DesiredSource = cloneAppSource(desiredSource)
		}
		now := time.Now().UTC()
		state.Operations[index].Status = model.OperationStatusCompleted
		state.Operations[index].UpdatedAt = now
		state.Operations[index].CompletedAt = &now
		state.Operations[index].ManifestPath = manifestPath
		state.Operations[index].ResultMessage = strings.TrimSpace(message)
		if state.Operations[index].StartedAt == nil {
			state.Operations[index].StartedAt = &now
		}
		if err := applyOperationToApp(state, &state.Operations[index]); err != nil {
			return err
		}
		currentOp := state.Operations[index]
		if currentOp.Type == model.OperationTypeDelete {
			deleteAppDomainsByApp(state, currentOp.AppID)
			appIndex := findApp(state, currentOp.AppID)
			if appIndex >= 0 {
				if err := maybeFinalizeRequestedProjectDelete(state, state.Apps[appIndex].ProjectID); err != nil {
					return err
				}
			}
		}
		op = currentOp
		return nil
	})
	return op, err
}
func (s *Store) DispatchOperationToRuntime(id, runtimeID string) (model.Operation, error) {
	if s.usingDatabase() {
		return s.pgDispatchOperationToRuntime(id, runtimeID)
	}
	var op model.Operation
	err := s.withLockedState(true, func(state *model.State) error {
		index := findOperation(state, id)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		state.Operations[index].Status = model.OperationStatusWaitingAgent
		state.Operations[index].ExecutionMode = model.ExecutionModeAgent
		state.Operations[index].AssignedRuntimeID = runtimeID
		state.Operations[index].UpdatedAt = now
		state.Operations[index].ResultMessage = "task dispatched to external runtime agent"
		if err := applyInFlightOperationToApp(state, &state.Operations[index]); err != nil {
			return err
		}
		op = state.Operations[index]
		return nil
	})
	return op, err
}

func (s *Store) RequeueManagedOperation(id, message string) (model.Operation, error) {
	if s.usingDatabase() {
		return s.pgRequeueManagedOperation(id, message)
	}
	var op model.Operation
	err := s.withLockedState(true, func(state *model.State) error {
		index := findOperation(state, id)
		if index < 0 {
			return ErrNotFound
		}
		if !isRequeueableManagedOperation(state.Operations[index]) {
			return ErrConflict
		}
		requeueManagedOperationState(&state.Operations[index], message)
		if err := applyInFlightOperationToApp(state, &state.Operations[index]); err != nil {
			return err
		}
		op = state.Operations[index]
		return nil
	})
	return op, err
}

func (s *Store) RequeueInFlightManagedOperations(message string) (int, error) {
	if s.usingDatabase() {
		return s.pgRequeueInFlightManagedOperations(message)
	}
	count := 0
	err := s.withLockedState(true, func(state *model.State) error {
		for index := range state.Operations {
			if !isRequeueableManagedOperation(state.Operations[index]) {
				continue
			}
			requeueManagedOperationState(&state.Operations[index], message)
			if err := applyInFlightOperationToApp(state, &state.Operations[index]); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return count, err
}

func (s *Store) FailOperation(id, message string) (model.Operation, error) {
	if s.usingDatabase() {
		return s.pgFailOperation(id, message)
	}
	var op model.Operation
	err := s.withLockedState(true, func(state *model.State) error {
		index := findOperation(state, id)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		state.Operations[index].Status = model.OperationStatusFailed
		state.Operations[index].UpdatedAt = now
		state.Operations[index].CompletedAt = &now
		state.Operations[index].ErrorMessage = strings.TrimSpace(message)
		applyFailedOperationToApp(state, &state.Operations[index])
		op = state.Operations[index]
		return nil
	})
	return op, err
}

func (s *Store) ListAssignedOperations(runtimeID string) ([]model.Operation, error) {
	if s.usingDatabase() {
		return s.pgListAssignedOperations(runtimeID)
	}
	var ops []model.Operation
	err := s.withLockedState(false, func(state *model.State) error {
		for _, op := range state.Operations {
			if op.AssignedRuntimeID == runtimeID && op.Status == model.OperationStatusWaitingAgent {
				ops = append(ops, op)
			}
		}
		sort.Slice(ops, func(i, j int) bool {
			return ops[i].CreatedAt.Before(ops[j].CreatedAt)
		})
		return nil
	})
	return ops, err
}

func (s *Store) AppendAuditEvent(event model.AuditEvent) error {
	if event.ID == "" {
		event.ID = model.NewID("audit")
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	if s.usingDatabase() {
		return s.pgAppendAuditEvent(event)
	}
	return s.withLockedState(true, func(state *model.State) error {
		state.AuditEvents = append(state.AuditEvents, event)
		return nil
	})
}

func (s *Store) ListAuditEvents(tenantID string, platformAdmin bool, limit int) ([]model.AuditEvent, error) {
	if s.usingDatabase() {
		return s.pgListAuditEvents(tenantID, platformAdmin, limit)
	}
	var events []model.AuditEvent
	err := s.withLockedState(false, func(state *model.State) error {
		for _, event := range state.AuditEvents {
			if platformAdmin || event.TenantID == tenantID || event.TenantID == "" {
				events = append(events, event)
			}
		}
		sort.Slice(events, func(i, j int) bool {
			return events[i].CreatedAt.After(events[j].CreatedAt)
		})
		if limit > 0 && len(events) > limit {
			events = events[:limit]
		}
		return nil
	})
	return events, err
}

func (s *Store) readState() (model.State, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return model.State{}, nil
		}
		return model.State{}, fmt.Errorf("read state: %w", err)
	}
	if len(data) == 0 {
		return model.State{}, nil
	}
	var state model.State
	if err := json.Unmarshal(data, &state); err != nil {
		return model.State{}, fmt.Errorf("unmarshal state: %w", err)
	}
	return state, nil
}

func (s *Store) writeState(state model.State) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return fmt.Errorf("write temp state: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("rename temp state: %w", err)
	}
	return nil
}

func ensureDefaults(state *model.State) {
	if state.Version == "" {
		state.Version = "v0"
	}
	if state.Tenants == nil {
		state.Tenants = []model.Tenant{}
	}
	if state.Projects == nil {
		state.Projects = []model.Project{}
	}
	if state.ProjectDeleteRequests == nil {
		state.ProjectDeleteRequests = map[string]time.Time{}
	}
	if state.ProjectRuntimeReservations == nil {
		state.ProjectRuntimeReservations = []model.ProjectRuntimeReservation{}
	}
	if state.APIKeys == nil {
		state.APIKeys = []model.APIKey{}
	}
	repairAllAPIKeyStatuses(state)
	if state.EnrollmentTokens == nil {
		state.EnrollmentTokens = []model.EnrollmentToken{}
	}
	if state.NodeKeys == nil {
		state.NodeKeys = []model.NodeKey{}
	}
	for idx := range state.NodeKeys {
		state.NodeKeys[idx].Scope = model.NormalizeNodeKeyScope(state.NodeKeys[idx].Scope)
	}
	if state.Machines == nil {
		state.Machines = []model.Machine{}
	}
	for idx := range state.Machines {
		normalizeMachineForRead(&state.Machines[idx])
	}
	if state.NodeUpdaters == nil {
		state.NodeUpdaters = []model.NodeUpdater{}
	}
	if state.NodeUpdateTasks == nil {
		state.NodeUpdateTasks = []model.NodeUpdateTask{}
	}
	if state.Runtimes == nil {
		state.Runtimes = []model.Runtime{}
	}
	if state.RuntimeGrants == nil {
		state.RuntimeGrants = []model.RuntimeAccessGrant{}
	}
	if state.Apps == nil {
		state.Apps = []model.App{}
	}
	for idx := range state.Apps {
		model.NormalizeAppSourceState(&state.Apps[idx])
	}
	if state.AppDomains == nil {
		state.AppDomains = []model.AppDomain{}
	}
	if state.EdgeGroups == nil {
		state.EdgeGroups = []model.EdgeGroup{}
	}
	if state.EdgeNodes == nil {
		state.EdgeNodes = []model.EdgeNode{}
	}
	for idx := range state.EdgeNodes {
		normalizeEdgeNodeForRead(&state.EdgeNodes[idx])
	}
	if state.BackingServices == nil {
		state.BackingServices = []model.BackingService{}
	}
	if state.ServiceBindings == nil {
		state.ServiceBindings = []model.ServiceBinding{}
	}
	if state.Operations == nil {
		state.Operations = []model.Operation{}
	}
	for idx := range state.Operations {
		model.NormalizeOperationSourceState(&state.Operations[idx])
	}
	if state.AuditEvents == nil {
		state.AuditEvents = []model.AuditEvent{}
	}
	if state.Idempotency == nil {
		state.Idempotency = []model.IdempotencyRecord{}
	}
	ensureTenantBillingDefaults(state)
	if findRuntime(state, "runtime_managed_shared") < 0 {
		now := time.Now().UTC()
		state.Runtimes = append(state.Runtimes, model.Runtime{
			ID:         "runtime_managed_shared",
			Name:       "managed-shared",
			Type:       model.RuntimeTypeManagedShared,
			AccessMode: model.RuntimeAccessModePlatformShared,
			PoolMode:   model.RuntimePoolModeDedicated,
			Status:     model.RuntimeStatusActive,
			Endpoint:   "in-cluster",
			Labels:     map[string]string{"managed": "true"},
			CreatedAt:  now,
			UpdatedAt:  now,
		})
	}
	ensureRuntimeMetadata(state)
}

func redactAPIKey(key model.APIKey) model.APIKey {
	normalizeAPIKeyForRead(&key)
	key.Hash = ""
	return key
}

func redactEnrollmentToken(token model.EnrollmentToken) model.EnrollmentToken {
	token.Hash = ""
	return token
}

func redactNodeKey(key model.NodeKey) model.NodeKey {
	key.Hash = ""
	return key
}

func redactNodeUpdater(updater model.NodeUpdater) model.NodeUpdater {
	updater.TokenHash = ""
	updater.Labels = cloneMap(updater.Labels)
	updater.Capabilities = append([]string(nil), updater.Capabilities...)
	return updater
}

func redactNodeUpdateTask(task model.NodeUpdateTask) model.NodeUpdateTask {
	task.Payload = cloneMap(task.Payload)
	task.Logs = append([]model.NodeUpdateTaskLog(nil), task.Logs...)
	return task
}

func defaultNodeKeyLabel(label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return "default"
	}
	return label
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func applyAPIKeyUpdates(key model.APIKey, label *string, scopes *[]string) (model.APIKey, error) {
	updated := key
	if label != nil {
		trimmed := strings.TrimSpace(*label)
		if trimmed == "" {
			return model.APIKey{}, ErrInvalidInput
		}
		updated.Label = trimmed
	}
	if scopes != nil {
		normalized := model.NormalizeScopes(*scopes)
		if len(normalized) == 0 {
			return model.APIKey{}, ErrInvalidInput
		}
		updated.Scopes = normalized
	}
	return updated, nil
}

func normalizedMachineName(machineName, runtimeName, endpoint string) string {
	machineName = strings.TrimSpace(machineName)
	if machineName != "" {
		return machineName
	}
	runtimeName = strings.TrimSpace(runtimeName)
	if runtimeName != "" {
		return runtimeName
	}
	endpoint = strings.TrimSpace(endpoint)
	if endpoint != "" {
		return endpoint
	}
	return "node"
}

func normalizeClusterNodeName(name string) (string, error) {
	raw := strings.TrimSpace(name)
	if raw == "" {
		return "", nil
	}

	normalized := strings.ToLower(raw)
	if errs := k8svalidation.IsDNS1123Subdomain(normalized); len(errs) > 0 {
		return "", fmt.Errorf("%w: invalid cluster node name %q: %s", ErrInvalidInput, raw, strings.Join(errs, "; "))
	}
	return normalized, nil
}

func normalizedMachineFingerprint(machineFingerprint, machineName, runtimeName, endpoint string) string {
	machineFingerprint = strings.TrimSpace(machineFingerprint)
	if machineFingerprint != "" {
		return machineFingerprint
	}
	fallback := strings.TrimSpace(endpoint)
	if fallback == "" {
		fallback = strings.TrimSpace(machineName)
	}
	if fallback == "" {
		fallback = strings.TrimSpace(runtimeName)
	}
	if fallback == "" {
		fallback = "node"
	}
	return "legacy:" + strings.ToLower(fallback)
}

func runtimeConnectionMode(runtimeType string) string {
	switch runtimeType {
	case model.RuntimeTypeManagedOwned:
		return model.MachineConnectionModeCluster
	case model.RuntimeTypeExternalOwned:
		return model.MachineConnectionModeAgent
	default:
		return ""
	}
}

func runtimeTypeFromConnectionMode(connectionMode string) string {
	switch strings.TrimSpace(connectionMode) {
	case model.MachineConnectionModeCluster:
		return model.RuntimeTypeManagedOwned
	case model.MachineConnectionModeAgent:
		return model.RuntimeTypeExternalOwned
	default:
		return ""
	}
}

func ensureRuntimeMetadata(state *model.State) {
	if state == nil {
		return
	}
	if state.Runtimes == nil {
		state.Runtimes = []model.Runtime{}
	}

	legacyMachines := make(map[string]model.Machine, len(state.Machines))
	for _, machine := range state.Machines {
		if strings.TrimSpace(machine.RuntimeID) == "" {
			continue
		}
		existing, ok := legacyMachines[machine.RuntimeID]
		if ok && existing.UpdatedAt.After(machine.UpdatedAt) {
			continue
		}
		legacyMachines[machine.RuntimeID] = machine
	}

	for _, machine := range legacyMachines {
		if findRuntime(state, machine.RuntimeID) >= 0 {
			continue
		}
		runtimeType := runtimeTypeFromConnectionMode(machine.ConnectionMode)
		if runtimeType == "" {
			continue
		}
		runtimeName := strings.TrimSpace(machine.RuntimeName)
		if runtimeName == "" {
			runtimeName = strings.TrimSpace(machine.ClusterNodeName)
		}
		if runtimeName == "" {
			runtimeName = strings.TrimSpace(machine.Name)
		}
		if runtimeName == "" {
			runtimeName = "node"
		}
		state.Runtimes = append(state.Runtimes, model.Runtime{
			ID:                machine.RuntimeID,
			TenantID:          machine.TenantID,
			Name:              runtimeName,
			MachineName:       machine.Name,
			Type:              runtimeType,
			AccessMode:        normalizeRuntimeAccessMode(runtimeType, ""),
			PoolMode:          model.NormalizeRuntimePoolMode(runtimeType, ""),
			ConnectionMode:    machine.ConnectionMode,
			Status:            machine.Status,
			Endpoint:          machine.Endpoint,
			Labels:            cloneMap(machine.Labels),
			NodeKeyID:         machine.NodeKeyID,
			ClusterNodeName:   machine.ClusterNodeName,
			FingerprintPrefix: machine.FingerprintPrefix,
			FingerprintHash:   machine.FingerprintHash,
			LastSeenAt:        machine.LastSeenAt,
			LastHeartbeatAt:   machine.LastSeenAt,
			CreatedAt:         machine.CreatedAt,
			UpdatedAt:         machine.UpdatedAt,
		})
	}

	for idx := range state.Runtimes {
		backfillRuntimeMetadata(&state.Runtimes[idx], legacyMachines[state.Runtimes[idx].ID])
	}
}

func backfillRuntimeMetadata(runtime *model.Runtime, machine model.Machine) {
	if runtime == nil {
		return
	}
	runtime.AccessMode = normalizeRuntimeAccessMode(runtime.Type, runtime.AccessMode)
	runtime.PoolMode = model.NormalizeRuntimePoolMode(runtime.Type, runtime.PoolMode)
	if runtime.MachineName == "" {
		if strings.TrimSpace(machine.Name) != "" {
			runtime.MachineName = machine.Name
		} else {
			runtime.MachineName = runtime.Name
		}
	}
	if runtime.ConnectionMode == "" {
		if strings.TrimSpace(machine.ConnectionMode) != "" {
			runtime.ConnectionMode = machine.ConnectionMode
		} else {
			runtime.ConnectionMode = runtimeConnectionMode(runtime.Type)
		}
	}
	if runtime.Endpoint == "" {
		runtime.Endpoint = machine.Endpoint
	}
	if runtime.Labels == nil && machine.Labels != nil {
		runtime.Labels = cloneMap(machine.Labels)
	}
	if runtime.NodeKeyID == "" {
		runtime.NodeKeyID = machine.NodeKeyID
	}
	if runtime.ClusterNodeName == "" {
		switch {
		case strings.TrimSpace(machine.ClusterNodeName) != "":
			runtime.ClusterNodeName = machine.ClusterNodeName
		case runtime.Type == model.RuntimeTypeManagedOwned && (runtime.NodeKeyID != "" || runtime.FingerprintHash != "" || runtime.Status == model.RuntimeStatusActive):
			runtime.ClusterNodeName = runtime.Name
		}
	}
	if runtime.FingerprintPrefix == "" {
		runtime.FingerprintPrefix = machine.FingerprintPrefix
	}
	if runtime.FingerprintHash == "" {
		runtime.FingerprintHash = machine.FingerprintHash
	}
	if runtime.LastSeenAt == nil {
		if machine.LastSeenAt != nil {
			runtime.LastSeenAt = machine.LastSeenAt
		} else if runtime.LastHeartbeatAt != nil {
			runtime.LastSeenAt = runtime.LastHeartbeatAt
		}
	}
}

func applyRuntimeIdentity(runtime *model.Runtime, machineName, machineFingerprint, runtimeType, endpoint string, labels map[string]string, nodeKeyID string, now time.Time) {
	if runtime == nil {
		return
	}
	runtime.AccessMode = normalizeRuntimeAccessMode(runtimeType, runtime.AccessMode)
	runtime.PoolMode = model.NormalizeRuntimePoolMode(runtimeType, runtime.PoolMode)
	runtime.MachineName = normalizedMachineName(machineName, runtime.Name, endpoint)
	runtime.ConnectionMode = runtimeConnectionMode(runtimeType)
	runtime.FingerprintPrefix = model.SecretPrefix(machineFingerprint)
	runtime.FingerprintHash = model.HashSecret(machineFingerprint)
	runtime.LastSeenAt = &now
	if runtimeType == model.RuntimeTypeManagedOwned {
		runtime.ClusterNodeName = runtime.Name
	} else {
		runtime.ClusterNodeName = ""
	}
	if runtime.Endpoint == "" {
		runtime.Endpoint = strings.TrimSpace(endpoint)
	}
	if runtime.Labels == nil && labels != nil {
		runtime.Labels = cloneMap(labels)
	}
	if runtime.NodeKeyID == "" {
		runtime.NodeKeyID = nodeKeyID
	}
}

func cloneAppSource(in *model.AppSource) *model.AppSource {
	return model.CloneAppSource(in)
}

func applyOperationSourceStateToApp(app *model.App, op model.Operation) {
	if app == nil {
		return
	}
	build := model.AppBuildSource(*app)
	if op.DesiredSource != nil {
		build = cloneAppSource(op.DesiredSource)
	}
	origin := model.AppOriginSource(*app)
	if op.DesiredOriginSource != nil {
		origin = cloneAppSource(op.DesiredOriginSource)
	}
	model.SetAppSourceState(app, origin, build)
}

func cloneAppSpec(in *model.AppSpec) *model.AppSpec {
	if in == nil {
		return nil
	}
	spec, _ := model.StripFugueInjectedAppEnvFromSpec(*in)
	in = &spec
	out := *in
	if len(in.Command) > 0 {
		out.Command = append([]string(nil), in.Command...)
	}
	if len(in.Args) > 0 {
		out.Args = append([]string(nil), in.Args...)
	}
	if len(in.Ports) > 0 {
		out.Ports = append([]int(nil), in.Ports...)
	}
	out.Env = cloneMap(in.Env)
	out.GeneratedEnv = cloneAppGeneratedEnv(in.GeneratedEnv)
	if len(in.Files) > 0 {
		out.Files = append([]model.AppFile(nil), in.Files...)
	}
	if in.Workspace != nil {
		workspace := *in.Workspace
		out.Workspace = &workspace
	}
	if in.NetworkPolicy != nil {
		out.NetworkPolicy = cloneAppNetworkPolicy(in.NetworkPolicy)
	}
	if in.PersistentStorage != nil {
		storage := *in.PersistentStorage
		if len(in.PersistentStorage.Mounts) > 0 {
			storage.Mounts = append([]model.AppPersistentStorageMount(nil), in.PersistentStorage.Mounts...)
		}
		out.PersistentStorage = &storage
	}
	if in.VolumeReplication != nil {
		replication := *in.VolumeReplication
		out.VolumeReplication = &replication
	}
	if in.Failover != nil {
		failover := *in.Failover
		out.Failover = &failover
	}
	if in.Resources != nil {
		resources := *in.Resources
		out.Resources = &resources
	}
	if in.RightSizing != nil {
		rightSizing := *in.RightSizing
		out.RightSizing = &rightSizing
	}
	if in.Postgres != nil {
		pg := *in.Postgres
		if in.Postgres.Resources != nil {
			resources := *in.Postgres.Resources
			pg.Resources = &resources
		}
		out.Postgres = &pg
	}
	model.ApplyAppSpecDefaults(&out)
	return &out
}

func cloneAppNetworkPolicy(in *model.AppNetworkPolicySpec) *model.AppNetworkPolicySpec {
	if in == nil {
		return nil
	}
	out := *in
	if in.Egress != nil {
		egress := *in.Egress
		egress.AllowApps = cloneAppNetworkPolicyPeers(in.Egress.AllowApps)
		out.Egress = &egress
	}
	if in.Ingress != nil {
		ingress := *in.Ingress
		ingress.AllowApps = cloneAppNetworkPolicyPeers(in.Ingress.AllowApps)
		out.Ingress = &ingress
	}
	return &out
}

func cloneAppNetworkPolicyPeers(in []model.AppNetworkPolicyAppPeer) []model.AppNetworkPolicyAppPeer {
	if len(in) == 0 {
		return nil
	}
	out := make([]model.AppNetworkPolicyAppPeer, len(in))
	for index, peer := range in {
		out[index] = peer
		if len(peer.Ports) > 0 {
			out[index].Ports = append([]int(nil), peer.Ports...)
		}
	}
	return out
}

func cloneAppGeneratedEnv(in map[string]model.AppGeneratedEnvSpec) map[string]model.AppGeneratedEnvSpec {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]model.AppGeneratedEnvSpec, len(in))
	for key, spec := range in {
		out[key] = spec
	}
	return out
}

func cloneAppRoute(in *model.AppRoute) *model.AppRoute {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func applyOperationToApp(state *model.State, op *model.Operation) error {
	appIndex := findApp(state, op.AppID)
	if appIndex < 0 {
		return ErrNotFound
	}
	now := time.Now().UTC()
	app := &state.Apps[appIndex]
	if isDeletedApp(*app) && op.Type != model.OperationTypeDelete {
		return nil
	}
	if op.DesiredSpec != nil {
		if err := applyGeneratedEnvSpec(op.DesiredSpec, &app.Spec); err != nil {
			return err
		}
	}
	switch op.Type {
	case model.OperationTypeImport:
		// Import only prepares the build artifact and queues a follow-up deploy.
	case model.OperationTypeDeploy:
		if op.DesiredSpec == nil {
			return ErrInvalidInput
		}
		if err := applyDesiredSpecBackingServicesState(state, app, op.DesiredSpec); err != nil {
			return err
		}
		app.Spec = *op.DesiredSpec
		if app.Route != nil {
			if model.AppExposesPublicService(app.Spec) {
				app.Route.ServicePort = model.AppPublicServicePort(app.Spec)
			} else {
				app.Route = nil
			}
		}
		applyOperationSourceStateToApp(app, *op)
		app.Status.Phase = "deployed"
		app.Status.CurrentRuntimeID = app.Spec.RuntimeID
		app.Status.CurrentReplicas = app.Spec.Replicas
		if op.ExecutionMode != model.ExecutionModeManaged {
			app.Status.CurrentReleaseStartedAt = nil
			app.Status.CurrentReleaseReadyAt = nil
		}
	case model.OperationTypeScale:
		if op.DesiredReplicas == nil {
			return ErrInvalidInput
		}
		app.Spec.Replicas = *op.DesiredReplicas
		if *op.DesiredReplicas == 0 {
			app.Status.Phase = "disabled"
		} else {
			app.Status.Phase = "scaled"
		}
		app.Status.CurrentRuntimeID = app.Spec.RuntimeID
		app.Status.CurrentReplicas = *op.DesiredReplicas
		if *op.DesiredReplicas == 0 || op.ExecutionMode != model.ExecutionModeManaged {
			app.Status.CurrentReleaseStartedAt = nil
			app.Status.CurrentReleaseReadyAt = nil
		}
	case model.OperationTypeDelete:
		app.Name = deletedAppName(app.Name, op.ID)
		app.Route = nil
		app.Spec.Replicas = 0
		app.Status.Phase = "deleted"
		app.Status.CurrentRuntimeID = ""
		app.Status.CurrentReplicas = 0
		app.Status.CurrentReleaseStartedAt = nil
		app.Status.CurrentReleaseReadyAt = nil
		state.ServiceBindings = deleteServiceBindingsByApp(state.ServiceBindings, app.ID)
		state.BackingServices = deleteOwnedBackingServicesByApp(state.BackingServices, app.ID)
	case model.OperationTypeMigrate:
		if op.DesiredSpec != nil {
			if err := applyDesiredSpecBackingServicesState(state, app, op.DesiredSpec); err != nil {
				return err
			}
		}
		if err := applyCompletedMigrateToAppModel(app, op); err != nil {
			return err
		}
	case model.OperationTypeFailover:
		if op.DesiredSpec != nil {
			if err := applyDesiredSpecBackingServicesState(state, app, op.DesiredSpec); err != nil {
				return err
			}
		}
		if err := applyCompletedFailoverToAppModel(app, op); err != nil {
			return err
		}
	case model.OperationTypeDatabaseSwitchover, model.OperationTypeDatabaseLocalize:
		if op.DesiredSpec == nil {
			return ErrInvalidInput
		}
		if err := applyDesiredSpecBackingServicesState(state, app, op.DesiredSpec); err != nil {
			return err
		}
		app.Spec = *op.DesiredSpec
	default:
		return ErrInvalidInput
	}
	if op.Type != model.OperationTypeDatabaseSwitchover && op.Type != model.OperationTypeDatabaseLocalize {
		app.Status.LastOperationID = op.ID
		app.Status.LastMessage = op.ResultMessage
	}
	app.Status.UpdatedAt = now
	app.UpdatedAt = now
	return nil
}

func applyInFlightOperationToApp(state *model.State, op *model.Operation) error {
	appIndex := findApp(state, op.AppID)
	if appIndex < 0 {
		return ErrNotFound
	}
	if op.Type == model.OperationTypeDatabaseSwitchover || op.Type == model.OperationTypeDatabaseLocalize {
		return nil
	}
	if isDeletedApp(state.Apps[appIndex]) && op.Type != model.OperationTypeDelete {
		return nil
	}
	phase, err := inFlightOperationPhase(*op)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	app := &state.Apps[appIndex]
	app.Status.Phase = phase
	app.Status.LastOperationID = op.ID
	app.Status.LastMessage = effectiveInFlightOperationMessage(*op)
	app.Status.UpdatedAt = now
	app.UpdatedAt = now
	return nil
}

func operationCanTransitionToCompleted(op model.Operation) bool {
	switch op.Status {
	case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
		return true
	default:
		return false
	}
}

func operationCanUpdateProgress(op model.Operation) bool {
	switch op.Status {
	case model.OperationStatusRunning, model.OperationStatusWaitingAgent:
		return true
	default:
		return false
	}
}

func isRequeueableManagedOperation(op model.Operation) bool {
	return op.Status == model.OperationStatusRunning && op.ExecutionMode == model.ExecutionModeManaged
}

func requeueManagedOperationState(op *model.Operation, message string) {
	now := time.Now().UTC()
	op.Status = model.OperationStatusPending
	op.ExecutionMode = model.ExecutionModeManaged
	op.AssignedRuntimeID = ""
	op.ManifestPath = ""
	op.ErrorMessage = ""
	op.StartedAt = nil
	op.CompletedAt = nil
	op.UpdatedAt = now
	if strings.TrimSpace(message) != "" {
		op.ResultMessage = strings.TrimSpace(message)
		return
	}
	op.ResultMessage = defaultInFlightOperationMessage(*op)
}

func inFlightOperationPhase(op model.Operation) (string, error) {
	switch op.Type {
	case model.OperationTypeImport:
		return "importing", nil
	case model.OperationTypeDeploy:
		return "deploying", nil
	case model.OperationTypeScale:
		if op.DesiredReplicas != nil && *op.DesiredReplicas == 0 {
			return "disabling", nil
		}
		return "scaling", nil
	case model.OperationTypeDelete:
		return "deleting", nil
	case model.OperationTypeMigrate:
		return "migrating", nil
	case model.OperationTypeFailover:
		return "failing-over", nil
	case model.OperationTypeDatabaseSwitchover:
		return "database-switchover", nil
	case model.OperationTypeDatabaseLocalize:
		return "database-localize", nil
	default:
		return "", ErrInvalidInput
	}
}

func effectiveInFlightOperationMessage(op model.Operation) string {
	message := strings.TrimSpace(op.ResultMessage)
	if message != "" {
		return message
	}
	return defaultInFlightOperationMessage(op)
}

func defaultInFlightOperationMessage(op model.Operation) string {
	switch op.Status {
	case model.OperationStatusPending:
		return operationActionLabel(op) + " queued"
	case model.OperationStatusRunning:
		return operationActionLabel(op) + " in progress"
	case model.OperationStatusWaitingAgent:
		return "task dispatched to external runtime agent"
	default:
		return ""
	}
}

func operationActionLabel(op model.Operation) string {
	switch op.Type {
	case model.OperationTypeImport:
		return "import"
	case model.OperationTypeDeploy:
		return "deploy"
	case model.OperationTypeScale:
		if op.DesiredReplicas != nil && *op.DesiredReplicas == 0 {
			return "disable"
		}
		return "scale"
	case model.OperationTypeDelete:
		return "delete"
	case model.OperationTypeMigrate:
		return "migrate"
	case model.OperationTypeFailover:
		return "failover"
	case model.OperationTypeDatabaseSwitchover:
		return "database switchover"
	case model.OperationTypeDatabaseLocalize:
		return "database localize"
	default:
		return "operation"
	}
}

func firstPositiveSpecPort(ports []int) int {
	for _, port := range ports {
		if port > 0 {
			return port
		}
	}
	return 0
}

func applyFailedOperationToApp(state *model.State, op *model.Operation) {
	appIndex := findApp(state, op.AppID)
	if appIndex < 0 {
		return
	}
	if op.Type == model.OperationTypeDatabaseSwitchover || op.Type == model.OperationTypeDatabaseLocalize {
		return
	}
	if isDeletedApp(state.Apps[appIndex]) && op.Type != model.OperationTypeDelete {
		return
	}
	now := time.Now().UTC()
	app := &state.Apps[appIndex]
	app.Status.Phase = failedPhaseForApp(*app)
	app.Status.LastOperationID = op.ID
	app.Status.LastMessage = strings.TrimSpace(op.ErrorMessage)
	app.Status.UpdatedAt = now
	app.UpdatedAt = now
}

func deleteProjectsByTenant(projects []model.Project, tenantID string) []model.Project {
	filtered := projects[:0]
	for _, project := range projects {
		if project.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, project)
	}
	return filtered
}

func deleteAppsByProject(apps []model.App, projectID string) []model.App {
	filtered := apps[:0]
	for _, app := range apps {
		if app.ProjectID == projectID {
			continue
		}
		filtered = append(filtered, app)
	}
	return filtered
}

func deleteAPIKeysByTenant(keys []model.APIKey, tenantID string) []model.APIKey {
	filtered := keys[:0]
	for _, key := range keys {
		if key.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, key)
	}
	return filtered
}

func deleteEnrollmentTokensByTenant(tokens []model.EnrollmentToken, tenantID string) []model.EnrollmentToken {
	filtered := tokens[:0]
	for _, token := range tokens {
		if token.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, token)
	}
	return filtered
}

func deleteNodeKeysByTenant(keys []model.NodeKey, tenantID string, deletedIDs map[string]struct{}) []model.NodeKey {
	filtered := keys[:0]
	for _, key := range keys {
		if key.TenantID == tenantID {
			deletedIDs[key.ID] = struct{}{}
			continue
		}
		filtered = append(filtered, key)
	}
	return filtered
}

func deleteLegacyMachinesByTenant(machines []model.Machine, tenantID string, deletedNodeKeyIDs map[string]struct{}) []model.Machine {
	filtered := machines[:0]
	for _, machine := range machines {
		if machine.TenantID == tenantID {
			continue
		}
		if _, ok := deletedNodeKeyIDs[machine.NodeKeyID]; ok {
			machine.NodeKeyID = ""
		}
		filtered = append(filtered, machine)
	}
	return filtered
}

func deleteRuntimesByTenant(runtimes []model.Runtime, tenantID string, deletedNodeKeyIDs, deletedRuntimeIDs map[string]struct{}) []model.Runtime {
	filtered := runtimes[:0]
	for _, runtime := range runtimes {
		if runtime.TenantID == tenantID {
			if deletedRuntimeIDs != nil {
				deletedRuntimeIDs[runtime.ID] = struct{}{}
			}
			continue
		}
		if _, ok := deletedNodeKeyIDs[runtime.NodeKeyID]; ok {
			runtime.NodeKeyID = ""
		}
		filtered = append(filtered, runtime)
	}
	return filtered
}

func deleteRuntimeAccessGrants(grants []model.RuntimeAccessGrant, tenantID string, deletedRuntimeIDs map[string]struct{}) []model.RuntimeAccessGrant {
	filtered := grants[:0]
	for _, grant := range grants {
		if grant.TenantID == tenantID {
			continue
		}
		if _, ok := deletedRuntimeIDs[grant.RuntimeID]; ok {
			continue
		}
		filtered = append(filtered, grant)
	}
	return filtered
}

func deleteRuntimeAccessGrantsByRuntime(grants []model.RuntimeAccessGrant, runtimeID string) []model.RuntimeAccessGrant {
	filtered := grants[:0]
	for _, grant := range grants {
		if grant.RuntimeID == runtimeID {
			continue
		}
		filtered = append(filtered, grant)
	}
	return filtered
}

func validateRuntimeDeletion(runtimeObj model.Runtime) error {
	if runtimeObj.Type == model.RuntimeTypeManagedShared {
		return fmt.Errorf("%w: managed shared runtimes cannot be deleted", ErrConflict)
	}
	if strings.TrimSpace(runtimeObj.TenantID) == "" {
		return fmt.Errorf("%w: runtime must belong to a tenant before it can be deleted", ErrConflict)
	}
	if !strings.EqualFold(strings.TrimSpace(runtimeObj.Status), model.RuntimeStatusOffline) {
		return fmt.Errorf("%w: runtime must be offline before it can be deleted", ErrConflict)
	}
	return nil
}

func runtimeHasDeleteBlockersState(state *model.State, runtimeID string) bool {
	runtimeID = strings.TrimSpace(runtimeID)
	if state == nil || runtimeID == "" {
		return false
	}

	for _, app := range state.Apps {
		if isDeletedApp(app) {
			continue
		}
		if strings.TrimSpace(app.Spec.RuntimeID) == runtimeID {
			return true
		}
		if app.Spec.Failover != nil && strings.TrimSpace(app.Spec.Failover.TargetRuntimeID) == runtimeID {
			return true
		}
		if app.Spec.Postgres != nil {
			for _, postgresRuntimeID := range managedPostgresReferencedRuntimeIDs(app.Spec.RuntimeID, *app.Spec.Postgres) {
				if postgresRuntimeID == runtimeID {
					return true
				}
			}
		}
		if strings.TrimSpace(app.Status.CurrentRuntimeID) == runtimeID {
			return true
		}
	}

	for _, service := range state.BackingServices {
		if service.Spec.Postgres == nil || isDeletedBackingService(service) {
			continue
		}
		for _, postgresRuntimeID := range managedPostgresReferencedRuntimeIDs("", *service.Spec.Postgres) {
			if postgresRuntimeID == runtimeID {
				return true
			}
		}
	}

	for _, op := range state.Operations {
		if !isActiveOperationStatus(op.Status) {
			continue
		}
		if strings.TrimSpace(op.SourceRuntimeID) == runtimeID {
			return true
		}
		if strings.TrimSpace(op.TargetRuntimeID) == runtimeID {
			return true
		}
		if strings.TrimSpace(op.AssignedRuntimeID) == runtimeID {
			return true
		}
		if op.DesiredSpec != nil && strings.TrimSpace(op.DesiredSpec.RuntimeID) == runtimeID {
			return true
		}
		if op.DesiredSpec != nil && op.DesiredSpec.Failover != nil && strings.TrimSpace(op.DesiredSpec.Failover.TargetRuntimeID) == runtimeID {
			return true
		}
		if op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
			for _, postgresRuntimeID := range managedPostgresReferencedRuntimeIDs(op.DesiredSpec.RuntimeID, *op.DesiredSpec.Postgres) {
				if postgresRuntimeID == runtimeID {
					return true
				}
			}
		}
	}

	return false
}

func deleteAppsByTenant(apps []model.App, tenantID string) []model.App {
	filtered := apps[:0]
	for _, app := range apps {
		if app.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, app)
	}
	return filtered
}

func deleteOperationsByApp(ops []model.Operation, appID string) []model.Operation {
	return deleteOperationsByAppIDs(ops, []string{appID})
}

func deleteOperationsByAppIDs(ops []model.Operation, appIDs []string) []model.Operation {
	if len(appIDs) == 0 {
		return ops
	}
	remove := make(map[string]struct{}, len(appIDs))
	for _, appID := range appIDs {
		if strings.TrimSpace(appID) == "" {
			continue
		}
		remove[appID] = struct{}{}
	}
	filtered := ops[:0]
	for _, op := range ops {
		if _, ok := remove[op.AppID]; ok {
			continue
		}
		filtered = append(filtered, op)
	}
	return filtered
}

func deleteOperationsByTenant(ops []model.Operation, tenantID string) []model.Operation {
	filtered := ops[:0]
	for _, op := range ops {
		if op.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, op)
	}
	return filtered
}

func deleteAuditEventsByTenant(events []model.AuditEvent, tenantID string) []model.AuditEvent {
	filtered := events[:0]
	for _, event := range events {
		if event.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, event)
	}
	return filtered
}

func findTenant(state *model.State, id string) int {
	for idx, tenant := range state.Tenants {
		if tenant.ID == id {
			return idx
		}
	}
	return -1
}

func findProject(state *model.State, id string) int {
	for idx, project := range state.Projects {
		if project.ID == id {
			return idx
		}
	}
	return -1
}

func findAPIKey(state *model.State, id string) int {
	for idx, key := range state.APIKeys {
		if key.ID == id {
			return idx
		}
	}
	return -1
}

func findRuntime(state *model.State, id string) int {
	for idx, runtime := range state.Runtimes {
		if runtime.ID == id {
			return idx
		}
	}
	return -1
}

func findNodeKey(state *model.State, id string) int {
	for idx, key := range state.NodeKeys {
		if key.ID == id {
			return idx
		}
	}
	return -1
}

func findRuntimeByFingerprintHash(state *model.State, tenantID, fingerprintHash string) int {
	bestIndex := -1
	for idx, runtime := range state.Runtimes {
		if runtime.TenantID != tenantID || runtime.FingerprintHash != fingerprintHash {
			continue
		}
		if bestIndex < 0 {
			bestIndex = idx
			continue
		}
		best := state.Runtimes[bestIndex]
		if runtime.UpdatedAt.After(best.UpdatedAt) || (runtime.UpdatedAt.Equal(best.UpdatedAt) && runtime.CreatedAt.After(best.CreatedAt)) {
			bestIndex = idx
		}
	}
	return bestIndex
}

func findRuntimeCandidate(state *model.State, tenantID, nodeKeyID, runtimeType, machineName, runtimeName, endpoint string) int {
	bestIndex := -1
	bestScore := 0
	for idx, runtime := range state.Runtimes {
		if runtime.TenantID != tenantID || runtime.NodeKeyID != nodeKeyID || runtime.Type != runtimeType {
			continue
		}
		score := 0
		if endpoint != "" && strings.EqualFold(strings.TrimSpace(runtime.Endpoint), strings.TrimSpace(endpoint)) {
			score += 1
		}
		if runtimeName != "" && strings.EqualFold(runtime.Name, strings.TrimSpace(runtimeName)) {
			score += 4
		}
		if runtimeName != "" && strings.EqualFold(runtime.ClusterNodeName, strings.TrimSpace(runtimeName)) {
			score += 4
		}
		if machineName != "" && strings.EqualFold(runtime.MachineName, strings.TrimSpace(machineName)) {
			score += 2
		}
		if score > bestScore {
			bestScore = score
			bestIndex = idx
		}
	}
	if bestScore == 0 {
		return -1
	}
	return bestIndex
}

func findApp(state *model.State, id string) int {
	for idx, app := range state.Apps {
		if app.ID == id {
			return idx
		}
	}
	return -1
}

func findOperation(state *model.State, id string) int {
	for idx, op := range state.Operations {
		if op.ID == id {
			return idx
		}
	}
	return -1
}

func projectBelongsToTenant(state *model.State, projectID, tenantID string) bool {
	for _, project := range state.Projects {
		if project.ID == projectID && project.TenantID == tenantID {
			return true
		}
	}
	return false
}

func projectDeleteRequested(state *model.State, projectID string) bool {
	if state.ProjectDeleteRequests == nil {
		return false
	}
	_, ok := state.ProjectDeleteRequests[strings.TrimSpace(projectID)]
	return ok
}

func projectDeleteFinalizedAuditEvent(project model.Project) model.AuditEvent {
	return model.AuditEvent{
		TenantID:   project.TenantID,
		ActorType:  model.ActorTypeSystem,
		ActorID:    "project-delete-finalizer",
		Action:     "project.delete",
		TargetType: "project",
		TargetID:   project.ID,
		Metadata: map[string]string{
			"finalized_from_request": "true",
			"name":                   project.Name,
		},
	}
}

func markProjectDeleteRequested(state *model.State, projectID string, requestedAt time.Time) bool {
	if state.ProjectDeleteRequests == nil {
		state.ProjectDeleteRequests = make(map[string]time.Time)
	}
	normalizedProjectID := strings.TrimSpace(projectID)
	_, alreadyRequested := state.ProjectDeleteRequests[normalizedProjectID]
	state.ProjectDeleteRequests[normalizedProjectID] = requestedAt
	return alreadyRequested
}

func clearProjectDeleteRequested(state *model.State, projectID string) {
	if state.ProjectDeleteRequests == nil {
		return
	}
	delete(state.ProjectDeleteRequests, strings.TrimSpace(projectID))
	if len(state.ProjectDeleteRequests) == 0 {
		state.ProjectDeleteRequests = nil
	}
}

func projectHasLiveApps(state *model.State, projectID string) bool {
	for _, app := range state.Apps {
		if app.ProjectID == projectID && !isDeletedApp(app) {
			return true
		}
	}
	return false
}

func deleteProjectFromState(state *model.State, projectID string) (model.Project, error) {
	index := findProject(state, projectID)
	if index < 0 {
		return model.Project{}, ErrNotFound
	}
	project := state.Projects[index]
	appIDs := appIDsForProject(state.Apps, projectID)
	state.Projects = append(state.Projects[:index], state.Projects[index+1:]...)
	state.ProjectRuntimeReservations = deleteProjectRuntimeReservationsByProject(state.ProjectRuntimeReservations, projectID)
	state.Apps = deleteAppsByProject(state.Apps, projectID)
	state.ServiceBindings = deleteServiceBindingsByAppIDs(state.ServiceBindings, appIDs)
	state.BackingServices = deleteBackingServicesByProject(state.BackingServices, projectID)
	state.Operations = deleteOperationsByAppIDs(state.Operations, appIDs)
	clearProjectDeleteRequested(state, projectID)
	return project, nil
}

func maybeFinalizeRequestedProjectDelete(state *model.State, projectID string) error {
	if !projectDeleteRequested(state, projectID) {
		return nil
	}
	if projectHasLiveApps(state, projectID) {
		return nil
	}
	deleteUnboundBackingServicesByProject(state, projectID)
	if projectHasLiveResources(state, projectID) {
		return nil
	}
	project, err := deleteProjectFromState(state, projectID)
	if err != nil {
		return err
	}
	state.AuditEvents = append(state.AuditEvents, projectDeleteFinalizedAuditEvent(project))
	return nil
}

func projectHasLiveResources(state *model.State, projectID string) bool {
	if projectHasLiveApps(state, projectID) {
		return true
	}
	for _, service := range state.BackingServices {
		if service.ProjectID == projectID && !isDeletedBackingService(service) {
			return true
		}
	}
	return false
}

func appIDsForProject(apps []model.App, projectID string) []string {
	out := make([]string, 0)
	for _, app := range apps {
		if app.ProjectID == projectID {
			out = append(out, app.ID)
		}
	}
	return out
}

func trimmedStringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		out[trimmed] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func sortedTrimmedStringKeys(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}

func runtimeVisibleToTenant(state *model.State, runtimeID, tenantID string) bool {
	index := findRuntime(state, runtimeID)
	if index < 0 {
		return false
	}
	runtime := state.Runtimes[index]
	if runtime.TenantID != "" && runtime.TenantID == tenantID {
		return true
	}
	if runtime.Type == model.RuntimeTypeManagedShared {
		return true
	}
	switch normalizeRuntimeAccessMode(runtime.Type, runtime.AccessMode) {
	case model.RuntimeAccessModePlatformShared, model.RuntimeAccessModePublic:
		return true
	}
	return findRuntimeAccessGrant(state, runtime.ID, tenantID) >= 0
}

func resolveProjectRuntimeID(project model.Project, requestedRuntimeID string) string {
	if runtimeID := strings.TrimSpace(requestedRuntimeID); runtimeID != "" {
		return runtimeID
	}
	if runtimeID := strings.TrimSpace(project.DefaultRuntimeID); runtimeID != "" {
		return runtimeID
	}
	return model.DefaultManagedRuntimeID
}

func defaultBackingServiceRuntimeForProject(spec model.BackingServiceSpec, project model.Project) model.BackingServiceSpec {
	defaultRuntimeID := strings.TrimSpace(project.DefaultRuntimeID)
	if defaultRuntimeID == "" || spec.Postgres == nil || strings.TrimSpace(spec.Postgres.RuntimeID) != "" {
		return spec
	}
	out := cloneBackingServiceSpec(spec)
	out.Postgres.RuntimeID = defaultRuntimeID
	return out
}

func findManagedOwnedRuntimeByNodeKeyAndName(state *model.State, nodeKeyID, runtimeName string) int {
	for idx := range state.Runtimes {
		runtime := state.Runtimes[idx]
		if runtime.Type != model.RuntimeTypeManagedOwned {
			continue
		}
		if runtime.NodeKeyID == nodeKeyID && strings.EqualFold(runtime.Name, strings.TrimSpace(runtimeName)) {
			return idx
		}
	}
	return -1
}

func nextAvailableRuntimeName(state *model.State, tenantID, requested string) string {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		requested = "node"
	}
	name := requested
	suffix := 2
	for runtimeNameExists(state, tenantID, name) {
		name = fmt.Sprintf("%s-%d", requested, suffix)
		suffix++
	}
	return name
}

func runtimeNameExists(state *model.State, tenantID, name string) bool {
	for _, runtime := range state.Runtimes {
		if runtime.TenantID == tenantID && strings.EqualFold(runtime.Name, name) {
			return true
		}
	}
	return false
}

func resetRuntimeSharing(state *model.State, runtimeID string) {
	if state == nil {
		return
	}
	state.RuntimeGrants = deleteRuntimeAccessGrantsByRuntime(state.RuntimeGrants, runtimeID)
}

func detachRuntimeOwnership(state *model.State, runtimeIndex int, now time.Time) {
	if state == nil || runtimeIndex < 0 || runtimeIndex >= len(state.Runtimes) {
		return
	}
	runtime := &state.Runtimes[runtimeIndex]
	detachMachineOwnershipByRuntime(state, runtime.ID, now)
	resetRuntimeSharing(state, runtime.ID)
	runtime.AccessMode = model.RuntimeAccessModePrivate
	runtime.PublicOffer = nil
	runtime.PoolMode = model.RuntimePoolModeDedicated
	runtime.Status = model.RuntimeStatusOffline
	runtime.NodeKeyID = ""
	runtime.ClusterNodeName = ""
	runtime.FingerprintPrefix = ""
	runtime.FingerprintHash = ""
	runtime.AgentKeyPrefix = ""
	runtime.AgentKeyHash = ""
	runtime.UpdatedAt = now
}

func detachMachineOwnershipByRuntime(state *model.State, runtimeID string, now time.Time) {
	if state == nil {
		return
	}
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return
	}
	for idx := range state.Machines {
		if strings.TrimSpace(state.Machines[idx].RuntimeID) != runtimeID {
			continue
		}
		machine := state.Machines[idx]
		machine.RuntimeID = ""
		machine.RuntimeName = ""
		machine.NodeKeyID = ""
		machine.ClusterNodeName = ""
		machine.Status = model.RuntimeStatusOffline
		machine.UpdatedAt = now
		state.Machines[idx] = machine
	}
}

func normalizeRuntimeAccessMode(runtimeType, accessMode string) string {
	switch strings.TrimSpace(accessMode) {
	case model.RuntimeAccessModePlatformShared:
		return model.RuntimeAccessModePlatformShared
	case model.RuntimeAccessModePublic:
		if runtimeType == model.RuntimeTypeManagedShared {
			return model.RuntimeAccessModePlatformShared
		}
		return model.RuntimeAccessModePublic
	case model.RuntimeAccessModePrivate:
		if runtimeType == model.RuntimeTypeManagedShared {
			return model.RuntimeAccessModePlatformShared
		}
		return model.RuntimeAccessModePrivate
	default:
		if runtimeType == model.RuntimeTypeManagedShared {
			return model.RuntimeAccessModePlatformShared
		}
		return model.RuntimeAccessModePrivate
	}
}

func findRuntimeAccessGrant(state *model.State, runtimeID, tenantID string) int {
	if state == nil {
		return -1
	}
	for idx, grant := range state.RuntimeGrants {
		if grant.RuntimeID == runtimeID && grant.TenantID == tenantID {
			return idx
		}
	}
	return -1
}

func findRuntimeIndexesByFingerprintHash(state *model.State, fingerprintHash string) []int {
	if state == nil || strings.TrimSpace(fingerprintHash) == "" {
		return nil
	}
	indexes := make([]int, 0, 1)
	for idx, runtime := range state.Runtimes {
		if runtime.FingerprintHash != fingerprintHash {
			continue
		}
		indexes = append(indexes, idx)
	}
	sort.Slice(indexes, func(i, j int) bool {
		left := state.Runtimes[indexes[i]]
		right := state.Runtimes[indexes[j]]
		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.After(right.UpdatedAt)
		}
		return left.CreatedAt.After(right.CreatedAt)
	})
	return indexes
}
