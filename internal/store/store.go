package store

import (
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
)

var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
	ErrInvalidInput = errors.New("invalid input")
)

type Store struct {
	path        string
	databaseURL string
	db          *sql.DB
	dbInitMu    sync.Mutex
	dbReady     bool
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
		return s.ensureDatabaseReady()
	}
	return s.withFileLockedState(true, func(state *model.State) error {
		ensureDefaults(state)
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
		state.APIKeys = deleteAPIKeysByTenant(state.APIKeys, id)
		state.EnrollmentTokens = deleteEnrollmentTokensByTenant(state.EnrollmentTokens, id)

		deletedNodeKeyIDs := make(map[string]struct{})
		state.NodeKeys = deleteNodeKeysByTenant(state.NodeKeys, id, deletedNodeKeyIDs)
		state.Runtimes = deleteRuntimesByTenant(state.Runtimes, id, deletedNodeKeyIDs)
		state.Apps = deleteAppsByTenant(state.Apps, id)
		state.Operations = deleteOperationsByTenant(state.Operations, id)
		state.AuditEvents = deleteAuditEventsByTenant(state.AuditEvents, id)
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

func (s *Store) CreateProject(tenantID, name, description string) (model.Project, error) {
	name = strings.TrimSpace(name)
	if tenantID == "" || name == "" {
		return model.Project{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateProject(tenantID, name, description)
	}

	var project model.Project
	err := s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		slug := model.Slugify(name)
		for _, existing := range state.Projects {
			if existing.TenantID == tenantID && existing.Slug == slug {
				return ErrConflict
			}
		}
		now := time.Now().UTC()
		project = model.Project{
			ID:          model.NewID("project"),
			TenantID:    tenantID,
			Name:        name,
			Slug:        slug,
			Description: strings.TrimSpace(description),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		state.Projects = append(state.Projects, project)
		return nil
	})
	return project, err
}

func (s *Store) ListAPIKeys(tenantID string, platformAdmin bool) ([]model.APIKey, error) {
	if s.usingDatabase() {
		return s.pgListAPIKeys(tenantID, platformAdmin)
	}
	var keys []model.APIKey
	err := s.withLockedState(false, func(state *model.State) error {
		for _, key := range state.APIKeys {
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

func (s *Store) CreateNodeKey(tenantID, label string) (model.NodeKey, string, error) {
	label = defaultNodeKeyLabel(label)
	if tenantID == "" {
		return model.NodeKey{}, "", ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateNodeKey(tenantID, label)
	}

	secret := model.NewSecret("fugue_nk")
	now := time.Now().UTC()
	key := model.NodeKey{
		ID:        model.NewID("nodekey"),
		TenantID:  tenantID,
		Label:     label,
		Prefix:    model.SecretPrefix(secret),
		Hash:      model.HashSecret(secret),
		Status:    model.NodeKeyStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}

	err := s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, tenantID) < 0 {
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

func (s *Store) BootstrapNode(secret, runtimeName, endpoint string, labels map[string]string) (model.NodeKey, model.Runtime, string, error) {
	secret = strings.TrimSpace(secret)
	runtimeName = strings.TrimSpace(runtimeName)
	if secret == "" {
		return model.NodeKey{}, model.Runtime{}, "", ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgBootstrapNode(secret, runtimeName, endpoint, labels)
	}

	var key model.NodeKey
	var runtime model.Runtime
	var runtimeSecret string
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

		runtimeSecret = model.NewSecret("fugue_rt")
		runtime = model.Runtime{
			ID:              model.NewID("runtime"),
			TenantID:        key.TenantID,
			Name:            nextAvailableRuntimeName(state, key.TenantID, runtimeName),
			Type:            model.RuntimeTypeExternalOwned,
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
		state.Runtimes = append(state.Runtimes, runtime)
		return nil
	})
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", err
	}

	return redactNodeKey(key), runtime, runtimeSecret, nil
}

func (s *Store) BootstrapClusterNode(secret, runtimeName, endpoint string, labels map[string]string) (model.NodeKey, model.Runtime, error) {
	secret = strings.TrimSpace(secret)
	runtimeName = strings.TrimSpace(runtimeName)
	if secret == "" {
		return model.NodeKey{}, model.Runtime{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgBootstrapClusterNode(secret, runtimeName, endpoint, labels)
	}

	var key model.NodeKey
	var runtime model.Runtime
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

		existingIndex := findManagedOwnedRuntimeByNodeKeyAndName(state, key.ID, runtimeName)
		if existingIndex >= 0 {
			state.Runtimes[existingIndex].Status = model.RuntimeStatusActive
			state.Runtimes[existingIndex].Endpoint = strings.TrimSpace(endpoint)
			state.Runtimes[existingIndex].Labels = cloneMap(labels)
			state.Runtimes[existingIndex].UpdatedAt = now
			state.Runtimes[existingIndex].LastHeartbeatAt = &now
			runtime = state.Runtimes[existingIndex]
			return nil
		}

		runtime = model.Runtime{
			ID:              model.NewID("runtime"),
			TenantID:        key.TenantID,
			Name:            nextAvailableRuntimeName(state, key.TenantID, runtimeName),
			Type:            model.RuntimeTypeManagedOwned,
			Status:          model.RuntimeStatusActive,
			Endpoint:        strings.TrimSpace(endpoint),
			Labels:          cloneMap(labels),
			NodeKeyID:       key.ID,
			LastHeartbeatAt: &now,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		state.Runtimes = append(state.Runtimes, runtime)
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

func (s *Store) ConsumeEnrollmentToken(secret, runtimeName, endpoint string, labels map[string]string) (model.Runtime, string, error) {
	secret = strings.TrimSpace(secret)
	runtimeName = strings.TrimSpace(runtimeName)
	if secret == "" {
		return model.Runtime{}, "", ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgConsumeEnrollmentToken(secret, runtimeName, endpoint, labels)
	}

	var runtime model.Runtime
	var runtimeSecret string
	err := s.withLockedState(true, func(state *model.State) error {
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

		name := nextAvailableRuntimeName(state, token.TenantID, runtimeName)
		runtimeSecret = model.NewSecret("fugue_rt")
		runtime = model.Runtime{
			ID:              model.NewID("runtime"),
			TenantID:        token.TenantID,
			Name:            name,
			Type:            model.RuntimeTypeExternalOwned,
			Status:          model.RuntimeStatusActive,
			Endpoint:        strings.TrimSpace(endpoint),
			Labels:          cloneMap(labels),
			AgentKeyPrefix:  model.SecretPrefix(runtimeSecret),
			AgentKeyHash:    model.HashSecret(runtimeSecret),
			LastHeartbeatAt: &now,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		state.Runtimes = append(state.Runtimes, runtime)
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
		hash := model.HashSecret(secret)
		now := time.Now().UTC()
		for idx := range state.Runtimes {
			if state.Runtimes[idx].AgentKeyHash != hash {
				continue
			}
			state.Runtimes[idx].LastHeartbeatAt = &now
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
		index := findRuntime(state, runtimeID)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		state.Runtimes[index].LastHeartbeatAt = &now
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

func (s *Store) MarkRuntimeOfflineStale(after time.Duration) (int, error) {
	if s.usingDatabase() {
		return s.pgMarkRuntimeOfflineStale(after)
	}
	var count int
	err := s.withLockedState(true, func(state *model.State) error {
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
		for _, runtime := range state.Runtimes {
			if runtime.Type != model.RuntimeTypeExternalOwned && runtime.Type != model.RuntimeTypeManagedOwned {
				continue
			}
			if platformAdmin || runtime.TenantID == tenantID {
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

func (s *Store) ListRuntimes(tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	if s.usingDatabase() {
		return s.pgListRuntimes(tenantID, platformAdmin)
	}
	var runtimes []model.Runtime
	err := s.withLockedState(false, func(state *model.State) error {
		for _, runtime := range state.Runtimes {
			if platformAdmin || runtime.TenantID == tenantID || runtime.Type == model.RuntimeTypeManagedShared {
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
		index := findRuntime(state, id)
		if index < 0 {
			return ErrNotFound
		}
		runtime = state.Runtimes[index]
		return nil
	})
	return runtime, err
}

func (s *Store) ListApps(tenantID string, platformAdmin bool) ([]model.App, error) {
	if s.usingDatabase() {
		return s.pgListApps(tenantID, platformAdmin)
	}
	var apps []model.App
	err := s.withLockedState(false, func(state *model.State) error {
		for _, app := range state.Apps {
			if platformAdmin || app.TenantID == tenantID {
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
		return nil
	})
	return app, err
}

func (s *Store) GetAppByHostname(hostname string) (model.App, error) {
	hostname = strings.TrimSpace(strings.ToLower(hostname))
	if s.usingDatabase() {
		return s.pgGetAppByHostname(hostname)
	}
	var app model.App
	err := s.withLockedState(false, func(state *model.State) error {
		for _, candidate := range state.Apps {
			if candidate.Route == nil {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(candidate.Route.Hostname), hostname) {
				app = candidate
				return nil
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

func (s *Store) createApp(tenantID, projectID, name, description string, spec model.AppSpec, source *model.AppSource, route *model.AppRoute) (model.App, error) {
	name = strings.TrimSpace(name)
	if tenantID == "" || projectID == "" || name == "" || spec.Image == "" || spec.Replicas < 1 {
		return model.App{}, ErrInvalidInput
	}
	if spec.RuntimeID == "" {
		spec.RuntimeID = "runtime_managed_shared"
	}
	if s.usingDatabase() {
		return s.pgCreateApp(tenantID, projectID, name, description, spec, source, route)
	}

	var app model.App
	err := s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		if !projectBelongsToTenant(state, projectID, tenantID) {
			return ErrNotFound
		}
		if !runtimeVisibleToTenant(state, spec.RuntimeID, tenantID) {
			return ErrNotFound
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
		app = model.App{
			ID:          model.NewID("app"),
			TenantID:    tenantID,
			ProjectID:   projectID,
			Name:        name,
			Description: strings.TrimSpace(description),
			Source:      cloneAppSource(source),
			Route:       cloneAppRoute(route),
			Spec:        spec,
			Status: model.AppStatus{
				Phase:           "created",
				CurrentReplicas: 0,
				UpdatedAt:       now,
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
		state.Apps = append(state.Apps, app)
		return nil
	})
	return app, err
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

		switch op.Type {
		case model.OperationTypeDeploy:
			if op.DesiredSpec == nil {
				return ErrInvalidInput
			}
			if !runtimeVisibleToTenant(state, op.DesiredSpec.RuntimeID, op.TenantID) {
				return ErrNotFound
			}
			op.TargetRuntimeID = op.DesiredSpec.RuntimeID
		case model.OperationTypeScale:
			if op.DesiredReplicas == nil || *op.DesiredReplicas < 1 {
				return ErrInvalidInput
			}
			op.TargetRuntimeID = app.Spec.RuntimeID
		case model.OperationTypeMigrate:
			if op.TargetRuntimeID == "" || !runtimeVisibleToTenant(state, op.TargetRuntimeID, op.TenantID) {
				return ErrNotFound
			}
			op.SourceRuntimeID = app.Spec.RuntimeID
		default:
			return ErrInvalidInput
		}

		now := time.Now().UTC()
		op.DesiredSpec = cloneAppSpec(op.DesiredSpec)
		op.DesiredSource = cloneAppSource(op.DesiredSource)
		op.ID = model.NewID("op")
		op.Status = model.OperationStatusPending
		op.ExecutionMode = model.ExecutionModeManaged
		op.CreatedAt = now
		op.UpdatedAt = now
		state.Operations = append(state.Operations, op)
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

func (s *Store) ClaimNextPendingOperation() (model.Operation, bool, error) {
	if s.usingDatabase() {
		return s.pgClaimNextPendingOperation()
	}
	var op model.Operation
	var found bool
	err := s.withLockedState(true, func(state *model.State) error {
		pending := make([]int, 0)
		for idx := range state.Operations {
			if state.Operations[idx].Status == model.OperationStatusPending {
				pending = append(pending, idx)
			}
		}
		if len(pending) == 0 {
			return nil
		}
		sort.Slice(pending, func(i, j int) bool {
			return state.Operations[pending[i]].CreatedAt.Before(state.Operations[pending[j]].CreatedAt)
		})
		index := pending[0]
		now := time.Now().UTC()
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
		}
		state.Operations[index].UpdatedAt = now
		op = state.Operations[index]
		found = true
		return nil
	})
	return op, found, err
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
		op = state.Operations[index]
		return nil
	})
	return op, err
}

func (s *Store) CompleteManagedOperation(id, manifestPath, message string) (model.Operation, error) {
	return s.completeOperation(id, "", manifestPath, message)
}

func (s *Store) CompleteAgentOperation(id, runtimeID, manifestPath, message string) (model.Operation, error) {
	return s.completeOperation(id, runtimeID, manifestPath, message)
}

func (s *Store) completeOperation(id, runtimeID, manifestPath, message string) (model.Operation, error) {
	if s.usingDatabase() {
		return s.pgCompleteOperation(id, runtimeID, manifestPath, message)
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
		op = state.Operations[index]
		return nil
	})
	return op, err
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

func (s *Store) ListAuditEvents(tenantID string, platformAdmin bool) ([]model.AuditEvent, error) {
	if s.usingDatabase() {
		return s.pgListAuditEvents(tenantID, platformAdmin)
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
	if state.APIKeys == nil {
		state.APIKeys = []model.APIKey{}
	}
	if state.EnrollmentTokens == nil {
		state.EnrollmentTokens = []model.EnrollmentToken{}
	}
	if state.NodeKeys == nil {
		state.NodeKeys = []model.NodeKey{}
	}
	if state.Runtimes == nil {
		state.Runtimes = []model.Runtime{}
	}
	if state.Apps == nil {
		state.Apps = []model.App{}
	}
	if state.Operations == nil {
		state.Operations = []model.Operation{}
	}
	if state.AuditEvents == nil {
		state.AuditEvents = []model.AuditEvent{}
	}
	if findRuntime(state, "runtime_managed_shared") < 0 {
		now := time.Now().UTC()
		state.Runtimes = append(state.Runtimes, model.Runtime{
			ID:        "runtime_managed_shared",
			Name:      "managed-shared",
			Type:      model.RuntimeTypeManagedShared,
			Status:    model.RuntimeStatusActive,
			Endpoint:  "in-cluster",
			Labels:    map[string]string{"managed": "true"},
			CreatedAt: now,
			UpdatedAt: now,
		})
	}
}

func redactAPIKey(key model.APIKey) model.APIKey {
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

func cloneAppSource(in *model.AppSource) *model.AppSource {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneAppSpec(in *model.AppSpec) *model.AppSpec {
	if in == nil {
		return nil
	}
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
	return &out
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
	switch op.Type {
	case model.OperationTypeDeploy:
		if op.DesiredSpec == nil {
			return ErrInvalidInput
		}
		app.Spec = *op.DesiredSpec
		if op.DesiredSource != nil {
			app.Source = cloneAppSource(op.DesiredSource)
		}
		app.Status.Phase = "deployed"
		app.Status.CurrentRuntimeID = app.Spec.RuntimeID
		app.Status.CurrentReplicas = app.Spec.Replicas
	case model.OperationTypeScale:
		if op.DesiredReplicas == nil {
			return ErrInvalidInput
		}
		app.Spec.Replicas = *op.DesiredReplicas
		app.Status.Phase = "scaled"
		app.Status.CurrentRuntimeID = app.Spec.RuntimeID
		app.Status.CurrentReplicas = *op.DesiredReplicas
	case model.OperationTypeMigrate:
		if op.TargetRuntimeID == "" {
			return ErrInvalidInput
		}
		app.Spec.RuntimeID = op.TargetRuntimeID
		app.Status.Phase = "migrated"
		app.Status.CurrentRuntimeID = op.TargetRuntimeID
		app.Status.CurrentReplicas = app.Spec.Replicas
	default:
		return ErrInvalidInput
	}
	app.Status.LastOperationID = op.ID
	app.Status.LastMessage = op.ResultMessage
	app.Status.UpdatedAt = now
	app.UpdatedAt = now
	return nil
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

func deleteRuntimesByTenant(runtimes []model.Runtime, tenantID string, deletedNodeKeyIDs map[string]struct{}) []model.Runtime {
	filtered := runtimes[:0]
	for _, runtime := range runtimes {
		if runtime.TenantID == tenantID {
			continue
		}
		if _, ok := deletedNodeKeyIDs[runtime.NodeKeyID]; ok {
			runtime.NodeKeyID = ""
		}
		filtered = append(filtered, runtime)
	}
	return filtered
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

func runtimeVisibleToTenant(state *model.State, runtimeID, tenantID string) bool {
	index := findRuntime(state, runtimeID)
	if index < 0 {
		return false
	}
	runtime := state.Runtimes[index]
	return runtime.Type == model.RuntimeTypeManagedShared || runtime.TenantID == tenantID
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
