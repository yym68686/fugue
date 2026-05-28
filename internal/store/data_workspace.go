package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const defaultDataBackendID = "data_backend_fugue_default_r2"
const defaultDataMultipartPartSize int64 = 64 * 1024 * 1024

type DataWorkspaceUpdate struct {
	Name             *string
	ProjectID        *string
	DefaultRegion    *string
	StorageBackendID *string
	Assets           *[]model.DataAsset
	QuotaBytes       *int64
}

func ensureDataDefaults(state *model.State) {
	if state.DataBackends == nil {
		state.DataBackends = []model.DataBackend{}
	}
	if state.DataBackendSecrets == nil {
		state.DataBackendSecrets = []model.DataBackendSecret{}
	}
	if state.DataWorkspaces == nil {
		state.DataWorkspaces = []model.DataWorkspace{}
	}
	if state.DataSnapshots == nil {
		state.DataSnapshots = []model.DataSnapshot{}
	}
	if state.DataTransfers == nil {
		state.DataTransfers = []model.DataTransfer{}
	}
	if state.DataGrants == nil {
		state.DataGrants = []model.DataGrant{}
	}
	if state.DataRuntimeCaches == nil {
		state.DataRuntimeCaches = []model.RuntimeDataCacheMetadata{}
	}
	if findDataBackend(state, defaultDataBackendID) < 0 {
		now := time.Now().UTC()
		state.DataBackends = append(state.DataBackends, model.DataBackend{
			ID:           defaultDataBackendID,
			Name:         "fugue-default-r2",
			Slug:         "fugue-default-r2",
			Provider:     model.DataBackendProviderCloudflareR2,
			Status:       "active",
			Capabilities: model.DataBackendCapabilitiesForProvider(model.DataBackendProviderCloudflareR2),
			CreatedAt:    now,
			UpdatedAt:    now,
		})
	}
	for idx := range state.DataBackends {
		state.DataBackends[idx] = normalizeDataBackend(state.DataBackends[idx])
	}
	for idx := range state.DataWorkspaces {
		state.DataWorkspaces[idx] = normalizeDataWorkspace(state.DataWorkspaces[idx])
	}
	for idx := range state.DataSnapshots {
		state.DataSnapshots[idx].Manifest = model.NormalizeDataManifest(state.DataSnapshots[idx].Manifest)
	}
}

func (s *Store) DefaultDataBackendID() string {
	return defaultDataBackendID
}

func (s *Store) SeedDefaultDataBackendFromEnv() error {
	if s == nil {
		return nil
	}
	if s.usingDatabase() {
		return s.pgSeedDefaultDataBackendFromEnv()
	}
	return s.withLockedState(true, seedDefaultDataBackendFromEnvInState)
}

func seedDefaultDataBackendFromEnvInState(state *model.State) error {
	backend, credentials, ok := defaultDataBackendConfigFromEnv()
	if !ok {
		return nil
	}
	index := findDataBackend(state, defaultDataBackendID)
	if index < 0 {
		state.DataBackends = append(state.DataBackends, backend)
		index = len(state.DataBackends) - 1
	} else {
		existing := state.DataBackends[index]
		if existing.CreatedAt.IsZero() {
			existing.CreatedAt = backend.CreatedAt
		}
		existing.Name = backend.Name
		existing.Slug = backend.Slug
		existing.Provider = backend.Provider
		existing.Bucket = backend.Bucket
		existing.Region = backend.Region
		existing.Endpoint = backend.Endpoint
		existing.BaseURL = backend.BaseURL
		existing.Prefix = backend.Prefix
		existing.Status = backend.Status
		existing.Capabilities = backend.Capabilities
		existing.Credentials = redactAccessKeyOnly(credentials)
		existing.UpdatedAt = backend.UpdatedAt
		backend = existing
	}
	if dataBackendCredentialsPresent(credentials) {
		secretID := backend.CredentialSecretID
		secretIndex := findDataBackendSecret(state, secretID, backend.ID)
		secret := model.DataBackendSecret{
			ID:        secretID,
			TenantID:  backend.TenantID,
			BackendID: backend.ID,
		}
		if secretIndex >= 0 {
			secret = state.DataBackendSecrets[secretIndex]
		}
		encrypted, err := encryptDataBackendSecret(secret, credentials)
		if err != nil {
			return err
		}
		backend.CredentialSecretID = encrypted.ID
		if secretIndex >= 0 {
			state.DataBackendSecrets[secretIndex] = encrypted
		} else {
			state.DataBackendSecrets = append(state.DataBackendSecrets, encrypted)
		}
	}
	state.DataBackends[index] = normalizeDataBackend(backend)
	return nil
}

func (s *Store) ListDataBackends(tenantID string, platformAdmin bool) ([]model.DataBackend, error) {
	if s.usingDatabase() {
		return s.pgListDataBackends(tenantID, platformAdmin)
	}
	var backends []model.DataBackend
	err := s.withLockedState(false, func(state *model.State) error {
		for _, backend := range state.DataBackends {
			if platformAdmin || strings.TrimSpace(backend.TenantID) == "" || backend.TenantID == tenantID {
				backends = append(backends, model.RedactDataBackendCredentials(normalizeDataBackend(backend)))
			}
		}
		sort.Slice(backends, func(i, j int) bool {
			return backends[i].Name < backends[j].Name
		})
		return nil
	})
	return backends, err
}

func (s *Store) GetDataBackend(idOrName, tenantID string, platformAdmin bool) (model.DataBackend, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" {
		return model.DataBackend{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetDataBackend(idOrName, tenantID, platformAdmin, true)
	}
	var backend model.DataBackend
	err := s.withLockedState(false, func(state *model.State) error {
		index := findDataBackendByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		backend = model.RedactDataBackendCredentials(normalizeDataBackend(state.DataBackends[index]))
		return nil
	})
	return backend, err
}

func (s *Store) CreateDataBackend(backend model.DataBackend) (model.DataBackend, error) {
	backend = normalizeDataBackend(backend)
	if backend.Name == "" || backend.Provider == "" {
		return model.DataBackend{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateDataBackend(backend)
	}
	credentials := backend.Credentials
	backend.Credentials = redactAccessKeyOnly(credentials)
	err := s.withLockedState(true, func(state *model.State) error {
		if findDataBackendBySlug(state, backend.TenantID, backend.Slug) >= 0 {
			return ErrConflict
		}
		if backend.ID == "" {
			backend.ID = model.NewID("data_backend")
		}
		now := time.Now().UTC()
		if backend.CreatedAt.IsZero() {
			backend.CreatedAt = now
		}
		backend.UpdatedAt = now
		if dataBackendCredentialsPresent(credentials) {
			secret, err := encryptDataBackendSecret(model.DataBackendSecret{
				ID:        model.NewID("data_backend_secret"),
				TenantID:  backend.TenantID,
				BackendID: backend.ID,
			}, credentials)
			if err != nil {
				return err
			}
			backend.CredentialSecretID = secret.ID
			state.DataBackendSecrets = append(state.DataBackendSecrets, secret)
		}
		state.DataBackends = append(state.DataBackends, backend)
		return nil
	})
	if err != nil {
		return model.DataBackend{}, err
	}
	return model.RedactDataBackendCredentials(backend), nil
}

func (s *Store) GetDataBackendForUse(idOrName, tenantID string, platformAdmin bool) (model.DataBackend, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" {
		return model.DataBackend{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetDataBackendForUse(idOrName, tenantID, platformAdmin)
	}
	var backend model.DataBackend
	err := s.withLockedState(false, func(state *model.State) error {
		index := findDataBackendByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		backend = normalizeDataBackend(state.DataBackends[index])
		secretIndex := findDataBackendSecret(state, backend.CredentialSecretID, backend.ID)
		if secretIndex >= 0 {
			credentials, err := decryptDataBackendSecret(state.DataBackendSecrets[secretIndex])
			if err != nil {
				return err
			}
			backend.Credentials = credentials
		}
		return nil
	})
	return backend, err
}

func (s *Store) DeleteDataBackend(idOrName, tenantID string, platformAdmin bool) (model.DataBackend, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" {
		return model.DataBackend{}, ErrInvalidInput
	}
	if idOrName == defaultDataBackendID || strings.EqualFold(idOrName, "fugue-default-r2") {
		return model.DataBackend{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteDataBackend(idOrName, tenantID, platformAdmin)
	}
	var deleted model.DataBackend
	err := s.withLockedState(true, func(state *model.State) error {
		index := findDataBackendByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		for _, workspace := range state.DataWorkspaces {
			if workspace.StorageBackendID == state.DataBackends[index].ID {
				return ErrConflict
			}
		}
		deleted = state.DataBackends[index]
		state.DataBackends = append(state.DataBackends[:index], state.DataBackends[index+1:]...)
		return nil
	})
	if err != nil {
		return model.DataBackend{}, err
	}
	return model.RedactDataBackendCredentials(deleted), nil
}

func (s *Store) RotateDataBackendCredentials(idOrName, tenantID string, platformAdmin bool, credentials model.DataBackendCredentials) (model.DataBackend, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" || !dataBackendCredentialsPresent(credentials) {
		return model.DataBackend{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRotateDataBackendCredentials(idOrName, tenantID, platformAdmin, credentials)
	}
	var rotated model.DataBackend
	err := s.withLockedState(true, func(state *model.State) error {
		index := findDataBackendByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		backend := normalizeDataBackend(state.DataBackends[index])
		secretIndex := findDataBackendSecret(state, backend.CredentialSecretID, backend.ID)
		secret := model.DataBackendSecret{
			ID:        model.NewID("data_backend_secret"),
			TenantID:  backend.TenantID,
			BackendID: backend.ID,
			CreatedAt: now,
		}
		if secretIndex >= 0 {
			secret = state.DataBackendSecrets[secretIndex]
		}
		secret.LastRotated = now
		encrypted, err := encryptDataBackendSecret(secret, credentials)
		if err != nil {
			return err
		}
		if secretIndex >= 0 {
			state.DataBackendSecrets[secretIndex] = encrypted
		} else {
			state.DataBackendSecrets = append(state.DataBackendSecrets, encrypted)
		}
		backend.CredentialSecretID = encrypted.ID
		backend.Credentials = redactAccessKeyOnly(credentials)
		backend.UpdatedAt = now
		state.DataBackends[index] = backend
		rotated = backend
		return nil
	})
	if err != nil {
		return model.DataBackend{}, err
	}
	return model.RedactDataBackendCredentials(rotated), nil
}

func (s *Store) ListDataWorkspaces(tenantID, projectID string, platformAdmin bool) ([]model.DataWorkspace, error) {
	if s.usingDatabase() {
		return s.pgListDataWorkspaces(tenantID, projectID, platformAdmin)
	}
	var workspaces []model.DataWorkspace
	err := s.withLockedState(false, func(state *model.State) error {
		for _, workspace := range state.DataWorkspaces {
			if !dataTenantAllowed(workspace.TenantID, tenantID, platformAdmin) {
				continue
			}
			if projectID != "" && workspace.ProjectID != projectID {
				continue
			}
			workspaces = append(workspaces, normalizeDataWorkspace(workspace))
		}
		sort.Slice(workspaces, func(i, j int) bool {
			return workspaces[i].UpdatedAt.After(workspaces[j].UpdatedAt)
		})
		return nil
	})
	return workspaces, err
}

func (s *Store) GetDataWorkspace(idOrName, tenantID string, platformAdmin bool) (model.DataWorkspace, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" {
		return model.DataWorkspace{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetDataWorkspace(idOrName, tenantID, platformAdmin)
	}
	var workspace model.DataWorkspace
	err := s.withLockedState(false, func(state *model.State) error {
		index := findDataWorkspaceByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		workspace = normalizeDataWorkspace(state.DataWorkspaces[index])
		return nil
	})
	return workspace, err
}

func (s *Store) CreateDataWorkspace(workspace model.DataWorkspace) (model.DataWorkspace, error) {
	workspace = normalizeDataWorkspace(workspace)
	if workspace.Name == "" {
		return model.DataWorkspace{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateDataWorkspace(workspace)
	}
	err := s.withLockedState(true, func(state *model.State) error {
		if workspace.TenantID != "" && findTenant(state, workspace.TenantID) < 0 {
			return ErrNotFound
		}
		if workspace.ProjectID != "" {
			projectIndex := findProject(state, workspace.ProjectID)
			if projectIndex < 0 || state.Projects[projectIndex].TenantID != workspace.TenantID {
				return ErrNotFound
			}
		}
		if findDataWorkspaceBySlug(state, workspace.TenantID, workspace.Slug) >= 0 {
			return ErrConflict
		}
		if workspace.StorageBackendID == "" {
			workspace.StorageBackendID = defaultDataBackendID
		}
		if findDataBackend(state, workspace.StorageBackendID) < 0 {
			return ErrNotFound
		}
		if workspace.ID == "" {
			workspace.ID = model.NewID("data_ws")
		}
		now := time.Now().UTC()
		if workspace.CreatedAt.IsZero() {
			workspace.CreatedAt = now
		}
		workspace.UpdatedAt = now
		state.DataWorkspaces = append(state.DataWorkspaces, workspace)
		return nil
	})
	return workspace, err
}

func (s *Store) UpdateDataWorkspace(idOrName, tenantID string, platformAdmin bool, update DataWorkspaceUpdate) (model.DataWorkspace, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" {
		return model.DataWorkspace{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateDataWorkspace(idOrName, tenantID, platformAdmin, update)
	}
	var updated model.DataWorkspace
	err := s.withLockedState(true, func(state *model.State) error {
		index := findDataWorkspaceByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		workspace := state.DataWorkspaces[index]
		if update.Name != nil {
			name := strings.TrimSpace(*update.Name)
			if name == "" {
				return ErrInvalidInput
			}
			nextSlug := model.Slugify(name)
			if nextSlug != workspace.Slug && findDataWorkspaceBySlug(state, workspace.TenantID, nextSlug) >= 0 {
				return ErrConflict
			}
			workspace.Name = name
			workspace.Slug = nextSlug
		}
		if update.ProjectID != nil {
			projectID := strings.TrimSpace(*update.ProjectID)
			if projectID != "" {
				projectIndex := findProject(state, projectID)
				if projectIndex < 0 || state.Projects[projectIndex].TenantID != workspace.TenantID {
					return ErrNotFound
				}
			}
			workspace.ProjectID = projectID
		}
		if update.DefaultRegion != nil {
			workspace.DefaultRegion = strings.TrimSpace(*update.DefaultRegion)
		}
		if update.StorageBackendID != nil {
			backendID := strings.TrimSpace(*update.StorageBackendID)
			if backendID == "" {
				backendID = defaultDataBackendID
			}
			if findDataBackendByIDNameOrSlug(state, backendID, workspace.TenantID, platformAdmin) < 0 {
				return ErrNotFound
			}
			workspace.StorageBackendID = backendID
		}
		if update.Assets != nil {
			workspace.Assets = normalizeDataAssets(*update.Assets)
		}
		if update.QuotaBytes != nil {
			workspace.QuotaBytes = *update.QuotaBytes
		}
		workspace.UpdatedAt = time.Now().UTC()
		state.DataWorkspaces[index] = normalizeDataWorkspace(workspace)
		updated = state.DataWorkspaces[index]
		return nil
	})
	return updated, err
}

func (s *Store) DeleteDataWorkspace(idOrName, tenantID string, platformAdmin bool) (model.DataWorkspace, error) {
	if s.usingDatabase() {
		return s.pgDeleteDataWorkspace(idOrName, tenantID, platformAdmin)
	}
	var deleted model.DataWorkspace
	err := s.withLockedState(true, func(state *model.State) error {
		index := findDataWorkspaceByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		deleted = state.DataWorkspaces[index]
		state.DataWorkspaces = append(state.DataWorkspaces[:index], state.DataWorkspaces[index+1:]...)
		filteredSnapshots := state.DataSnapshots[:0]
		for _, snapshot := range state.DataSnapshots {
			if snapshot.WorkspaceID != deleted.ID {
				filteredSnapshots = append(filteredSnapshots, snapshot)
			}
		}
		state.DataSnapshots = filteredSnapshots
		return nil
	})
	return deleted, err
}

func (s *Store) ListDataSnapshots(workspaceID string, includeDeleted bool) ([]model.DataSnapshot, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListDataSnapshots(workspaceID, includeDeleted)
	}
	var snapshots []model.DataSnapshot
	err := s.withLockedState(false, func(state *model.State) error {
		if findDataWorkspace(state, workspaceID) < 0 {
			return ErrNotFound
		}
		for _, snapshot := range state.DataSnapshots {
			if snapshot.WorkspaceID != workspaceID {
				continue
			}
			if snapshot.DeletedAt != nil && !includeDeleted {
				continue
			}
			snapshots = append(snapshots, snapshot)
		}
		sort.Slice(snapshots, func(i, j int) bool {
			return snapshots[i].CreatedAt.After(snapshots[j].CreatedAt)
		})
		return nil
	})
	return snapshots, err
}

func (s *Store) GetDataSnapshot(workspaceID, idOrVersion string) (model.DataSnapshot, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	idOrVersion = strings.TrimSpace(idOrVersion)
	if workspaceID == "" || idOrVersion == "" {
		return model.DataSnapshot{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetDataSnapshot(workspaceID, idOrVersion)
	}
	var snapshot model.DataSnapshot
	err := s.withLockedState(false, func(state *model.State) error {
		index := findDataSnapshotByIDOrVersion(state, workspaceID, idOrVersion)
		if index < 0 || state.DataSnapshots[index].DeletedAt != nil {
			return ErrNotFound
		}
		snapshot = state.DataSnapshots[index]
		return nil
	})
	return snapshot, err
}

func (s *Store) LatestDataSnapshot(workspaceID string) (model.DataSnapshot, error) {
	snapshots, err := s.ListDataSnapshots(workspaceID, false)
	if err != nil {
		return model.DataSnapshot{}, err
	}
	if len(snapshots) == 0 {
		return model.DataSnapshot{}, ErrNotFound
	}
	return snapshots[0], nil
}

func (s *Store) CreateDataSnapshot(snapshot model.DataSnapshot) (model.DataSnapshot, error) {
	snapshot.Manifest = model.NormalizeDataManifest(snapshot.Manifest)
	snapshot.Version = strings.TrimSpace(snapshot.Version)
	if snapshot.Version == "" {
		snapshot.Version = defaultDataSnapshotVersion(time.Now().UTC())
	}
	if strings.EqualFold(snapshot.Version, "latest") {
		return model.DataSnapshot{}, ErrInvalidInput
	}
	if snapshot.WorkspaceID == "" {
		return model.DataSnapshot{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateDataSnapshot(snapshot)
	}
	err := s.withLockedState(true, func(state *model.State) error {
		workspaceIndex := findDataWorkspace(state, snapshot.WorkspaceID)
		if workspaceIndex < 0 {
			return ErrNotFound
		}
		workspace := state.DataWorkspaces[workspaceIndex]
		if findDataSnapshotByVersion(state, snapshot.WorkspaceID, snapshot.Version) >= 0 {
			return ErrConflict
		}
		if snapshot.ID == "" {
			snapshot.ID = model.NewID("data_snap")
		}
		now := time.Now().UTC()
		if snapshot.CreatedAt.IsZero() {
			snapshot.CreatedAt = now
		}
		snapshot.TenantID = workspace.TenantID
		snapshot.ProjectID = workspace.ProjectID
		snapshot.Manifest.WorkspaceID = snapshot.WorkspaceID
		snapshot.Manifest.SnapshotID = snapshot.ID
		snapshot.ManifestDigest = strings.TrimSpace(snapshot.ManifestDigest)
		if snapshot.ManifestDigest == "" {
			snapshot.ManifestDigest = digestManifest(snapshot.Manifest)
		}
		snapshot.Manifest.Digest = snapshot.ManifestDigest
		snapshot.FileCount = snapshot.Manifest.FileCount
		snapshot.TotalBytes = snapshot.Manifest.TotalBytes
		snapshot.AssetCount = countManifestAssets(snapshot.Manifest)
		state.DataSnapshots = append(state.DataSnapshots, snapshot)
		state.DataWorkspaces[workspaceIndex].UsedBytes = totalWorkspaceSnapshotBytes(state.DataSnapshots, workspace.ID)
		state.DataWorkspaces[workspaceIndex].UpdatedAt = now
		return nil
	})
	return snapshot, err
}

func (s *Store) DeleteDataSnapshot(workspaceID, idOrVersion string) (model.DataSnapshot, error) {
	if s.usingDatabase() {
		return s.pgDeleteDataSnapshot(workspaceID, idOrVersion)
	}
	var deleted model.DataSnapshot
	err := s.withLockedState(true, func(state *model.State) error {
		index := findDataSnapshotByIDOrVersion(state, workspaceID, idOrVersion)
		if index < 0 || state.DataSnapshots[index].DeletedAt != nil {
			return ErrNotFound
		}
		now := time.Now().UTC()
		state.DataSnapshots[index].DeletedAt = &now
		deleted = state.DataSnapshots[index]
		return nil
	})
	return deleted, err
}

func (s *Store) CreateDataTransfer(transfer model.DataTransfer) (model.DataTransfer, error) {
	transfer.WorkspaceID = strings.TrimSpace(transfer.WorkspaceID)
	transfer.Direction = strings.TrimSpace(transfer.Direction)
	transfer.Manifest = model.NormalizeDataManifest(transfer.Manifest)
	if transfer.WorkspaceID == "" || transfer.Direction == "" {
		return model.DataTransfer{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateDataTransfer(transfer)
	}
	err := s.withLockedState(true, func(state *model.State) error {
		workspaceIndex := findDataWorkspace(state, transfer.WorkspaceID)
		if workspaceIndex < 0 {
			return ErrNotFound
		}
		workspace := state.DataWorkspaces[workspaceIndex]
		if transfer.ID == "" {
			transfer.ID = model.NewID("data_transfer")
		}
		now := time.Now().UTC()
		if transfer.CreatedAt.IsZero() {
			transfer.CreatedAt = now
		}
		transfer.UpdatedAt = now
		if transfer.Status == "" {
			transfer.Status = model.DataTransferStatusPlanned
		}
		transfer.TenantID = workspace.TenantID
		if transfer.PartSize == 0 {
			transfer.PartSize = defaultDataMultipartPartSize
		}
		state.DataTransfers = append(state.DataTransfers, transfer)
		return nil
	})
	return transfer, err
}

func (s *Store) UpdateDataTransfer(transfer model.DataTransfer) (model.DataTransfer, error) {
	if transfer.ID == "" {
		return model.DataTransfer{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateDataTransfer(transfer)
	}
	var updated model.DataTransfer
	err := s.withLockedState(true, func(state *model.State) error {
		index := findDataTransfer(state, transfer.ID)
		if index < 0 {
			return ErrNotFound
		}
		current := state.DataTransfers[index]
		if transfer.Status != "" {
			current.Status = transfer.Status
		}
		if transfer.SnapshotID != "" {
			current.SnapshotID = transfer.SnapshotID
		}
		if transfer.Version != "" {
			current.Version = transfer.Version
		}
		if transfer.Message != "" {
			current.Message = transfer.Message
		}
		if len(transfer.Manifest.Entries) > 0 {
			current.Manifest = model.NormalizeDataManifest(transfer.Manifest)
		}
		if transfer.PlanBlobs != nil {
			current.PlanBlobs = transfer.PlanBlobs
		}
		if transfer.PartSize > 0 {
			current.PartSize = transfer.PartSize
		}
		if transfer.ExpiresAt != nil {
			current.ExpiresAt = transfer.ExpiresAt
		}
		current.BytesTotal = transfer.BytesTotal
		current.BytesDone = transfer.BytesDone
		current.FilesTotal = transfer.FilesTotal
		current.FilesDone = transfer.FilesDone
		current.ErrorCode = transfer.ErrorCode
		current.ErrorMessage = transfer.ErrorMessage
		current.StartedAt = transfer.StartedAt
		current.FinishedAt = transfer.FinishedAt
		current.UpdatedAt = time.Now().UTC()
		state.DataTransfers[index] = current
		updated = current
		return nil
	})
	return updated, err
}

func (s *Store) GetDataTransfer(id string) (model.DataTransfer, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.DataTransfer{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetDataTransfer(id)
	}
	var transfer model.DataTransfer
	err := s.withLockedState(false, func(state *model.State) error {
		index := findDataTransfer(state, id)
		if index < 0 {
			return ErrNotFound
		}
		transfer = state.DataTransfers[index]
		return nil
	})
	return transfer, err
}

func (s *Store) ListDataTransfers(tenantID, workspaceID string, platformAdmin bool) ([]model.DataTransfer, error) {
	if s.usingDatabase() {
		return s.pgListDataTransfers(tenantID, workspaceID, platformAdmin)
	}
	var transfers []model.DataTransfer
	err := s.withLockedState(false, func(state *model.State) error {
		for _, transfer := range state.DataTransfers {
			if !dataTenantAllowed(transfer.TenantID, tenantID, platformAdmin) {
				continue
			}
			if workspaceID != "" && transfer.WorkspaceID != workspaceID {
				continue
			}
			transfers = append(transfers, transfer)
		}
		sort.Slice(transfers, func(i, j int) bool {
			return transfers[i].UpdatedAt.After(transfers[j].UpdatedAt)
		})
		return nil
	})
	return transfers, err
}

func (s *Store) CancelDataTransfer(id string) (model.DataTransfer, error) {
	transfer, err := s.GetDataTransfer(id)
	if err != nil {
		return model.DataTransfer{}, err
	}
	now := time.Now().UTC()
	transfer.Status = model.DataTransferStatusCanceled
	transfer.FinishedAt = &now
	return s.UpdateDataTransfer(transfer)
}

func (s *Store) CreateDataGrant(grant model.DataGrant) (model.DataGrant, string, error) {
	grant.WorkspaceID = strings.TrimSpace(grant.WorkspaceID)
	grant.Mode = strings.TrimSpace(grant.Mode)
	if grant.WorkspaceID == "" || grant.Mode == "" {
		return model.DataGrant{}, "", ErrInvalidInput
	}
	secret := model.NewSecret("fugue_data_grant")
	grant.TokenPrefix = model.SecretPrefix(secret)
	grant.TokenHash = model.HashSecret(secret)
	if s.usingDatabase() {
		created, err := s.pgCreateDataGrant(grant)
		return created, secret, err
	}
	err := s.withLockedState(true, func(state *model.State) error {
		workspaceIndex := findDataWorkspace(state, grant.WorkspaceID)
		if workspaceIndex < 0 {
			return ErrNotFound
		}
		workspace := state.DataWorkspaces[workspaceIndex]
		if grant.ID == "" {
			grant.ID = model.NewID("data_grant")
		}
		now := time.Now().UTC()
		if grant.CreatedAt.IsZero() {
			grant.CreatedAt = now
		}
		if grant.Status == "" {
			grant.Status = model.DataGrantStatusActive
		}
		grant.TenantID = workspace.TenantID
		state.DataGrants = append(state.DataGrants, grant)
		return nil
	})
	if err != nil {
		return model.DataGrant{}, "", err
	}
	return redactDataGrant(grant), secret, nil
}

func (s *Store) ListDataGrants(workspaceID string) ([]model.DataGrant, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListDataGrants(workspaceID)
	}
	var grants []model.DataGrant
	err := s.withLockedState(false, func(state *model.State) error {
		if findDataWorkspace(state, workspaceID) < 0 {
			return ErrNotFound
		}
		for _, grant := range state.DataGrants {
			if grant.WorkspaceID == workspaceID {
				grants = append(grants, redactDataGrant(grant))
			}
		}
		sort.Slice(grants, func(i, j int) bool {
			return grants[i].CreatedAt.After(grants[j].CreatedAt)
		})
		return nil
	})
	return grants, err
}

func (s *Store) RevokeDataGrant(id string) (model.DataGrant, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.DataGrant{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRevokeDataGrant(id)
	}
	var revoked model.DataGrant
	err := s.withLockedState(true, func(state *model.State) error {
		index := findDataGrant(state, id)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		state.DataGrants[index].Status = model.DataGrantStatusRevoked
		state.DataGrants[index].RevokedAt = &now
		revoked = redactDataGrant(state.DataGrants[index])
		return nil
	})
	return revoked, err
}

func (s *Store) GetDataGrant(id string) (model.DataGrant, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.DataGrant{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetDataGrant(id)
	}
	var grant model.DataGrant
	err := s.withLockedState(false, func(state *model.State) error {
		index := findDataGrant(state, id)
		if index < 0 {
			return ErrNotFound
		}
		grant = redactDataGrant(state.DataGrants[index])
		return nil
	})
	return grant, err
}

func (s *Store) AuthenticateDataGrant(secret string) (model.DataGrant, error) {
	hash := model.HashSecret(strings.TrimSpace(secret))
	if hash == "" {
		return model.DataGrant{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgAuthenticateDataGrant(hash)
	}
	var grant model.DataGrant
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		for index := range state.DataGrants {
			current := state.DataGrants[index]
			if subtle.ConstantTimeCompare([]byte(current.TokenHash), []byte(hash)) != 1 {
				continue
			}
			if current.Status != model.DataGrantStatusActive {
				return ErrNotFound
			}
			if current.ExpiresAt != nil && current.ExpiresAt.Before(now) {
				state.DataGrants[index].Status = model.DataGrantStatusExpired
				return ErrNotFound
			}
			state.DataGrants[index].LastUsedAt = &now
			grant = redactDataGrant(state.DataGrants[index])
			return nil
		}
		return ErrNotFound
	})
	return grant, err
}

func (s *Store) DataBlobExists(sha256Digest string) bool {
	_, err := os.Stat(s.dataBlobPath(sha256Digest))
	return err == nil
}

func (s *Store) WriteDataBlob(sha256Digest string, reader io.Reader) (int64, error) {
	digest := strings.TrimSpace(strings.ToLower(sha256Digest))
	if len(digest) != 64 {
		return 0, ErrInvalidInput
	}
	target := s.dataBlobPath(digest)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return 0, fmt.Errorf("create data blob directory: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(target), ".tmp-*")
	if err != nil {
		return 0, fmt.Errorf("create data blob temp file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(temp, hasher), reader)
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return 0, fmt.Errorf("write data blob: %w", err)
	}
	got := hex.EncodeToString(hasher.Sum(nil))
	if got != digest {
		return 0, fmt.Errorf("%w: checksum mismatch: expected %s got %s", ErrInvalidInput, digest, got)
	}
	if err := os.Rename(tempPath, target); err != nil {
		return 0, fmt.Errorf("commit data blob: %w", err)
	}
	return written, nil
}

func (s *Store) OpenDataBlob(sha256Digest string) (*os.File, os.FileInfo, error) {
	path := s.dataBlobPath(sha256Digest)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	return file, info, nil
}

func (s *Store) ListDataBlobDigests() ([]model.DataGCSweepCandidate, error) {
	root := filepath.Join(s.dataBlobRoot(), "sha256")
	var candidates []model.DataGCSweepCandidate
	err := filepath.WalkDir(root, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		name := entry.Name()
		if len(name) != 64 {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		candidates = append(candidates, model.DataGCSweepCandidate{Key: name, Size: info.Size(), LastModified: info.ModTime().UTC()})
		return nil
	})
	if os.IsNotExist(err) {
		return nil, nil
	}
	return candidates, err
}

func (s *Store) DeleteDataBlobDigest(sha256Digest string) error {
	err := os.Remove(s.dataBlobPath(sha256Digest))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (s *Store) dataBlobRoot() string {
	baseDir := filepath.Dir(strings.TrimSpace(s.path))
	if baseDir == "" || baseDir == "." {
		baseDir = "."
	}
	return filepath.Join(baseDir, "data-blobs")
}

func (s *Store) dataBlobPath(sha256Digest string) string {
	digest := strings.TrimSpace(strings.ToLower(sha256Digest))
	if len(digest) >= 4 {
		return filepath.Join(s.dataBlobRoot(), "sha256", digest[:2], digest[2:4], digest)
	}
	return filepath.Join(s.dataBlobRoot(), "sha256", digest)
}

func normalizeDataBackend(backend model.DataBackend) model.DataBackend {
	backend.Name = strings.TrimSpace(backend.Name)
	if backend.Name == "" && backend.ID == defaultDataBackendID {
		backend.Name = "fugue-default-r2"
	}
	backend.Slug = model.SlugifyOptional(backend.Slug)
	if backend.Slug == "" {
		backend.Slug = model.Slugify(backend.Name)
	}
	backend.Provider = model.NormalizeDataBackendProvider(backend.Provider)
	backend.Bucket = strings.TrimSpace(backend.Bucket)
	backend.Region = strings.TrimSpace(backend.Region)
	backend.Endpoint = strings.TrimRight(strings.TrimSpace(backend.Endpoint), "/")
	backend.BaseURL = strings.TrimRight(strings.TrimSpace(backend.BaseURL), "/")
	backend.Prefix = strings.Trim(strings.TrimSpace(backend.Prefix), "/")
	backend.Status = strings.TrimSpace(backend.Status)
	if backend.Status == "" {
		backend.Status = "active"
	}
	if backend.Capabilities == (model.DataBackendCapabilities{}) {
		backend.Capabilities = model.DataBackendCapabilitiesForProvider(backend.Provider)
	}
	return backend
}

func normalizeDataWorkspace(workspace model.DataWorkspace) model.DataWorkspace {
	workspace.Name = strings.TrimSpace(workspace.Name)
	workspace.Slug = model.SlugifyOptional(workspace.Slug)
	if workspace.Slug == "" {
		workspace.Slug = model.Slugify(workspace.Name)
	}
	workspace.ProjectID = strings.TrimSpace(workspace.ProjectID)
	workspace.DefaultRegion = strings.TrimSpace(workspace.DefaultRegion)
	workspace.StorageBackendID = strings.TrimSpace(workspace.StorageBackendID)
	if workspace.StorageBackendID == "" {
		workspace.StorageBackendID = defaultDataBackendID
	}
	workspace.Assets = normalizeDataAssets(workspace.Assets)
	return workspace
}

func normalizeDataAssets(assets []model.DataAsset) []model.DataAsset {
	out := make([]model.DataAsset, 0, len(assets))
	seen := map[string]struct{}{}
	for _, asset := range assets {
		asset = model.NormalizeDataAsset(asset)
		if asset.Name == "" || asset.Path == "" {
			continue
		}
		if _, exists := seen[asset.Name]; exists {
			continue
		}
		seen[asset.Name] = struct{}{}
		out = append(out, asset)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func dataTenantAllowed(resourceTenantID, tenantID string, platformAdmin bool) bool {
	if platformAdmin {
		return true
	}
	if strings.TrimSpace(resourceTenantID) == "" {
		return true
	}
	return strings.TrimSpace(resourceTenantID) == strings.TrimSpace(tenantID)
}

func findDataBackend(state *model.State, id string) int {
	for idx, backend := range state.DataBackends {
		if backend.ID == id {
			return idx
		}
	}
	return -1
}

func findDataBackendBySlug(state *model.State, tenantID, slug string) int {
	slug = model.Slugify(slug)
	for idx, backend := range state.DataBackends {
		if backend.Slug == slug && strings.TrimSpace(backend.TenantID) == strings.TrimSpace(tenantID) {
			return idx
		}
	}
	return -1
}

func findDataBackendByIDNameOrSlug(state *model.State, value, tenantID string, platformAdmin bool) int {
	value = strings.TrimSpace(value)
	slug := model.Slugify(value)
	for idx, backend := range state.DataBackends {
		if !dataTenantAllowed(backend.TenantID, tenantID, platformAdmin) {
			continue
		}
		if backend.ID == value || strings.EqualFold(backend.Name, value) || backend.Slug == slug {
			return idx
		}
	}
	return -1
}

func findDataWorkspace(state *model.State, id string) int {
	for idx, workspace := range state.DataWorkspaces {
		if workspace.ID == id {
			return idx
		}
	}
	return -1
}

func findDataWorkspaceBySlug(state *model.State, tenantID, slug string) int {
	slug = model.Slugify(slug)
	for idx, workspace := range state.DataWorkspaces {
		if workspace.Slug == slug && workspace.TenantID == tenantID {
			return idx
		}
	}
	return -1
}

func findDataWorkspaceByIDNameOrSlug(state *model.State, value, tenantID string, platformAdmin bool) int {
	value = strings.TrimSpace(value)
	slug := model.Slugify(value)
	for idx, workspace := range state.DataWorkspaces {
		if !dataTenantAllowed(workspace.TenantID, tenantID, platformAdmin) {
			continue
		}
		if workspace.ID == value || strings.EqualFold(workspace.Name, value) || workspace.Slug == slug {
			return idx
		}
	}
	return -1
}

func findDataSnapshotByVersion(state *model.State, workspaceID, version string) int {
	for idx, snapshot := range state.DataSnapshots {
		if snapshot.WorkspaceID == workspaceID && snapshot.Version == version {
			return idx
		}
	}
	return -1
}

func findDataSnapshotByIDOrVersion(state *model.State, workspaceID, value string) int {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "latest") {
		latest := -1
		for idx, snapshot := range state.DataSnapshots {
			if snapshot.WorkspaceID != workspaceID || snapshot.DeletedAt != nil {
				continue
			}
			if latest < 0 || snapshot.CreatedAt.After(state.DataSnapshots[latest].CreatedAt) {
				latest = idx
			}
		}
		return latest
	}
	for idx, snapshot := range state.DataSnapshots {
		if snapshot.WorkspaceID == workspaceID && (snapshot.ID == value || snapshot.Version == value) {
			return idx
		}
	}
	return -1
}

func findDataTransfer(state *model.State, id string) int {
	for idx, transfer := range state.DataTransfers {
		if transfer.ID == id {
			return idx
		}
	}
	return -1
}

func findDataGrant(state *model.State, id string) int {
	for idx, grant := range state.DataGrants {
		if grant.ID == id {
			return idx
		}
	}
	return -1
}

func findDataBackendSecret(state *model.State, id, backendID string) int {
	for idx, secret := range state.DataBackendSecrets {
		if id != "" && secret.ID == id {
			return idx
		}
		if id == "" && secret.BackendID == backendID {
			return idx
		}
	}
	return -1
}

func digestManifest(manifest model.DataManifest) string {
	manifest = model.NormalizeDataManifest(manifest)
	raw, _ := json.Marshal(manifest.Entries)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func countManifestAssets(manifest model.DataManifest) int {
	seen := map[string]struct{}{}
	for _, entry := range manifest.Entries {
		if strings.TrimSpace(entry.AssetName) != "" {
			seen[entry.AssetName] = struct{}{}
		}
	}
	return len(seen)
}

func totalWorkspaceSnapshotBytes(snapshots []model.DataSnapshot, workspaceID string) int64 {
	seen := map[string]struct{}{}
	var total int64
	for _, snapshot := range snapshots {
		if snapshot.WorkspaceID != workspaceID || snapshot.DeletedAt != nil {
			continue
		}
		for _, entry := range snapshot.Manifest.Entries {
			if entry.Kind != model.DataManifestEntryKindFile || entry.SHA256 == "" {
				continue
			}
			if _, exists := seen[entry.SHA256]; exists {
				continue
			}
			seen[entry.SHA256] = struct{}{}
			total += entry.Size
		}
	}
	return total
}

func defaultDataSnapshotVersion(now time.Time) string {
	return "v" + now.UTC().Format("20060102-150405") + "-" + strings.TrimPrefix(model.NewID("snap"), "snap_")
}

func defaultDataBackendConfigFromEnv() (model.DataBackend, model.DataBackendCredentials, bool) {
	bucket := strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_BUCKET"))
	accessKeyID := strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_ACCESS_KEY_ID"))
	secretAccessKey := strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY"))
	sessionToken := strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_SESSION_TOKEN"))
	endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_ENDPOINT")), "/")
	accountID := strings.TrimSpace(os.Getenv("FUGUE_DATA_R2_ACCOUNT_ID"))
	provider := model.NormalizeDataBackendProvider(os.Getenv("FUGUE_DATA_BACKEND_PROVIDER"))
	if provider == model.DataBackendProviderFugueManaged {
		provider = model.DataBackendProviderCloudflareR2
	}
	if endpoint == "" && provider == model.DataBackendProviderCloudflareR2 && accountID != "" {
		endpoint = "https://" + accountID + ".r2.cloudflarestorage.com"
	}
	credentials := model.DataBackendCredentials{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Token:           sessionToken,
	}
	if bucket == "" && endpoint == "" && !dataBackendCredentialsPresent(credentials) {
		return model.DataBackend{}, model.DataBackendCredentials{}, false
	}
	now := time.Now().UTC()
	backend := normalizeDataBackend(model.DataBackend{
		ID:           defaultDataBackendID,
		Name:         "fugue-default-r2",
		Slug:         "fugue-default-r2",
		Provider:     provider,
		Bucket:       bucket,
		Region:       strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_REGION")),
		Endpoint:     endpoint,
		Prefix:       strings.Trim(strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_PREFIX")), "/"),
		Status:       "active",
		Capabilities: model.DataBackendCapabilitiesForProvider(provider),
		Credentials:  redactAccessKeyOnly(credentials),
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	return backend, credentials, true
}

func dataBackendCredentialsPresent(credentials model.DataBackendCredentials) bool {
	return strings.TrimSpace(credentials.AccessKeyID) != "" ||
		strings.TrimSpace(credentials.SecretAccessKey) != "" ||
		strings.TrimSpace(credentials.Token) != ""
}

func redactAccessKeyOnly(credentials model.DataBackendCredentials) model.DataBackendCredentials {
	return model.DataBackendCredentials{AccessKeyID: strings.TrimSpace(credentials.AccessKeyID)}
}

func encryptDataBackendSecret(secret model.DataBackendSecret, credentials model.DataBackendCredentials) (model.DataBackendSecret, error) {
	key, keyID := dataBackendSecretKey()
	raw, err := json.Marshal(credentials)
	if err != nil {
		return model.DataBackendSecret{}, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return model.DataBackendSecret{}, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return model.DataBackendSecret{}, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return model.DataBackendSecret{}, err
	}
	ciphertext := gcm.Seal(nil, nonce, raw, nil)
	secret.Ciphertext = base64.StdEncoding.EncodeToString(append(nonce, ciphertext...))
	secret.KeyID = keyID
	now := time.Now().UTC()
	if secret.ID == "" {
		secret.ID = model.NewID("data_backend_secret")
	}
	if secret.CreatedAt.IsZero() {
		secret.CreatedAt = now
	}
	secret.UpdatedAt = now
	if secret.LastRotated.IsZero() {
		secret.LastRotated = now
	}
	return secret, nil
}

func decryptDataBackendSecret(secret model.DataBackendSecret) (model.DataBackendCredentials, error) {
	key, _ := dataBackendSecretKey()
	raw, err := base64.StdEncoding.DecodeString(secret.Ciphertext)
	if err != nil {
		return model.DataBackendCredentials{}, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return model.DataBackendCredentials{}, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return model.DataBackendCredentials{}, err
	}
	if len(raw) < gcm.NonceSize() {
		return model.DataBackendCredentials{}, ErrInvalidInput
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		return model.DataBackendCredentials{}, err
	}
	var credentials model.DataBackendCredentials
	if err := json.Unmarshal(plain, &credentials); err != nil {
		return model.DataBackendCredentials{}, err
	}
	return credentials, nil
}

func dataBackendSecretKey() ([]byte, string) {
	for _, envName := range []string{"FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "FUGUE_BUNDLE_SIGNING_KEY", "FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY"} {
		value := strings.TrimSpace(os.Getenv(envName))
		if value == "" {
			continue
		}
		if decoded, err := base64.StdEncoding.DecodeString(value); err == nil && len(decoded) >= 32 {
			sum := sha256.Sum256(decoded)
			return sum[:], envName
		}
		sum := sha256.Sum256([]byte(value))
		return sum[:], envName
	}
	sum := sha256.Sum256([]byte("fugue-local-development-data-credential-key"))
	return sum[:], "development-fallback"
}

func redactDataGrant(grant model.DataGrant) model.DataGrant {
	grant.TokenHash = ""
	return grant
}

type sqlRowScanner interface {
	Scan(dest ...any) error
}

func scanDataBackend(scanner sqlRowScanner) (model.DataBackend, error) {
	var backend model.DataBackend
	var capabilitiesRaw, credentialsRaw []byte
	if err := scanner.Scan(&backend.ID, &backend.TenantID, &backend.Name, &backend.Slug, &backend.Provider, &backend.Bucket, &backend.Region, &backend.Endpoint, &backend.BaseURL, &backend.Prefix, &backend.Status, &capabilitiesRaw, &credentialsRaw, &backend.CreatedAt, &backend.UpdatedAt); err != nil {
		return model.DataBackend{}, mapDBErr(err)
	}
	capabilities, err := decodeJSONValue[model.DataBackendCapabilities](capabilitiesRaw)
	if err != nil {
		return model.DataBackend{}, err
	}
	credentials, err := decodeJSONValue[model.DataBackendCredentials](credentialsRaw)
	if err != nil {
		return model.DataBackend{}, err
	}
	backend.Capabilities = capabilities
	backend.Credentials = credentials
	return normalizeDataBackend(backend), nil
}

func scanDataWorkspace(scanner sqlRowScanner) (model.DataWorkspace, error) {
	var workspace model.DataWorkspace
	var assetsRaw []byte
	if err := scanner.Scan(&workspace.ID, &workspace.TenantID, &workspace.ProjectID, &workspace.Name, &workspace.Slug, &workspace.DefaultRegion, &workspace.StorageBackendID, &workspace.QuotaBytes, &workspace.UsedBytes, &assetsRaw, &workspace.CreatedAt, &workspace.UpdatedAt); err != nil {
		return model.DataWorkspace{}, mapDBErr(err)
	}
	assets, err := decodeJSONValue[[]model.DataAsset](assetsRaw)
	if err != nil {
		return model.DataWorkspace{}, err
	}
	workspace.Assets = assets
	return normalizeDataWorkspace(workspace), nil
}

func scanDataSnapshot(scanner sqlRowScanner) (model.DataSnapshot, error) {
	var snapshot model.DataSnapshot
	var manifestRaw []byte
	if err := scanner.Scan(&snapshot.ID, &snapshot.TenantID, &snapshot.ProjectID, &snapshot.WorkspaceID, &snapshot.Version, &snapshot.Message, &snapshot.ManifestDigest, &manifestRaw, &snapshot.AssetCount, &snapshot.FileCount, &snapshot.TotalBytes, &snapshot.CreatedBy, &snapshot.CreatedAt, &snapshot.DeletedAt); err != nil {
		return model.DataSnapshot{}, mapDBErr(err)
	}
	manifest, err := decodeJSONValue[model.DataManifest](manifestRaw)
	if err != nil {
		return model.DataSnapshot{}, err
	}
	snapshot.Manifest = model.NormalizeDataManifest(manifest)
	return snapshot, nil
}

func scanDataTransfer(scanner sqlRowScanner) (model.DataTransfer, error) {
	var transfer model.DataTransfer
	var manifestRaw, planBlobsRaw []byte
	if err := scanner.Scan(&transfer.ID, &transfer.TenantID, &transfer.WorkspaceID, &transfer.SnapshotID, &transfer.Version, &transfer.Message, &transfer.Direction, &transfer.Status, &transfer.Source, &transfer.Target, &manifestRaw, &planBlobsRaw, &transfer.PartSize, &transfer.ExpiresAt, &transfer.BytesTotal, &transfer.BytesDone, &transfer.FilesTotal, &transfer.FilesDone, &transfer.ErrorCode, &transfer.ErrorMessage, &transfer.CreatedAt, &transfer.UpdatedAt, &transfer.StartedAt, &transfer.FinishedAt); err != nil {
		return model.DataTransfer{}, mapDBErr(err)
	}
	manifest, err := decodeJSONValue[model.DataManifest](manifestRaw)
	if err != nil {
		return model.DataTransfer{}, err
	}
	transfer.Manifest = model.NormalizeDataManifest(manifest)
	planBlobs, err := decodeJSONValue[[]model.DataTransferPlanBlob](planBlobsRaw)
	if err != nil {
		return model.DataTransfer{}, err
	}
	transfer.PlanBlobs = planBlobs
	return transfer, nil
}

func scanDataGrant(scanner sqlRowScanner) (model.DataGrant, error) {
	var grant model.DataGrant
	if err := scanner.Scan(&grant.ID, &grant.TenantID, &grant.WorkspaceID, &grant.SnapshotID, &grant.AssetName, &grant.Mode, &grant.Status, &grant.TokenPrefix, &grant.TokenHash, &grant.CreatedBy, &grant.CreatedAt, &grant.ExpiresAt, &grant.RevokedAt, &grant.LastUsedAt); err != nil {
		return model.DataGrant{}, mapDBErr(err)
	}
	return grant, nil
}

func (s *Store) pgListDataBackends(tenantID string, platformAdmin bool) ([]model.DataBackend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := `SELECT id, COALESCE(tenant_id, ''), name, slug, provider, bucket, region, endpoint, base_url, prefix, status, capabilities_json, credentials_json, created_at, updated_at FROM fugue_data_backends`
	args := []any{}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` WHERE tenant_id IS NULL OR tenant_id = $1`
	}
	query += ` ORDER BY name ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	backends := []model.DataBackend{}
	for rows.Next() {
		backend, err := scanDataBackend(rows)
		if err != nil {
			return nil, err
		}
		backends = append(backends, model.RedactDataBackendCredentials(backend))
	}
	return backends, mapDBErr(rows.Err())
}

func (s *Store) pgGetDataBackend(idOrName, tenantID string, platformAdmin bool, redact bool) (model.DataBackend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	slug := model.Slugify(idOrName)
	query := `SELECT id, COALESCE(tenant_id, ''), name, slug, provider, bucket, region, endpoint, base_url, prefix, status, capabilities_json, credentials_json, created_at, updated_at FROM fugue_data_backends WHERE (id = $1 OR name = $1 OR slug = $2)`
	args := []any{idOrName, slug}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND (tenant_id IS NULL OR tenant_id = $3)`
	}
	backend, err := scanDataBackend(s.db.QueryRowContext(ctx, query, args...))
	if err != nil {
		return model.DataBackend{}, err
	}
	if redact {
		backend = model.RedactDataBackendCredentials(backend)
	}
	return backend, nil
}

func (s *Store) pgCreateDataBackend(backend model.DataBackend) (model.DataBackend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backend = normalizeDataBackend(backend)
	credentials := backend.Credentials
	backend.Credentials = redactAccessKeyOnly(credentials)
	if backend.ID == "" {
		backend.ID = model.NewID("data_backend")
	}
	now := time.Now().UTC()
	if backend.CreatedAt.IsZero() {
		backend.CreatedAt = now
	}
	backend.UpdatedAt = now
	capabilitiesJSON, err := marshalJSON(backend.Capabilities)
	if err != nil {
		return model.DataBackend{}, err
	}
	credentialsJSON, err := marshalJSON(backend.Credentials)
	if err != nil {
		return model.DataBackend{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.DataBackend{}, err
	}
	defer tx.Rollback()
	created, err := scanDataBackend(tx.QueryRowContext(ctx, `
INSERT INTO fugue_data_backends (id, tenant_id, name, slug, provider, bucket, region, endpoint, base_url, prefix, status, capabilities_json, credentials_json, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
RETURNING id, COALESCE(tenant_id, ''), name, slug, provider, bucket, region, endpoint, base_url, prefix, status, capabilities_json, credentials_json, created_at, updated_at
`, backend.ID, nullIfEmpty(backend.TenantID), backend.Name, backend.Slug, backend.Provider, backend.Bucket, backend.Region, backend.Endpoint, backend.BaseURL, backend.Prefix, backend.Status, capabilitiesJSON, credentialsJSON, backend.CreatedAt, backend.UpdatedAt))
	if err != nil {
		return model.DataBackend{}, mapDBErr(err)
	}
	if dataBackendCredentialsPresent(credentials) {
		secret, err := encryptDataBackendSecret(model.DataBackendSecret{
			ID:        model.NewID("data_backend_secret"),
			TenantID:  backend.TenantID,
			BackendID: backend.ID,
		}, credentials)
		if err != nil {
			return model.DataBackend{}, err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_data_backend_secrets (id, tenant_id, backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`, secret.ID, nullIfEmpty(secret.TenantID), secret.BackendID, secret.Ciphertext, secret.KeyID, secret.CreatedAt, secret.UpdatedAt, secret.LastRotated); err != nil {
			return model.DataBackend{}, mapDBErr(err)
		}
		created.CredentialSecretID = secret.ID
	}
	if err := tx.Commit(); err != nil {
		return model.DataBackend{}, err
	}
	return model.RedactDataBackendCredentials(created), nil
}

func (s *Store) pgGetDataBackendForUse(idOrName, tenantID string, platformAdmin bool) (model.DataBackend, error) {
	backend, err := s.pgGetDataBackend(idOrName, tenantID, platformAdmin, false)
	if err != nil {
		return model.DataBackend{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var secret model.DataBackendSecret
	err = s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at
FROM fugue_data_backend_secrets WHERE backend_id = $1
`, backend.ID).Scan(&secret.ID, &secret.TenantID, &secret.BackendID, &secret.Ciphertext, &secret.KeyID, &secret.CreatedAt, &secret.UpdatedAt, &secret.LastRotated)
	if err == nil {
		credentials, decryptErr := decryptDataBackendSecret(secret)
		if decryptErr != nil {
			return model.DataBackend{}, decryptErr
		}
		backend.CredentialSecretID = secret.ID
		backend.Credentials = credentials
		return backend, nil
	}
	if errors.Is(mapDBErr(err), ErrNotFound) {
		return backend, nil
	}
	return model.DataBackend{}, mapDBErr(err)
}

func (s *Store) pgSeedDefaultDataBackendFromEnv() error {
	backend, credentials, ok := defaultDataBackendConfigFromEnv()
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	capabilitiesJSON, err := marshalJSON(backend.Capabilities)
	if err != nil {
		return err
	}
	credentialsJSON, err := marshalJSON(backend.Credentials)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_data_backends (id, tenant_id, name, slug, provider, bucket, region, endpoint, base_url, prefix, status, capabilities_json, credentials_json, created_at, updated_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13)
ON CONFLICT (id) DO UPDATE SET
	name = EXCLUDED.name,
	slug = EXCLUDED.slug,
	provider = EXCLUDED.provider,
	bucket = EXCLUDED.bucket,
	region = EXCLUDED.region,
	endpoint = EXCLUDED.endpoint,
	base_url = EXCLUDED.base_url,
	prefix = EXCLUDED.prefix,
	status = EXCLUDED.status,
	capabilities_json = EXCLUDED.capabilities_json,
	credentials_json = EXCLUDED.credentials_json,
	updated_at = EXCLUDED.updated_at
`, backend.ID, backend.Name, backend.Slug, backend.Provider, backend.Bucket, backend.Region, backend.Endpoint, backend.BaseURL, backend.Prefix, backend.Status, capabilitiesJSON, credentialsJSON, backend.UpdatedAt); err != nil {
		return mapDBErr(err)
	}
	if dataBackendCredentialsPresent(credentials) {
		existing := model.DataBackendSecret{}
		err := tx.QueryRowContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at
FROM fugue_data_backend_secrets WHERE backend_id = $1
`, backend.ID).Scan(&existing.ID, &existing.TenantID, &existing.BackendID, &existing.Ciphertext, &existing.KeyID, &existing.CreatedAt, &existing.UpdatedAt, &existing.LastRotated)
		if err != nil && !errors.Is(mapDBErr(err), ErrNotFound) {
			return mapDBErr(err)
		}
		if existing.ID == "" {
			existing = model.DataBackendSecret{ID: model.NewID("data_backend_secret"), BackendID: backend.ID}
		}
		secret, err := encryptDataBackendSecret(existing, credentials)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_data_backend_secrets (id, tenant_id, backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, $7)
ON CONFLICT (backend_id) DO UPDATE SET
	ciphertext = EXCLUDED.ciphertext,
	key_id = EXCLUDED.key_id,
	updated_at = EXCLUDED.updated_at,
	last_rotated_at = EXCLUDED.last_rotated_at
`, secret.ID, secret.BackendID, secret.Ciphertext, secret.KeyID, secret.CreatedAt, secret.UpdatedAt, secret.LastRotated); err != nil {
			return mapDBErr(err)
		}
	}
	return tx.Commit()
}

func (s *Store) pgDeleteDataBackend(idOrName, tenantID string, platformAdmin bool) (model.DataBackend, error) {
	backend, err := s.pgGetDataBackend(idOrName, tenantID, platformAdmin, false)
	if err != nil {
		return model.DataBackend{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var inUse bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_data_workspaces WHERE storage_backend_id = $1)`, backend.ID).Scan(&inUse); err != nil {
		return model.DataBackend{}, mapDBErr(err)
	}
	if inUse {
		return model.DataBackend{}, ErrConflict
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM fugue_data_backends WHERE id = $1`, backend.ID); err != nil {
		return model.DataBackend{}, mapDBErr(err)
	}
	return model.RedactDataBackendCredentials(backend), nil
}

func (s *Store) pgRotateDataBackendCredentials(idOrName, tenantID string, platformAdmin bool, credentials model.DataBackendCredentials) (model.DataBackend, error) {
	backend, err := s.pgGetDataBackend(idOrName, tenantID, platformAdmin, false)
	if err != nil {
		return model.DataBackend{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.DataBackend{}, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	redacted := redactAccessKeyOnly(credentials)
	credentialsJSON, err := marshalJSON(redacted)
	if err != nil {
		return model.DataBackend{}, err
	}
	rotated, err := scanDataBackend(tx.QueryRowContext(ctx, `
UPDATE fugue_data_backends SET credentials_json = $2, updated_at = $3 WHERE id = $1
RETURNING id, COALESCE(tenant_id, ''), name, slug, provider, bucket, region, endpoint, base_url, prefix, status, capabilities_json, credentials_json, created_at, updated_at
`, backend.ID, credentialsJSON, now))
	if err != nil {
		return model.DataBackend{}, mapDBErr(err)
	}
	var existing model.DataBackendSecret
	err = tx.QueryRowContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at
FROM fugue_data_backend_secrets WHERE backend_id = $1
`, backend.ID).Scan(&existing.ID, &existing.TenantID, &existing.BackendID, &existing.Ciphertext, &existing.KeyID, &existing.CreatedAt, &existing.UpdatedAt, &existing.LastRotated)
	if err != nil && !errors.Is(mapDBErr(err), ErrNotFound) {
		return model.DataBackend{}, mapDBErr(err)
	}
	if errors.Is(mapDBErr(err), ErrNotFound) {
		existing = model.DataBackendSecret{
			ID:        model.NewID("data_backend_secret"),
			TenantID:  backend.TenantID,
			BackendID: backend.ID,
			CreatedAt: now,
		}
	}
	existing.LastRotated = now
	secret, err := encryptDataBackendSecret(existing, credentials)
	if err != nil {
		return model.DataBackend{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_data_backend_secrets (id, tenant_id, backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (backend_id) DO UPDATE SET ciphertext = EXCLUDED.ciphertext, key_id = EXCLUDED.key_id, updated_at = EXCLUDED.updated_at, last_rotated_at = EXCLUDED.last_rotated_at
`, secret.ID, nullIfEmpty(secret.TenantID), secret.BackendID, secret.Ciphertext, secret.KeyID, secret.CreatedAt, secret.UpdatedAt, secret.LastRotated); err != nil {
		return model.DataBackend{}, mapDBErr(err)
	}
	rotated.CredentialSecretID = secret.ID
	if err := tx.Commit(); err != nil {
		return model.DataBackend{}, err
	}
	return model.RedactDataBackendCredentials(rotated), nil
}

func (s *Store) pgListDataWorkspaces(tenantID, projectID string, platformAdmin bool) ([]model.DataWorkspace, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := `SELECT id, COALESCE(tenant_id, ''), project_id, name, slug, default_region, storage_backend_id, quota_bytes, used_bytes, assets_json, created_at, updated_at FROM fugue_data_workspaces`
	args := []any{}
	conds := []string{}
	if !platformAdmin {
		args = append(args, tenantID)
		conds = append(conds, fmt.Sprintf(`tenant_id = $%d`, len(args)))
	}
	if projectID != "" {
		args = append(args, projectID)
		conds = append(conds, fmt.Sprintf(`project_id = $%d`, len(args)))
	}
	if len(conds) > 0 {
		query += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	query += ` ORDER BY updated_at DESC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	workspaces := []model.DataWorkspace{}
	for rows.Next() {
		workspace, err := scanDataWorkspace(rows)
		if err != nil {
			return nil, err
		}
		workspaces = append(workspaces, workspace)
	}
	return workspaces, mapDBErr(rows.Err())
}

func (s *Store) pgGetDataWorkspace(idOrName, tenantID string, platformAdmin bool) (model.DataWorkspace, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	slug := model.Slugify(idOrName)
	query := `SELECT id, COALESCE(tenant_id, ''), project_id, name, slug, default_region, storage_backend_id, quota_bytes, used_bytes, assets_json, created_at, updated_at FROM fugue_data_workspaces WHERE (id = $1 OR name = $1 OR slug = $2)`
	args := []any{idOrName, slug}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND tenant_id = $3`
	}
	return scanDataWorkspace(s.db.QueryRowContext(ctx, query, args...))
}

func (s *Store) pgCreateDataWorkspace(workspace model.DataWorkspace) (model.DataWorkspace, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	workspace = normalizeDataWorkspace(workspace)
	if workspace.ID == "" {
		workspace.ID = model.NewID("data_ws")
	}
	now := time.Now().UTC()
	if workspace.CreatedAt.IsZero() {
		workspace.CreatedAt = now
	}
	workspace.UpdatedAt = now
	assetsJSON, err := marshalJSON(workspace.Assets)
	if err != nil {
		return model.DataWorkspace{}, err
	}
	created, err := scanDataWorkspace(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_data_workspaces (id, tenant_id, project_id, name, slug, default_region, storage_backend_id, quota_bytes, used_bytes, assets_json, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, $12), $8, $9, $10, $11, $11)
RETURNING id, COALESCE(tenant_id, ''), project_id, name, slug, default_region, storage_backend_id, quota_bytes, used_bytes, assets_json, created_at, updated_at
`, workspace.ID, nullIfEmpty(workspace.TenantID), workspace.ProjectID, workspace.Name, workspace.Slug, workspace.DefaultRegion, nullIfEmpty(workspace.StorageBackendID), workspace.QuotaBytes, workspace.UsedBytes, assetsJSON, workspace.CreatedAt, defaultDataBackendID))
	if err != nil {
		return model.DataWorkspace{}, mapDBErr(err)
	}
	return created, nil
}

func (s *Store) pgUpdateDataWorkspace(idOrName, tenantID string, platformAdmin bool, update DataWorkspaceUpdate) (model.DataWorkspace, error) {
	current, err := s.pgGetDataWorkspace(idOrName, tenantID, platformAdmin)
	if err != nil {
		return model.DataWorkspace{}, err
	}
	if update.Name != nil {
		current.Name = strings.TrimSpace(*update.Name)
		current.Slug = model.Slugify(current.Name)
	}
	if update.ProjectID != nil {
		current.ProjectID = strings.TrimSpace(*update.ProjectID)
	}
	if update.DefaultRegion != nil {
		current.DefaultRegion = strings.TrimSpace(*update.DefaultRegion)
	}
	if update.StorageBackendID != nil {
		current.StorageBackendID = strings.TrimSpace(*update.StorageBackendID)
		if current.StorageBackendID == "" {
			current.StorageBackendID = defaultDataBackendID
		}
	}
	if update.Assets != nil {
		current.Assets = normalizeDataAssets(*update.Assets)
	}
	if update.QuotaBytes != nil {
		current.QuotaBytes = *update.QuotaBytes
	}
	current.UpdatedAt = time.Now().UTC()
	assetsJSON, err := marshalJSON(current.Assets)
	if err != nil {
		return model.DataWorkspace{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return scanDataWorkspace(s.db.QueryRowContext(ctx, `
UPDATE fugue_data_workspaces
SET project_id = $2, name = $3, slug = $4, default_region = $5, storage_backend_id = $6, quota_bytes = $7, assets_json = $8, updated_at = $9
WHERE id = $1
RETURNING id, COALESCE(tenant_id, ''), project_id, name, slug, default_region, storage_backend_id, quota_bytes, used_bytes, assets_json, created_at, updated_at
`, current.ID, current.ProjectID, current.Name, current.Slug, current.DefaultRegion, current.StorageBackendID, current.QuotaBytes, assetsJSON, current.UpdatedAt))
}

func (s *Store) pgDeleteDataWorkspace(idOrName, tenantID string, platformAdmin bool) (model.DataWorkspace, error) {
	current, err := s.pgGetDataWorkspace(idOrName, tenantID, platformAdmin)
	if err != nil {
		return model.DataWorkspace{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, `DELETE FROM fugue_data_workspaces WHERE id = $1`, current.ID); err != nil {
		return model.DataWorkspace{}, mapDBErr(err)
	}
	return current, nil
}

func (s *Store) pgListDataSnapshots(workspaceID string, includeDeleted bool) ([]model.DataSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := `SELECT id, COALESCE(tenant_id, ''), project_id, workspace_id, version, message, manifest_digest, manifest_json, asset_count, file_count, total_bytes, created_by, created_at, deleted_at FROM fugue_data_snapshots WHERE workspace_id = $1`
	if !includeDeleted {
		query += ` AND deleted_at IS NULL`
	}
	query += ` ORDER BY created_at DESC`
	rows, err := s.db.QueryContext(ctx, query, workspaceID)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	snapshots := []model.DataSnapshot{}
	for rows.Next() {
		snapshot, err := scanDataSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, mapDBErr(rows.Err())
}

func (s *Store) pgGetDataSnapshot(workspaceID, idOrVersion string) (model.DataSnapshot, error) {
	if strings.EqualFold(strings.TrimSpace(idOrVersion), "latest") {
		snapshots, err := s.pgListDataSnapshots(workspaceID, false)
		if err != nil {
			return model.DataSnapshot{}, err
		}
		if len(snapshots) == 0 {
			return model.DataSnapshot{}, ErrNotFound
		}
		return snapshots[0], nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return scanDataSnapshot(s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), project_id, workspace_id, version, message, manifest_digest, manifest_json, asset_count, file_count, total_bytes, created_by, created_at, deleted_at
FROM fugue_data_snapshots
WHERE workspace_id = $1 AND (id = $2 OR version = $2) AND deleted_at IS NULL
`, workspaceID, idOrVersion))
}

func (s *Store) pgCreateDataSnapshot(snapshot model.DataSnapshot) (model.DataSnapshot, error) {
	workspace, err := s.pgGetDataWorkspace(snapshot.WorkspaceID, "", true)
	if err != nil {
		return model.DataSnapshot{}, err
	}
	snapshot.Manifest = model.NormalizeDataManifest(snapshot.Manifest)
	snapshot.Version = strings.TrimSpace(snapshot.Version)
	if snapshot.Version == "" {
		snapshot.Version = defaultDataSnapshotVersion(time.Now().UTC())
	}
	if strings.EqualFold(snapshot.Version, "latest") {
		return model.DataSnapshot{}, ErrInvalidInput
	}
	if snapshot.ID == "" {
		snapshot.ID = model.NewID("data_snap")
	}
	now := time.Now().UTC()
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = now
	}
	snapshot.TenantID = workspace.TenantID
	snapshot.ProjectID = workspace.ProjectID
	snapshot.Manifest.WorkspaceID = snapshot.WorkspaceID
	snapshot.Manifest.SnapshotID = snapshot.ID
	if snapshot.ManifestDigest == "" {
		snapshot.ManifestDigest = digestManifest(snapshot.Manifest)
	}
	snapshot.Manifest.Digest = snapshot.ManifestDigest
	snapshot.FileCount = snapshot.Manifest.FileCount
	snapshot.TotalBytes = snapshot.Manifest.TotalBytes
	snapshot.AssetCount = countManifestAssets(snapshot.Manifest)
	manifestJSON, err := marshalJSON(snapshot.Manifest)
	if err != nil {
		return model.DataSnapshot{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.DataSnapshot{}, err
	}
	defer tx.Rollback()
	created, err := scanDataSnapshot(tx.QueryRowContext(ctx, `
INSERT INTO fugue_data_snapshots (id, tenant_id, project_id, workspace_id, version, message, manifest_digest, manifest_json, asset_count, file_count, total_bytes, created_by, created_at, deleted_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NULL)
RETURNING id, COALESCE(tenant_id, ''), project_id, workspace_id, version, message, manifest_digest, manifest_json, asset_count, file_count, total_bytes, created_by, created_at, deleted_at
`, snapshot.ID, nullIfEmpty(snapshot.TenantID), snapshot.ProjectID, snapshot.WorkspaceID, snapshot.Version, snapshot.Message, snapshot.ManifestDigest, manifestJSON, snapshot.AssetCount, snapshot.FileCount, snapshot.TotalBytes, snapshot.CreatedBy, snapshot.CreatedAt))
	if err != nil {
		return model.DataSnapshot{}, mapDBErr(err)
	}
	if _, err := tx.ExecContext(ctx, `
WITH blobs AS (
	SELECT DISTINCT entry->>'sha256' AS sha256, (entry->>'size')::BIGINT AS size_bytes
	FROM fugue_data_snapshots, jsonb_array_elements(manifest_json->'entries') AS entry
	WHERE workspace_id = $1
	  AND deleted_at IS NULL
	  AND entry->>'kind' = 'file'
	  AND COALESCE(entry->>'sha256', '') <> ''
)
UPDATE fugue_data_workspaces
SET used_bytes = COALESCE((SELECT SUM(size_bytes) FROM blobs), 0), updated_at = $2
WHERE id = $1
`, snapshot.WorkspaceID, now); err != nil {
		return model.DataSnapshot{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.DataSnapshot{}, err
	}
	return created, nil
}

func (s *Store) pgDeleteDataSnapshot(workspaceID, idOrVersion string) (model.DataSnapshot, error) {
	snapshot, err := s.pgGetDataSnapshot(workspaceID, idOrVersion)
	if err != nil {
		return model.DataSnapshot{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.DataSnapshot{}, err
	}
	defer tx.Rollback()
	deleted, err := scanDataSnapshot(tx.QueryRowContext(ctx, `
UPDATE fugue_data_snapshots SET deleted_at = $3
WHERE workspace_id = $1 AND id = $2
RETURNING id, COALESCE(tenant_id, ''), project_id, workspace_id, version, message, manifest_digest, manifest_json, asset_count, file_count, total_bytes, created_by, created_at, deleted_at
`, workspaceID, snapshot.ID, now))
	if err != nil {
		return model.DataSnapshot{}, mapDBErr(err)
	}
	if _, err := tx.ExecContext(ctx, `
WITH blobs AS (
	SELECT DISTINCT entry->>'sha256' AS sha256, (entry->>'size')::BIGINT AS size_bytes
	FROM fugue_data_snapshots, jsonb_array_elements(manifest_json->'entries') AS entry
	WHERE workspace_id = $1
	  AND deleted_at IS NULL
	  AND entry->>'kind' = 'file'
	  AND COALESCE(entry->>'sha256', '') <> ''
)
UPDATE fugue_data_workspaces
SET used_bytes = COALESCE((SELECT SUM(size_bytes) FROM blobs), 0), updated_at = $2
WHERE id = $1
`, workspaceID, now); err != nil {
		return model.DataSnapshot{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.DataSnapshot{}, err
	}
	return deleted, nil
}

func (s *Store) pgCreateDataTransfer(transfer model.DataTransfer) (model.DataTransfer, error) {
	workspace, err := s.pgGetDataWorkspace(transfer.WorkspaceID, "", true)
	if err != nil {
		return model.DataTransfer{}, err
	}
	transfer.Manifest = model.NormalizeDataManifest(transfer.Manifest)
	if transfer.ID == "" {
		transfer.ID = model.NewID("data_transfer")
	}
	now := time.Now().UTC()
	if transfer.CreatedAt.IsZero() {
		transfer.CreatedAt = now
	}
	transfer.UpdatedAt = now
	if transfer.Status == "" {
		transfer.Status = model.DataTransferStatusPlanned
	}
	transfer.TenantID = workspace.TenantID
	if transfer.PartSize == 0 {
		transfer.PartSize = defaultDataMultipartPartSize
	}
	manifestJSON, err := marshalJSON(transfer.Manifest)
	if err != nil {
		return model.DataTransfer{}, err
	}
	planBlobsJSON, err := marshalJSON(transfer.PlanBlobs)
	if err != nil {
		return model.DataTransfer{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return scanDataTransfer(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_data_transfers (id, tenant_id, workspace_id, snapshot_id, version, message, direction, status, source, target, manifest_json, plan_blobs_json, part_size, expires_at, bytes_total, bytes_done, files_total, files_done, error_code, error_message, created_at, updated_at, started_at, finished_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
RETURNING id, COALESCE(tenant_id, ''), workspace_id, snapshot_id, version, message, direction, status, source, target, manifest_json, plan_blobs_json, part_size, expires_at, bytes_total, bytes_done, files_total, files_done, error_code, error_message, created_at, updated_at, started_at, finished_at
`, transfer.ID, nullIfEmpty(transfer.TenantID), transfer.WorkspaceID, transfer.SnapshotID, transfer.Version, transfer.Message, transfer.Direction, transfer.Status, transfer.Source, transfer.Target, manifestJSON, planBlobsJSON, transfer.PartSize, transfer.ExpiresAt, transfer.BytesTotal, transfer.BytesDone, transfer.FilesTotal, transfer.FilesDone, transfer.ErrorCode, transfer.ErrorMessage, transfer.CreatedAt, transfer.UpdatedAt, transfer.StartedAt, transfer.FinishedAt))
}

func (s *Store) pgUpdateDataTransfer(transfer model.DataTransfer) (model.DataTransfer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transfer.UpdatedAt = time.Now().UTC()
	transfer.Manifest = model.NormalizeDataManifest(transfer.Manifest)
	manifestJSON, err := marshalJSON(transfer.Manifest)
	if err != nil {
		return model.DataTransfer{}, err
	}
	planBlobsJSON, err := marshalJSON(transfer.PlanBlobs)
	if err != nil {
		return model.DataTransfer{}, err
	}
	return scanDataTransfer(s.db.QueryRowContext(ctx, `
UPDATE fugue_data_transfers
SET snapshot_id = $2, version = $3, message = $4, status = $5, manifest_json = $6, plan_blobs_json = $7, part_size = $8, expires_at = $9, bytes_total = $10, bytes_done = $11, files_total = $12, files_done = $13, error_code = $14, error_message = $15, updated_at = $16, started_at = $17, finished_at = $18
WHERE id = $1
RETURNING id, COALESCE(tenant_id, ''), workspace_id, snapshot_id, version, message, direction, status, source, target, manifest_json, plan_blobs_json, part_size, expires_at, bytes_total, bytes_done, files_total, files_done, error_code, error_message, created_at, updated_at, started_at, finished_at
`, transfer.ID, transfer.SnapshotID, transfer.Version, transfer.Message, transfer.Status, manifestJSON, planBlobsJSON, transfer.PartSize, transfer.ExpiresAt, transfer.BytesTotal, transfer.BytesDone, transfer.FilesTotal, transfer.FilesDone, transfer.ErrorCode, transfer.ErrorMessage, transfer.UpdatedAt, transfer.StartedAt, transfer.FinishedAt))
}

func (s *Store) pgGetDataTransfer(id string) (model.DataTransfer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return scanDataTransfer(s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), workspace_id, snapshot_id, version, message, direction, status, source, target, manifest_json, plan_blobs_json, part_size, expires_at, bytes_total, bytes_done, files_total, files_done, error_code, error_message, created_at, updated_at, started_at, finished_at
FROM fugue_data_transfers WHERE id = $1
`, id))
}

func (s *Store) pgListDataTransfers(tenantID, workspaceID string, platformAdmin bool) ([]model.DataTransfer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := `SELECT id, COALESCE(tenant_id, ''), workspace_id, snapshot_id, version, message, direction, status, source, target, manifest_json, plan_blobs_json, part_size, expires_at, bytes_total, bytes_done, files_total, files_done, error_code, error_message, created_at, updated_at, started_at, finished_at FROM fugue_data_transfers`
	args := []any{}
	conds := []string{}
	if !platformAdmin {
		args = append(args, tenantID)
		conds = append(conds, fmt.Sprintf(`tenant_id = $%d`, len(args)))
	}
	if workspaceID != "" {
		args = append(args, workspaceID)
		conds = append(conds, fmt.Sprintf(`workspace_id = $%d`, len(args)))
	}
	if len(conds) > 0 {
		query += ` WHERE ` + strings.Join(conds, " AND ")
	}
	query += ` ORDER BY updated_at DESC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	transfers := []model.DataTransfer{}
	for rows.Next() {
		transfer, err := scanDataTransfer(rows)
		if err != nil {
			return nil, err
		}
		transfers = append(transfers, transfer)
	}
	return transfers, mapDBErr(rows.Err())
}

func (s *Store) pgCreateDataGrant(grant model.DataGrant) (model.DataGrant, error) {
	workspace, err := s.pgGetDataWorkspace(grant.WorkspaceID, "", true)
	if err != nil {
		return model.DataGrant{}, err
	}
	if grant.ID == "" {
		grant.ID = model.NewID("data_grant")
	}
	if grant.Status == "" {
		grant.Status = model.DataGrantStatusActive
	}
	if grant.CreatedAt.IsZero() {
		grant.CreatedAt = time.Now().UTC()
	}
	grant.TenantID = workspace.TenantID
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	created, err := scanDataGrant(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_data_grants (id, tenant_id, workspace_id, snapshot_id, asset_name, mode, status, token_prefix, token_hash, created_by, created_at, expires_at, revoked_at, last_used_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
RETURNING id, COALESCE(tenant_id, ''), workspace_id, snapshot_id, asset_name, mode, status, token_prefix, token_hash, created_by, created_at, expires_at, revoked_at, last_used_at
`, grant.ID, nullIfEmpty(grant.TenantID), grant.WorkspaceID, grant.SnapshotID, grant.AssetName, grant.Mode, grant.Status, grant.TokenPrefix, grant.TokenHash, grant.CreatedBy, grant.CreatedAt, grant.ExpiresAt, grant.RevokedAt, grant.LastUsedAt))
	if err != nil {
		return model.DataGrant{}, mapDBErr(err)
	}
	return redactDataGrant(created), nil
}

func (s *Store) pgListDataGrants(workspaceID string) ([]model.DataGrant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), workspace_id, snapshot_id, asset_name, mode, status, token_prefix, token_hash, created_by, created_at, expires_at, revoked_at, last_used_at
FROM fugue_data_grants WHERE workspace_id = $1 ORDER BY created_at DESC
`, workspaceID)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	grants := []model.DataGrant{}
	for rows.Next() {
		grant, err := scanDataGrant(rows)
		if err != nil {
			return nil, err
		}
		grants = append(grants, redactDataGrant(grant))
	}
	return grants, mapDBErr(rows.Err())
}

func (s *Store) pgGetDataGrant(id string) (model.DataGrant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grant, err := scanDataGrant(s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), workspace_id, snapshot_id, asset_name, mode, status, token_prefix, token_hash, created_by, created_at, expires_at, revoked_at, last_used_at
FROM fugue_data_grants WHERE id = $1
`, id))
	if err != nil {
		return model.DataGrant{}, mapDBErr(err)
	}
	return redactDataGrant(grant), nil
}

func (s *Store) pgRevokeDataGrant(id string) (model.DataGrant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now().UTC()
	grant, err := scanDataGrant(s.db.QueryRowContext(ctx, `
UPDATE fugue_data_grants SET status = $2, revoked_at = $3 WHERE id = $1
RETURNING id, COALESCE(tenant_id, ''), workspace_id, snapshot_id, asset_name, mode, status, token_prefix, token_hash, created_by, created_at, expires_at, revoked_at, last_used_at
`, id, model.DataGrantStatusRevoked, now))
	if err != nil {
		return model.DataGrant{}, mapDBErr(err)
	}
	return redactDataGrant(grant), nil
}

func (s *Store) pgAuthenticateDataGrant(tokenHash string) (model.DataGrant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.DataGrant{}, err
	}
	defer tx.Rollback()
	grant, err := scanDataGrant(tx.QueryRowContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), workspace_id, snapshot_id, asset_name, mode, status, token_prefix, token_hash, created_by, created_at, expires_at, revoked_at, last_used_at
FROM fugue_data_grants WHERE token_hash = $1
`, tokenHash))
	if err != nil {
		return model.DataGrant{}, err
	}
	if grant.Status != model.DataGrantStatusActive || (grant.ExpiresAt != nil && grant.ExpiresAt.Before(now)) {
		return model.DataGrant{}, ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `UPDATE fugue_data_grants SET last_used_at = $2 WHERE id = $1`, grant.ID, now); err != nil {
		return model.DataGrant{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.DataGrant{}, err
	}
	return redactDataGrant(grant), nil
}

func IsDataNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}
