package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"fugue/internal/storagerecovery"
	"sort"
	"strings"
	"sync"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/edgetopology"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/operationview/contract"
	"fugue/internal/pgimage"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/sshidentity"

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrConflict            = errors.New("conflict")
	ErrInvalidInput        = errors.New("invalid input")
	ErrIdempotencyMismatch = errors.New("idempotency key mismatch")
	ErrActiveDeployExists  = errors.New("active deploy exists")
	ErrAlreadyCurrent      = errors.New("already current")
)

type Store struct {
	qualityEvidenceMetrics qualityEvidenceCounters
	compilerDNSInputs      compilerDNSInputCache
	readMetrics            [2]storeReadCounters
	platformContentHintMu  sync.Mutex
	platformContentHints   map[string][32]byte

	databaseURL                          string
	dataBlobDir                          string
	db                                   *sql.DB
	dbInitMu                             sync.Mutex
	dbReady                              bool
	storagePolicy                        storagePolicyState
	retention                            observationRetentionState
	lifecycle                            storageLifecycleState
	compilerInputArchiveObjects          inputarchive.Objects
	compilerInputArchiveReadLeaseEnabled bool

	platformArtifactKeyringMu       sync.RWMutex
	platformArtifactKeyring         bundleauth.Keyring
	platformArtifactKeyringRevision uint64
	sshIdentityKeyringMu            sync.RWMutex
	sshIdentityKeyring              sshidentity.Keyring
	sshIdentityKeyFile              string
}

func (s *Store) ConfigurePlatformArtifactSigning(keyring bundleauth.Keyring) {
	if s == nil {
		return
	}
	s.platformArtifactKeyringMu.Lock()
	defer s.platformArtifactKeyringMu.Unlock()
	s.platformArtifactKeyring = bundleauth.NewKeyring(
		keyring.PrimaryKey,
		keyring.PrimaryKeyID,
		keyring.PreviousKey,
		keyring.PreviousKeyID,
		platformArtifactRevokedKeyIDs(keyring),
	)
	s.platformArtifactKeyringRevision++
}

func (s *Store) platformArtifactSigningKeyring() bundleauth.Keyring {
	if s == nil {
		return bundleauth.Keyring{}
	}
	s.platformArtifactKeyringMu.RLock()
	defer s.platformArtifactKeyringMu.RUnlock()
	keyring := s.platformArtifactKeyring
	return bundleauth.NewKeyring(
		keyring.PrimaryKey,
		keyring.PrimaryKeyID,
		keyring.PreviousKey,
		keyring.PreviousKeyID,
		platformArtifactRevokedKeyIDs(keyring),
	)
}

func platformArtifactRevokedKeyIDs(keyring bundleauth.Keyring) []string {
	values := make([]string, 0, len(keyring.RevokedKeyIDs))
	for keyID := range keyring.RevokedKeyIDs {
		values = append(values, keyID)
	}
	return values
}

type ProjectUpdate struct {
	Name                  *string
	Description           *string
	DefaultRuntimeID      *string
	ClearDefaultRuntimeID bool
}

func New(databaseURL string) *Store {
	return &Store{databaseURL: strings.TrimSpace(databaseURL)}
}

// ConfigureDataBlobDir sets the directory for local workspace blob payloads.
// Call it before serving requests; relational metadata remains in PostgreSQL.
func (s *Store) ConfigureDataBlobDir(dir string) {
	s.dataBlobDir = strings.TrimSpace(dir)
}

func (*Store) BackendKind() string {
	return "postgres"
}

// Close releases the PostgreSQL connection pool after the store stops serving.
func (s *Store) Close() error {
	s.dbInitMu.Lock()
	defer s.dbInitMu.Unlock()
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Init() error {
	if err := s.ensureDatabaseReady(); err != nil {
		return err
	}

	if err := s.ConfigureCompilerInputArchiveFromEnv(); err != nil {
		return err
	}
	if err := s.SeedDefaultDataBackendFromEnv(); err != nil {
		return err
	}
	if err := s.SeedDefaultBackupBackendFromEnv(); err != nil {
		return err
	}
	if err := s.EnsureDefaultBackupPolicy(); err != nil {
		return err
	}
	if err := s.repairBackupPolicySchedules(); err != nil {
		return err
	}
	if err := s.pgRepairAppStatuses(); err != nil {
		return err
	}
	if err := s.pgVerifyEdgeInstanceFencing(); err != nil {
		return err
	}
	return nil
}

// BootstrapSchema creates the baseline PostgreSQL schema before independent
// schema migrations. It does not start or mark the control plane as ready.
func (s *Store) BootstrapSchema() error {
	return s.ensureDatabaseReady()
}

func (s *Store) CheckReadiness(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
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
	return s.pgVerifyEdgeActivationReadiness(pingCtx)
}

func (s *Store) ListTenants() ([]model.Tenant, error) {
	return s.pgListTenants()
}

func (s *Store) GetTenant(id string) (model.Tenant, error) {
	return s.pgGetTenant(id)
}

func (s *Store) CreateTenant(name string) (model.Tenant, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return model.Tenant{}, ErrInvalidInput
	}
	return s.pgCreateTenant(name)
}

func (s *Store) DeleteTenant(id string) (model.Tenant, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Tenant{}, ErrInvalidInput
	}
	return s.pgDeleteTenant(id)
}

func (s *Store) ListProjects(tenantID string) ([]model.Project, error) {
	return s.pgListProjects(tenantID)
}

func (s *Store) ListAllProjects() ([]model.Project, error) {
	return s.pgListAllProjects()
}

func (s *Store) GetProject(id string) (model.Project, error) {
	return s.pgGetProject(id)
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
	return s.pgUpdateProjectFields(id, update)
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
	return s.pgCreateProject(tenantID, name, description, runtimeID)
}

func (s *Store) MarkProjectDeleteRequested(id string) (model.Project, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Project{}, false, ErrInvalidInput
	}
	return s.pgMarkProjectDeleteRequested(id)
}

func (s *Store) DeleteProject(id string) (model.Project, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Project{}, ErrInvalidInput
	}
	return s.pgDeleteProject(id)
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
	return s.pgEnsureDefaultProject(tenantID)
}

func (s *Store) ListAPIKeys(tenantID string, platformAdmin bool) ([]model.APIKey, error) {
	return s.pgListAPIKeys(tenantID, platformAdmin)
}

func (s *Store) GetAPIKey(id string) (model.APIKey, error) {
	return s.pgGetAPIKey(id)
}

func (s *Store) CreateAPIKey(tenantID, label string, scopes []string) (model.APIKey, string, error) {
	label = strings.TrimSpace(label)
	if label == "" {
		return model.APIKey{}, "", ErrInvalidInput
	}
	if len(scopes) == 0 {
		return model.APIKey{}, "", ErrInvalidInput
	}
	return s.pgCreateAPIKey(tenantID, label, scopes)
}

func (s *Store) UpdateAPIKey(id string, label *string, scopes *[]string) (model.APIKey, error) {
	id = strings.TrimSpace(id)
	if id == "" || (label == nil && scopes == nil) {
		return model.APIKey{}, ErrInvalidInput
	}
	return s.pgUpdateAPIKey(id, label, scopes)
}

func (s *Store) RotateAPIKey(id string, label *string, scopes *[]string) (model.APIKey, string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.APIKey{}, "", ErrInvalidInput
	}
	return s.pgRotateAPIKey(id, label, scopes)
}

func (s *Store) DisableAPIKey(id string) (model.APIKey, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.APIKey{}, ErrInvalidInput
	}
	return s.pgDisableAPIKey(id)
}

func (s *Store) EnableAPIKey(id string) (model.APIKey, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.APIKey{}, ErrInvalidInput
	}
	return s.pgEnableAPIKey(id)
}

func (s *Store) DeleteAPIKey(id string) (model.APIKey, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.APIKey{}, ErrInvalidInput
	}
	return s.pgDeleteAPIKey(id)
}

func (s *Store) AuthenticateAPIKey(secret string) (model.Principal, error) {
	if strings.TrimSpace(secret) == "" {
		return model.Principal{}, ErrInvalidInput
	}
	return s.pgAuthenticateAPIKey(secret)
}

func (s *Store) ListEnrollmentTokens(tenantID string) ([]model.EnrollmentToken, error) {
	return s.pgListEnrollmentTokens(tenantID)
}

func (s *Store) CreateEnrollmentToken(tenantID, label string, ttl time.Duration) (model.EnrollmentToken, string, error) {
	label = strings.TrimSpace(label)
	if tenantID == "" || label == "" || ttl <= 0 {
		return model.EnrollmentToken{}, "", ErrInvalidInput
	}
	return s.pgCreateEnrollmentToken(tenantID, label, ttl)
}

func (s *Store) ListNodeKeys(tenantID string, platformAdmin bool) ([]model.NodeKey, error) {
	return s.pgListNodeKeys(tenantID, platformAdmin)
}

func (s *Store) GetNodeKey(id string) (model.NodeKey, error) {
	return s.pgGetNodeKey(id)
}

func (s *Store) AuthenticateNodeKey(secret string) (model.NodeKey, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return model.NodeKey{}, ErrInvalidInput
	}
	return s.pgAuthenticateNodeKey(secret)
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
	if (scope == model.NodeKeyScopePlatformNode) != (tenantID == "") {
		return model.NodeKey{}, "", ErrInvalidInput
	}
	return s.pgCreateScopedNodeKey(tenantID, label, scope)
}

func (s *Store) RevokeNodeKey(id string) (model.NodeKey, error) {
	return s.pgRevokeNodeKey(id)
}

func (s *Store) BootstrapNode(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.NodeKey, model.Runtime, string, error) {
	secret = strings.TrimSpace(secret)
	runtimeName = strings.TrimSpace(runtimeName)
	if secret == "" {
		return model.NodeKey{}, model.Runtime{}, "", ErrInvalidInput
	}
	return s.pgBootstrapNode(secret, runtimeName, endpoint, labels, machineName, machineFingerprint)
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
	return s.pgBootstrapClusterNode(secret, runtimeName, endpoint, labels, machineName, machineFingerprint)
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
	return s.pgCreateRuntime(tenantID, name, runtimeType, endpoint, labels)
}

func (s *Store) ConsumeEnrollmentToken(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Runtime, string, error) {
	secret = strings.TrimSpace(secret)
	runtimeName = strings.TrimSpace(runtimeName)
	if secret == "" {
		return model.Runtime{}, "", ErrInvalidInput
	}
	return s.pgConsumeEnrollmentToken(secret, runtimeName, endpoint, labels, machineName, machineFingerprint)
}

func (s *Store) AuthenticateRuntimeKey(secret string) (model.Runtime, model.Principal, error) {
	if strings.TrimSpace(secret) == "" {
		return model.Runtime{}, model.Principal{}, ErrInvalidInput
	}
	return s.pgAuthenticateRuntimeKey(secret)
}

func (s *Store) UpdateRuntimeHeartbeat(runtimeID, endpoint string) (model.Runtime, error) {
	return s.pgUpdateRuntimeHeartbeat(runtimeID, endpoint)
}

func (s *Store) UpdateRuntimeHeartbeatWithLabels(runtimeID, endpoint string, labels map[string]string) (model.Runtime, error) {
	if len(labels) == 0 {
		return s.UpdateRuntimeHeartbeat(runtimeID, endpoint)
	}
	return s.pgUpdateRuntimeHeartbeatWithLabels(runtimeID, endpoint, labels)
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
	return s.pgMarkRuntimeOfflineStale(after)
}

func (s *Store) ListNodes(tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	return s.pgListNodes(tenantID, platformAdmin)
}

func (s *Store) ListRuntimesByNodeKey(nodeKeyID, tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	nodeKeyID = strings.TrimSpace(nodeKeyID)
	if nodeKeyID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListRuntimesByNodeKey(nodeKeyID, tenantID, platformAdmin)
}

func (s *Store) ListRuntimes(tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	return s.pgListRuntimes(tenantID, platformAdmin)
}

func (s *Store) GetRuntime(id string) (model.Runtime, error) {
	return s.pgGetRuntime(id)
}

func (s *Store) DetachRuntimeOwnership(runtimeID string) (model.Runtime, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return model.Runtime{}, ErrInvalidInput
	}
	return s.pgDetachRuntimeOwnership(runtimeID)
}

func (s *Store) DeleteRuntime(runtimeID string) (model.Runtime, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return model.Runtime{}, ErrInvalidInput
	}
	return s.pgDeleteRuntime(runtimeID)
}

func (s *Store) RuntimeVisibleToTenant(runtimeID, tenantID string, platformAdmin bool) (bool, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return false, ErrInvalidInput
	}
	return s.pgRuntimeVisibleToTenant(runtimeID, tenantID, platformAdmin)
}

func (s *Store) ListRuntimeAccessGrants(runtimeID string) ([]model.RuntimeAccessGrant, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListRuntimeAccessGrants(runtimeID)
}

func (s *Store) GrantRuntimeAccess(runtimeID, ownerTenantID, granteeTenantID string) (model.RuntimeAccessGrant, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	ownerTenantID = strings.TrimSpace(ownerTenantID)
	granteeTenantID = strings.TrimSpace(granteeTenantID)
	if runtimeID == "" || ownerTenantID == "" || granteeTenantID == "" {
		return model.RuntimeAccessGrant{}, ErrInvalidInput
	}
	return s.pgGrantRuntimeAccess(runtimeID, ownerTenantID, granteeTenantID)
}

func (s *Store) RevokeRuntimeAccess(runtimeID, ownerTenantID, granteeTenantID string) (bool, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	ownerTenantID = strings.TrimSpace(ownerTenantID)
	granteeTenantID = strings.TrimSpace(granteeTenantID)
	if runtimeID == "" || ownerTenantID == "" || granteeTenantID == "" {
		return false, ErrInvalidInput
	}
	return s.pgRevokeRuntimeAccess(runtimeID, ownerTenantID, granteeTenantID)
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
	return s.pgSetRuntimeAccessMode(runtimeID, ownerTenantID, accessMode)
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
	return s.pgSetRuntimePublicOffer(runtimeID, ownerTenantID, normalizedOffer)
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
	return s.pgSetRuntimePoolMode(runtimeID, poolMode)
}

func (s *Store) EnsureManagedSharedLocationLabels(labels map[string]string) (model.Runtime, bool, error) {
	labels = normalizeManagedSharedLocationLabels(labels)
	return s.pgEnsureManagedSharedLocationLabels(labels)
}

func normalizeManagedSharedLocationLabels(labels map[string]string) map[string]string {
	normalized := map[string]string{}
	if value := strings.TrimSpace(labels[runtimepkg.LocationCountryCodeLabelKey]); value != "" {
		normalized[runtimepkg.LocationCountryCodeLabelKey] = strings.ToLower(value)
	} else if value := strings.TrimSpace(labels[runtimepkg.RegionLabelKey]); value != "" {
		normalized[runtimepkg.RegionLabelKey] = value
	}
	if value := strings.TrimSpace(labels[runtimepkg.EdgeGroupIDLabelKey]); edgetopology.ValidAuthorityID(value) {
		normalized[runtimepkg.EdgeGroupIDLabelKey] = value
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func (s *Store) ListApps(tenantID string, platformAdmin bool) ([]model.App, error) {
	return s.pgListApps(tenantID, platformAdmin)
}

// ListAppsMetadata returns app records without hydrating backing services or bindings.
// Callers that only need routing, source, spec, and status should prefer this lighter view.
func (s *Store) ListAppsMetadata(tenantID string, platformAdmin bool) ([]model.App, error) {
	return s.pgListAppsMetadata(tenantID, platformAdmin)
}

// ListDeletedAppsMetadata returns only durable app tombstones. It is kept
// separate from the normal app views so background cleanup can replay after a
// restart without exposing deleted applications to serving or user APIs.
func (s *Store) ListDeletedAppsMetadata(tenantID string, platformAdmin bool) ([]model.App, error) {
	return s.pgListDeletedAppsMetadata(tenantID, platformAdmin)
}

func (s *Store) ListAppsMetadataByIDs(appIDs []string) ([]model.App, error) {
	appIDSet := trimmedStringSet(appIDs)
	if len(appIDSet) == 0 {
		return nil, nil
	}
	return s.pgListAppsMetadataByIDs(sortedTrimmedStringKeys(appIDSet))
}

func (s *Store) ListAppsMetadataByProjectIDs(projectIDs []string) ([]model.App, error) {
	projectIDSet := trimmedStringSet(projectIDs)
	if len(projectIDSet) == 0 {
		return nil, nil
	}
	return s.pgListAppsMetadataByProjectIDs(sortedTrimmedStringKeys(projectIDSet))
}

func (s *Store) ListAppsByProjectIDs(projectIDs []string) ([]model.App, error) {
	projectIDSet := trimmedStringSet(projectIDs)
	if len(projectIDSet) == 0 {
		return nil, nil
	}
	return s.pgListAppsByProjectIDs(sortedTrimmedStringKeys(projectIDSet))
}

func (s *Store) GetApp(id string) (model.App, error) {
	return s.pgGetApp(id)
}

func (s *Store) GetAppMetadata(id string) (model.App, error) {
	return s.pgGetAppMetadata(id)
}

func (s *Store) GetAppByHostname(hostname string) (model.App, error) {
	hostname = normalizeAppDomainHostname(hostname)
	return s.pgGetAppByHostname(hostname)
}

func (s *Store) GetAppByRoutePrefix(hostname, pathPrefix string) (model.App, error) {
	hostname = normalizeAppDomainHostname(hostname)
	pathPrefix = model.NormalizeAppRoutePathPrefix(pathPrefix)
	app, err := s.pgGetAppByRoutePrefix(hostname, pathPrefix)
	if !errors.Is(err, ErrNotFound) {
		return app, err
	}
	return s.pgGetAppByProjectRoute(hostname, pathPrefix, true)
}

func (s *Store) GetAppByRoute(hostname, requestPath string) (model.App, error) {
	hostname = normalizeAppDomainHostname(hostname)
	requestPath = model.NormalizeAppRoutePathPrefix(requestPath)
	app, err := s.pgGetAppByRoute(hostname, requestPath)
	if !errors.Is(err, ErrNotFound) {
		return app, err
	}
	return s.pgGetAppByProjectRoute(hostname, requestPath, false)
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
	if route.DNSRouting != nil {
		return model.App{}, ErrInvalidInput
	}
	id = strings.TrimSpace(id)
	route = model.NormalizeAppRoute(route)
	if id == "" || route.Hostname == "" {
		return model.App{}, ErrInvalidInput
	}
	return s.pgUpdateAppRoute(id, route)
}

func (s *Store) UpdateAppImageMirrorLimit(id string, limit int) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" || limit < 0 {
		return model.App{}, ErrInvalidInput
	}
	limit = model.EffectiveAppImageMirrorLimit(limit)
	return s.pgUpdateAppImageMirrorLimit(id, limit)
}

// UpdateAppRightSizing updates control-plane recommendation policy only. The
// policy is not part of the Kubernetes workload template and must not enqueue
// or apply a workload deployment.
func (s *Store) UpdateAppRightSizing(id string, rightSizing model.AppRightSizingSpec) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" || model.NormalizeAppRightSizingMode(rightSizing.Mode) == "" {
		return model.App{}, ErrInvalidInput
	}
	rightSizing = model.NormalizeAppRightSizingSpec(rightSizing)
	return s.pgUpdateAppRightSizing(id, rightSizing)
}

func (s *Store) UpdateAppOriginSource(id string, source model.AppSource) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.App{}, ErrInvalidInput
	}
	return s.pgUpdateAppOriginSource(id, source)
}

func (s *Store) UpdateAppSourceSyncStatus(id string, sourceSync *model.AppSourceSyncStatus) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.App{}, ErrInvalidInput
	}
	return s.pgUpdateAppSourceSyncStatus(id, sourceSync)
}

func (s *Store) SyncObservedManagedPostgresSpec(id string, desiredSpec model.AppSpec) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.App{}, ErrInvalidInput
	}
	return s.pgSyncObservedManagedPostgresSpec(id, desiredSpec)
}

func (s *Store) SyncObservedManagedAppBaseline(id string, desiredSpec model.AppSpec, desiredSource *model.AppSource) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.App{}, ErrInvalidInput
	}
	return s.pgSyncObservedManagedAppBaseline(id, desiredSpec, desiredSource)
}

func (s *Store) PurgeApp(id string) (model.App, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.App{}, ErrInvalidInput
	}
	return s.pgPurgeApp(id)
}

func (s *Store) createApp(tenantID, projectID, name, description string, spec model.AppSpec, source *model.AppSource, route *model.AppRoute) (model.App, error) {
	if spec.WorkloadGeneration != "" {
		return model.App{}, ErrInvalidInput
	}
	if err := model.ValidateRecoveryPaths(spec.EphemeralPaths, spec); err != nil {
		return model.App{}, ErrInvalidInput
	}
	if spec.NodeLoss == nil {
		spec.NodeLoss = &model.AppNodeLossPolicy{Mode: model.AppNodeLossWait}
	}
	if route != nil && route.DNSRouting != nil {
		return model.App{}, ErrInvalidInput
	}
	if spec.CPURequestTemplateMilliCores != nil || spec.MemoryRequestTemplateMebibytes != nil {
		return model.App{}, ErrInvalidInput
	}
	name = strings.TrimSpace(name)
	route = normalizeAppRouteForStore(route)
	allowPendingImport := source != nil && isQueuedImportSourceType(source.Type) && strings.TrimSpace(spec.Image) == ""
	if tenantID == "" || projectID == "" || name == "" || (!allowPendingImport && spec.Image == "") || spec.Replicas < 1 {
		return model.App{}, ErrInvalidInput
	}
	if spec.Postgres != nil && spec.Postgres.Suspended {
		return model.App{}, ErrInvalidInput
	}
	if err := reconcileManagedPostgresRuntimeResources(spec.Postgres, nil); err != nil {
		return model.App{}, err
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
	return s.pgCreateApp(tenantID, projectID, name, description, spec, source, route)
}

func (s *Store) CreateOperation(op model.Operation) (model.Operation, error) {
	created, _, err := s.CreateOperationWithResult(op)
	return created, err
}

// CreateOperationWithResult creates an operation or, for an exact retry of an
// active managed PostgreSQL suspend, resume, or resize request, returns the
// sole existing operation. Callers that emit acceptance audit events must only
// do so when result.Created is true.
func (s *Store) CreateOperationWithResult(op model.Operation) (model.Operation, OperationCreateResult, error) {
	created, outcome, err := s.createOperationWithPolicy(op, operationCreatePolicy{})
	if err != nil {
		return model.Operation{}, OperationCreateResult{}, err
	}
	result := OperationCreateResult{
		Created: strings.TrimSpace(created.ID) != "" && !outcome.ReusedExistingOperation,
	}
	// Every migration gets an immutable ledger entry before a worker can act on
	// it.  If the audit write cannot be made, fail the operation closed so no
	// target artifact can be promoted without a 90-day record.
	if created.Type == model.OperationTypeMigrate && strings.TrimSpace(created.ID) != "" {
		if _, found, ledgerErr := s.LatestAppMigrationLedger(created.ID); ledgerErr != nil {
			return created, result, ledgerErr
		} else if !found {
			initialLedger := model.AppMigrationLedger{
				TenantID:              created.TenantID,
				AppID:                 created.AppID,
				OperationID:           created.ID,
				OldRuntimeID:          created.SourceRuntimeID,
				NewRuntimeID:          created.TargetRuntimeID,
				CutoverStatus:         model.AppMigrationCutoverPending,
				OldArtifactsProtected: true,
			}
			// Capture the image and desired replica baseline at ledger creation
			// time.  The app's durable spec may be updated after a migration is
			// queued; retaining this immutable baseline is what lets global cache
			// cleanup protect the source artifact while cutover is pending.
			if migrationApp, appErr := s.GetApp(created.AppID); appErr == nil {
				initialLedger.ImageRef = strings.TrimSpace(migrationApp.Spec.Image)
				initialLedger.DesiredReplicas = migrationApp.Spec.Replicas
				initialLedger.ProjectID = migrationApp.ProjectID
			} else if !errors.Is(appErr, ErrNotFound) {
				_, _ = s.FailOperation(created.ID, "migration ledger initialization could not load app: "+appErr.Error())
				return created, result, appErr
			}
			if _, ledgerErr := s.RecordAppMigrationLedger(initialLedger); ledgerErr != nil {
				_, _ = s.FailOperation(created.ID, "migration ledger initialization failed: "+ledgerErr.Error())
				return created, result, fmt.Errorf("initialize migration ledger for operation %s: %w", created.ID, ledgerErr)
			}
		}
	}
	return created, result, nil
}

func (s *Store) createOperationWithPolicy(op model.Operation, policy operationCreatePolicy) (model.Operation, operationCreateOutcome, error) {
	if op.TenantID == "" || op.Type == "" || op.AppID == "" {
		return model.Operation{}, operationCreateOutcome{}, ErrInvalidInput
	}
	if isOOMRightSizingActor(op) && !policy.AllowOOMRightSizingDeploy {
		return model.Operation{}, operationCreateOutcome{}, ErrInvalidInput
	}
	return s.pgCreateOperation(op, policy)
}

type OperationListFilter = contract.Filter

func (s *Store) ListOperations(tenantID string, platformAdmin bool) ([]model.Operation, error) {
	return s.ListOperationsFiltered(tenantID, platformAdmin, OperationListFilter{})
}

func (s *Store) ListOperationsFiltered(tenantID string, platformAdmin bool, filter OperationListFilter) ([]model.Operation, error) {
	return s.pgListOperationsFiltered(tenantID, platformAdmin, filter, true)
}

func (s *Store) ListOperationSummaries(tenantID string, platformAdmin bool) ([]model.Operation, error) {
	return s.ListOperationSummariesFiltered(tenantID, platformAdmin, OperationListFilter{})
}

func (s *Store) ListOperationSummariesFiltered(tenantID string, platformAdmin bool, filter OperationListFilter) ([]model.Operation, error) {
	return s.pgListOperationsFiltered(tenantID, platformAdmin, filter, false)
}

func (s *Store) ListOperationsByApp(tenantID string, platformAdmin bool, appID string) ([]model.Operation, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return s.ListOperations(tenantID, platformAdmin)
	}
	return s.ListOperationsFiltered(tenantID, platformAdmin, OperationListFilter{AppID: appID})
}

func (s *Store) ListOperationSummariesByApp(tenantID string, platformAdmin bool, appID string) ([]model.Operation, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return s.ListOperationSummaries(tenantID, platformAdmin)
	}
	return s.ListOperationSummariesFiltered(tenantID, platformAdmin, OperationListFilter{AppID: appID})
}

func sortOperationsOldestFirst(ops []model.Operation) {
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].CreatedAt.Equal(ops[j].CreatedAt) {
			return ops[i].ID < ops[j].ID
		}
		return ops[i].CreatedAt.Before(ops[j].CreatedAt)
	})
}

func cloneOperationControllerTimingSegments(segments []model.OperationControllerTimingSegment) []model.OperationControllerTimingSegment {
	if len(segments) == 0 {
		return nil
	}
	out := make([]model.OperationControllerTimingSegment, 0, len(segments))
	for _, segment := range segments {
		if strings.TrimSpace(segment.Name) == "" {
			continue
		}
		out = append(out, model.OperationControllerTimingSegment{
			Name:                 strings.TrimSpace(segment.Name),
			DurationMilliseconds: segment.DurationMilliseconds,
		})
	}
	return out
}

func (s *Store) HasActiveOperationByApp(tenantID string, platformAdmin bool, appID string) (bool, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return false, nil
	}
	return s.pgHasActiveOperationByApp(tenantID, platformAdmin, appID)
}

func (s *Store) ListConsoleOperationsByApp(tenantID string, platformAdmin bool, appID string, recentLimit int) ([]model.Operation, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return []model.Operation{}, nil
	}
	if recentLimit < 1 {
		recentLimit = 1
	}
	return s.pgListConsoleOperationsByApp(tenantID, platformAdmin, appID, recentLimit)
}

func (s *Store) ListOperationsWithDesiredSourceByApp(tenantID string, platformAdmin bool, appID string) ([]model.Operation, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return []model.Operation{}, nil
	}
	return s.pgListOperationsWithDesiredSourceByApp(tenantID, platformAdmin, appID)
}

func (s *Store) ListOperationsWithDesiredSourceByApps(tenantID string, platformAdmin bool, appIDs []string) (map[string][]model.Operation, error) {
	appIDSet := trimmedStringSet(appIDs)
	if len(appIDSet) == 0 {
		return map[string][]model.Operation{}, nil
	}
	return s.pgListOperationsWithDesiredSourceByApps(tenantID, platformAdmin, sortedTrimmedStringKeys(appIDSet))
}

// ListImageOperationsByApps is a narrow projection for console image usage.
// It avoids decoding large desired app specs when only source identity and
// timestamps are needed to build image candidates.
func (s *Store) ListImageOperationsByApps(tenantID string, platformAdmin bool, appIDs []string) (map[string][]model.Operation, error) {
	ids := trimmedStringSet(appIDs)
	if len(ids) == 0 {
		return map[string][]model.Operation{}, nil
	}
	return s.pgListImageOperationsByApps(tenantID, platformAdmin, sortedTrimmedStringKeys(ids))
}

func (s *Store) ListActiveOperations() ([]model.Operation, error) {
	return s.pgListActiveOperations()
}

func (s *Store) GetOperation(id string) (model.Operation, error) {
	return s.pgGetOperation(id)
}

func (s *Store) TryClaimPendingOperation(id string) (model.Operation, bool, error) {
	return s.pgTryClaimPendingOperation(id)
}

func (s *Store) ClaimNextPendingOperation() (model.Operation, bool, error) {
	return s.pgClaimNextPendingOperation()
}

func (s *Store) ClaimNextPendingForegroundOperation() (model.Operation, bool, error) {
	return s.pgClaimNextPendingForegroundOperation()
}

func (s *Store) ClaimNextPendingGitHubSyncImportOperation() (model.Operation, bool, error) {
	return s.pgClaimNextPendingGitHubSyncImportOperation()
}

func (s *Store) CompleteManagedOperationWithResult(id, manifestPath, message string, desiredSpec *model.AppSpec, desiredSource *model.AppSource) (model.Operation, error) {
	return s.completeOperation(id, "", manifestPath, message, desiredSpec, desiredSource, nil)
}

func (s *Store) CompleteManagedOperationWithSourceState(id, manifestPath, message string, desiredSpec *model.AppSpec, desiredSource, desiredOriginSource *model.AppSource) (model.Operation, error) {
	return s.completeOperation(id, "", manifestPath, message, desiredSpec, desiredSource, desiredOriginSource)
}

func (s *Store) CompleteManagedOperation(id, manifestPath, message string) (model.Operation, error) {
	return s.completeOperation(id, "", manifestPath, message, nil, nil, nil)
}

func (s *Store) CompleteAgentOperation(id, runtimeID, manifestPath, message string) (model.Operation, error) {
	return s.completeOperation(id, runtimeID, manifestPath, message, nil, nil, nil)
}

func (s *Store) UpdateOperationProgress(id, message string) (model.Operation, error) {
	message = strings.TrimSpace(message)
	if message == "" {
		return model.Operation{}, ErrInvalidInput
	}
	return s.pgUpdateOperationProgress(id, message)
}

func (s *Store) SetOperationControllerTiming(id string, segments []model.OperationControllerTimingSegment) (model.Operation, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Operation{}, ErrInvalidInput
	}
	return s.pgSetOperationControllerTiming(id, segments)
}

func (s *Store) completeOperation(id, runtimeID, manifestPath, message string, desiredSpec *model.AppSpec, desiredSource, desiredOriginSource *model.AppSource) (model.Operation, error) {
	return s.completeOperationGuarded(id, runtimeID, manifestPath, message, desiredSpec, desiredSource, desiredOriginSource, nil)
}

func (s *Store) completeOperationGuarded(id, runtimeID, manifestPath, message string, desiredSpec *model.AppSpec, desiredSource, desiredOriginSource *model.AppSource, guard *operationFinalizationGuard) (model.Operation, error) {
	return s.pgCompleteOperationGuarded(id, runtimeID, manifestPath, message, desiredSpec, desiredSource, desiredOriginSource, guard)
}
func (s *Store) DispatchOperationToRuntime(id, runtimeID string) (model.Operation, error) {
	return s.pgDispatchOperationToRuntime(id, runtimeID)
}

func (s *Store) RequeueManagedOperation(id, message string) (model.Operation, error) {
	return s.requeueManagedOperationGuarded(id, message, nil)
}

func (s *Store) requeueManagedOperationGuarded(id, message string, guard *operationFinalizationGuard) (model.Operation, error) {
	return s.pgRequeueManagedOperationGuarded(id, message, guard)
}

func (s *Store) RequeueInFlightManagedOperations(message string) (int, error) {
	return s.pgRequeueInFlightManagedOperations(message)
}

func (s *Store) FailOperation(id, message string) (model.Operation, error) {
	return s.failOperationGuarded(id, message, nil)
}

func (s *Store) failOperationGuarded(id, message string, guard *operationFinalizationGuard) (model.Operation, error) {
	op, err := s.pgFailOperationGuarded(id, message, guard)
	if err != nil {
		return op, err
	}
	if op.Type == model.OperationTypeMigrate {
		if ledgerErr := s.recordMigrationFailureLedger(op, message, model.OperationEvidenceSourceController); ledgerErr != nil {
			return op, errors.Join(err, ledgerErr)
		}
	}
	return op, nil
}

// FailAssignedAgentOperation atomically rejects a task that is still waiting
// for the specific runtime agent that requested the poll. This prevents a
// stale agent response from failing an operation after another worker has
// completed or been reassigned it.
func (s *Store) FailAssignedAgentOperation(id, runtimeID, message string) (model.Operation, bool, error) {
	id = strings.TrimSpace(id)
	runtimeID = strings.TrimSpace(runtimeID)
	if id == "" || runtimeID == "" {
		return model.Operation{}, false, ErrInvalidInput
	}
	op, claimed, err := s.pgFailAssignedAgentOperation(id, runtimeID, message)
	if err != nil || !claimed || op.Type != model.OperationTypeMigrate {
		return op, claimed, err
	}
	if ledgerErr := s.recordMigrationFailureLedger(op, message, model.OperationEvidenceSourceController); ledgerErr != nil {
		return op, claimed, ledgerErr
	}
	return op, claimed, nil
}

func releaseAttemptTerminal(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case model.ReleaseAttemptStatusCompleted,
		model.ReleaseAttemptStatusFailed,
		model.ReleaseAttemptStatusCancelled,
		model.ReleaseAttemptStatusSuperseded:
		return true
	default:
		return false
	}
}

func (s *Store) ListAssignedOperations(runtimeID string) ([]model.Operation, error) {
	return s.pgListAssignedOperations(runtimeID)
}

func (s *Store) AppendAuditEvent(event model.AuditEvent) error {
	if strings.TrimSpace(event.ChainID) != "" ||
		event.ChainSequence != 0 ||
		strings.TrimSpace(event.PreviousHash) != "" ||
		strings.TrimSpace(event.EventHash) != "" ||
		strings.TrimSpace(event.Provenance.Signature) != "" {
		return ErrInvalidInput
	}
	if event.ID == "" {
		event.ID = model.NewID("audit")
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	return s.pgAppendAuditEvent(event)
}

func (s *Store) ListAuditEvents(tenantID string, platformAdmin bool, limit int) ([]model.AuditEvent, error) {
	return s.pgListAuditEvents(tenantID, platformAdmin, limit)
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

func backfillRuntimeMetadata(runtime *model.Runtime, machine model.Machine) {
	if runtime == nil {
		return
	}
	runtime.AccessMode = normalizeRuntimeAccessMode(runtime.Type, runtime.AccessMode)
	runtime.PoolMode = model.NormalizeRuntimePoolMode(runtime.Type, runtime.PoolMode)
	runtime.ApplyNormalizedScope()
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
	if in.CPURequestTemplateMilliCores != nil {
		cpu := *in.CPURequestTemplateMilliCores
		out.CPURequestTemplateMilliCores = &cpu
	}
	if in.MemoryRequestTemplateMebibytes != nil {
		memory := *in.MemoryRequestTemplateMebibytes
		out.MemoryRequestTemplateMebibytes = &memory
	}
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
	if in.SSH != nil {
		ssh := *in.SSH
		ssh.AuthorizedKeyIDs = append([]string(nil), in.SSH.AuthorizedKeyIDs...)
		ssh.AuthorizedKeys = append([]string(nil), in.SSH.AuthorizedKeys...)
		out.SSH = &ssh
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
	out.NodeLoss = model.CloneAppNodeLossPolicy(in.NodeLoss)
	out.Continuity = model.CloneAppContinuityPolicy(in.Continuity)
	if in.Resources != nil {
		resources := *in.Resources
		out.Resources = &resources
	}
	if in.RightSizing != nil {
		rightSizing := *in.RightSizing
		out.RightSizing = &rightSizing
	}
	if in.Postgres != nil {
		out.Postgres = model.CloneAppPostgresSpec(in.Postgres)
	}
	model.ApplyAppSpecDefaults(&out)
	return &out
}

func cloneAppRightSizingSpec(in *model.AppRightSizingSpec) *model.AppRightSizingSpec {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

// Right-sizing mode/window/sample settings are control-plane policy. A
// workload operation may have been queued from an older AppSpec snapshot, so
// completing it must never roll this independently updated policy backward.
func preserveAppRightSizingPolicy(desired *model.AppSpec, current model.AppSpec) {
	if desired == nil {
		return
	}
	desired.RightSizing = cloneAppRightSizingSpec(current.RightSizing)
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
	out.DNSRouting = model.CloneAppStaticEdgeRouting(in.DNSRouting)
	return &out
}

func normalizeAppRouteForStore(in *model.AppRoute) *model.AppRoute {
	if in == nil {
		return nil
	}
	out := model.NormalizeAppRoute(*in)
	if out.PublicURL == "" {
		out.PublicURL = model.AppRoutePublicURL(out.Hostname, out.PathPrefix)
	}
	return &out
}

func routePathPrefixMatches(prefix, requestPath string) bool {
	prefix = model.NormalizeAppRoutePathPrefix(prefix)
	requestPath = model.NormalizeAppRoutePathPrefix(requestPath)
	if prefix == "/" {
		return true
	}
	return requestPath == prefix || strings.HasPrefix(requestPath, prefix+"/")
}

func applyOperationToApp(state *storeProjection, op *model.Operation) error {
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
		preserveAppRightSizingPolicy(op.DesiredSpec, app.Spec)
		if err := applyGeneratedEnvSpec(op.DesiredSpec, &app.Spec); err != nil {
			return err
		}
	}
	switch op.Type {
	case model.OperationTypeAppStorageUnmount:
		if err := applyStorageUnmountOperation(app, *op); err != nil {
			return err
		}
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
	case model.OperationTypeDatabaseSwitchover,
		model.OperationTypeDatabaseLocalize, storagerecovery.OperationType:
		if op.DesiredSpec == nil {
			return ErrInvalidInput
		}
		if err := applyDesiredSpecBackingServicesState(state, app, op.DesiredSpec); err != nil {
			return err
		}
		app.Spec = *op.DesiredSpec
	case model.OperationTypeDatabaseSuspend, model.OperationTypeDatabaseResume:
		if err := applyManagedPostgresLifecycleState(state, app, op); err != nil {
			return err
		}
	case model.OperationTypeAppCPUResize, model.OperationTypeAppMemoryResize:
		return applyAppRequestResize(app, op)
	case model.OperationTypeDatabaseResize:
		return applyManagedPostgresResizeState(state, app, op)
	case pgimage.OperationType:
		return applyPostgresImageMigration(state, app, op)
	case model.OperationTypeDatabaseSourceRetire:
		return applyPostgresSourceRetirement(state, app, op)
	default:
		return ErrInvalidInput
	}
	if !isManagedPostgresServiceOperation(op.Type) {
		app.Status.LastOperationID = op.ID
		app.Status.LastMessage = op.ResultMessage
	}
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
	// Resize recovery must retain the original attempt and its durable journal.
	// Requeueing would make an already accepted mutation cancellable as pending.
	return op.Status == model.OperationStatusRunning && op.ExecutionMode == model.ExecutionModeManaged && !model.IsAppRequestResize(op.Type) && op.Type != pgimage.OperationType && op.Type != model.OperationTypeDatabaseSourceRetire && op.Type != model.OperationTypeDatabaseReplicaRetire && op.Type != model.OperationTypeAppStorageUnmount
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
	case model.OperationTypeAppStorageUnmount:
		return "deploying", nil
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
	case model.OperationTypeDatabaseLocalize, storagerecovery.OperationType:
		return "database-localize", nil
	case model.OperationTypeDatabaseSuspend:
		return "database-suspend", nil
	case model.OperationTypeDatabaseResume:
		return "database-resume", nil
	case model.OperationTypeDatabaseResize:
		return "database-resize", nil
	case pgimage.OperationType:
		return pgimage.OperationType, nil
	case model.OperationTypeDatabaseSourceRetire:
		return model.OperationTypeDatabaseSourceRetire, nil
	case model.OperationTypeDatabaseReplicaRetire:
		return model.OperationTypeDatabaseReplicaRetire, nil
	case model.OperationTypeAppCPUResize, model.OperationTypeAppMemoryResize:
		return op.Type, nil
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
	case model.OperationTypeAppStorageUnmount:
		return "storage unmount"
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
	case model.OperationTypeDatabaseLocalize, storagerecovery.OperationType:
		return "database localize"
	case model.OperationTypeDatabaseSuspend:
		return "database suspend"
	case model.OperationTypeDatabaseResume:
		return "database resume"
	case model.OperationTypeDatabaseResize:
		return "database resize"
	case pgimage.OperationType:
		return "database image migration"
	case model.OperationTypeDatabaseSourceRetire:
		return "database source retirement"
	case model.OperationTypeDatabaseReplicaRetire:
		return "database replica retirement"
	case model.OperationTypeAppCPUResize, model.OperationTypeAppMemoryResize:
		return "app resource request resize"
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

func findTenant(state *storeProjection, id string) int {
	for idx, tenant := range state.Tenants {
		if tenant.ID == id {
			return idx
		}
	}
	return -1
}

func findProject(state *storeProjection, id string) int {
	for idx, project := range state.Projects {
		if project.ID == id {
			return idx
		}
	}
	return -1
}

func findApp(state *storeProjection, id string) int {
	for idx, app := range state.Apps {
		if app.ID == id {
			return idx
		}
	}
	return -1
}

func projectDeleteRequested(state *storeProjection, projectID string) bool {
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
