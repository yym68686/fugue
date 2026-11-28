package backupusage

import (
	"context"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
	"golang.org/x/sync/singleflight"
)

const (
	DefaultCacheTTL       = 30 * time.Second
	reconciliationTimeout = 15 * time.Second
)

type UsageArtifact struct {
	Artifact          model.BackupArtifact
	PhysicalDeletedAt *time.Time
}

// Source is the read surface used by reconciliation. It does not expose a
// concrete store, a writer, a client capable of deletion, or credentials.
type Source interface {
	BackupUsage(tenantID string, platformAdmin bool) (model.BackupUsage, error)
	ListBackupBackends(tenantID string, platformAdmin bool) ([]model.BackupBackend, error)
	ListUsageArtifacts(tenantID string, platformAdmin bool) ([]UsageArtifact, error)
	ResolveNamespace(backendID, tenantID string, platformAdmin bool) (Namespace, error)
	ReadInventory(ctx context.Context, namespaceKey string) (Inventory, error)
}

type Config struct {
	CacheTTL                 time.Duration
	InventoryRefreshInterval time.Duration
	ArtifactGCGrace          time.Duration
	FailedRunGCGrace         time.Duration
}

type Service struct {
	source   Source
	cfg      Config
	now      func() time.Time
	mu       sync.RWMutex
	cache    map[string]cacheEntry
	inFlight singleflight.Group
}

type cacheEntry struct {
	expires time.Time
	value   Reconciliation
}

func New(source Source, cfg Config) *Service {
	return &Service{source: source, cfg: cfg, now: time.Now}
}

func (s *Service) Load(ctx context.Context, tenantID string, platformAdmin bool) (Usage, error) {
	recordedUsage, err := s.source.BackupUsage(tenantID, platformAdmin)
	if err != nil {
		return Usage{}, err
	}
	usage := FromModel(recordedUsage)
	cacheKey := "platform"
	if !platformAdmin {
		cacheKey = "tenant:" + strings.TrimSpace(tenantID)
	}
	value, err := s.loadReconciliation(ctx, cacheKey, tenantID, platformAdmin)
	if err != nil {
		return Usage{}, err
	}
	usage.Reconciliation = &value
	if reconciliationMeasuredCompletely(value) {
		bytes := value.ReferencedBytes + value.UnreferencedBytes + value.RepositoryManagedBytes
		objects := value.ReferencedObjectCount + value.UnreferencedObjectCount + value.RepositoryManagedObjectCount
		usage.PhysicalBytes = &bytes
		usage.PhysicalObjectCount = &objects
	}
	return usage, nil
}

func (s *Service) loadReconciliation(ctx context.Context, key, tenantID string, platformAdmin bool) (Reconciliation, error) {
	if value, ok := s.cached(key); ok {
		return value, nil
	}
	value, err, _ := s.inFlight.Do(key, func() (any, error) {
		if value, ok := s.cached(key); ok {
			return value, nil
		}
		value, err := s.reconcile(ctx, tenantID, platformAdmin)
		if err == nil && s.cfg.CacheTTL > 0 {
			s.mu.Lock()
			if s.cache == nil {
				s.cache = make(map[string]cacheEntry)
			}
			s.cache[key] = cacheEntry{expires: s.now().Add(s.cfg.CacheTTL), value: value}
			s.mu.Unlock()
		}
		return value, err
	})
	if err != nil {
		return Reconciliation{}, err
	}
	return value.(Reconciliation), nil
}

func (s *Service) cached(key string) (Reconciliation, bool) {
	s.mu.RLock()
	entry, ok := s.cache[key]
	s.mu.RUnlock()
	return entry.value, ok && !s.now().After(entry.expires)
}
