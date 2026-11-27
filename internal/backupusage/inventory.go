package backupusage

import (
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/objectstorage"
)

// Namespace identifies physical backup objects without carrying storage credentials.
type Namespace struct {
	Provider, Endpoint, Bucket, Prefix string
}

func NamespaceOf(backend model.DataBackend) Namespace {
	return Namespace{
		Provider: model.NormalizeDataBackendProvider(backend.Provider),
		Endpoint: backend.Endpoint,
		Bucket:   backend.Bucket,
		Prefix:   backend.Prefix,
	}
}

func (n Namespace) Key() string {
	return n.ObjectID(strings.Trim(strings.TrimSpace(n.Prefix), "/"))
}

// ObjectID identifies the same physical object across overlapping prefixes.
func (n Namespace) ObjectID(physicalKey string) string {
	return strings.Join([]string{
		model.NormalizeDataBackendProvider(n.Provider),
		strings.ToLower(strings.TrimRight(strings.TrimSpace(n.Endpoint), "/")),
		strings.TrimSpace(n.Bucket),
		physicalKey,
	}, "\x00")
}

func (n Namespace) ContainsPhysicalKey(key string) bool {
	prefix := strings.Trim(strings.TrimSpace(n.Prefix), "/")
	if prefix == "" {
		return key != ""
	}
	return strings.HasPrefix(key, prefix+"/")
}

func (n Namespace) Measurable() bool {
	if strings.TrimSpace(n.Endpoint) == "" {
		return false
	}
	const probe = "fugue-usage-namespace-probe"
	logical, ok := objectstorage.LogicalKey(n.Prefix, objectstorage.PhysicalKey(n.Prefix, probe))
	return ok && logical == probe
}

type InventoryTotals struct {
	Count            int
	Bytes            int64
	ProvisionalCount int
	ProvisionalBytes int64
	RepositoryCount  int
	RepositoryBytes  int64
}

// InventoryObject retains the field layout used by persisted backup inventory pages.
type InventoryObject struct {
	ObservedAt   time.Time
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

type InventoryProgress struct {
	Generation string
	Pages      int
	Objects    int
}

// InventorySnapshot is a read-only view of a completed scan. Callers must not
// mutate its maps once supplied to the service.
type InventorySnapshot struct {
	InventoryProgress
	NamespaceOwner string
	StartedAt      time.Time
	FinishedAt     *time.Time
	Wanted         map[string]bool
	References     map[string]InventoryObject
	Totals         map[string]InventoryTotals
}

type Inventory struct {
	Current     *InventoryProgress
	Complete    *InventorySnapshot
	LastAttempt time.Time
	Error       string
}

func ObjectWithinCleanupGrace(lastModified, now time.Time, grace time.Duration) bool {
	if lastModified.IsZero() {
		return false
	}
	lastModified = lastModified.UTC()
	return !lastModified.After(now.Add(time.Minute)) && now.Sub(lastModified) <= grace
}
