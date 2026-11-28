// Package inventoryscan owns the backup inventory schedule and checkpoint format.
package inventoryscan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"fugue/internal/backupusage"
	"fugue/internal/model"
	"fugue/internal/objectstorage"
)

type Config struct{ RefreshInterval, PageTimeout, RetryInterval, MaxScanAge time.Duration }

func ConfigFromEnv() Config {
	read := func(key string, fallback time.Duration) time.Duration {
		d, e := time.ParseDuration(os.Getenv(key))
		if e != nil || d <= 0 {
			return fallback
		}
		return d
	}
	return Config{read("FUGUE_BACKUP_INVENTORY_REFRESH_INTERVAL", 15*time.Minute), read("FUGUE_BACKUP_INVENTORY_PAGE_TIMEOUT", 10*time.Second), read("FUGUE_BACKUP_INVENTORY_RETRY_INTERVAL", 30*time.Second), read("FUGUE_BACKUP_INVENTORY_MAX_SCAN_AGE", 24*time.Hour)}
}
func (c Config) withDefaults() Config {
	d := Config{15 * time.Minute, 10 * time.Second, 30 * time.Second, 24 * time.Hour}
	if c.RefreshInterval > 0 {
		d.RefreshInterval = c.RefreshInterval
	}
	if c.PageTimeout > 0 {
		d.PageTimeout = c.PageTimeout
	}
	if c.RetryInterval > 0 {
		d.RetryInterval = c.RetryInterval
	}
	if c.MaxScanAge > 0 {
		d.MaxScanAge = c.MaxScanAge
	}
	return d
}

// Memory and checkpoint size depend on known artifact references and owners,
// not on the number of unreferenced physical objects in the bucket.
type scanState struct {
	NamespaceOwner     string
	Generation         string
	StartedAt          time.Time
	FinishedAt         *time.Time
	Cursor             string
	LastKey            string
	Pages              int
	Objects            int
	Wanted             map[string]bool
	References         map[string]backupusage.InventoryObject
	Totals             map[string]backupusage.InventoryTotals
	RepositoryPrefixes []string
}
type checkpoint struct {
	Version     int
	Current     *scanState
	Complete    *scanState
	LastAttempt time.Time
	NextAttempt time.Time
	Error       string
}
type namespaceGroup struct {
	key      string
	backends []model.BackupBackend
}

func checkpointKey(backends []model.BackupBackend) string {
	// Backend identity, ownership and credential revision are part of the cache
	// boundary. Only a digest is persisted; credentials never enter a checkpoint.
	copyBackends := append([]model.BackupBackend(nil), backends...)
	for i := range copyBackends {
		copyBackends[i].CreatedAt = time.Time{}
		copyBackends[i].UpdatedAt = time.Time{}
	}
	sort.Slice(copyBackends, func(i, j int) bool { return copyBackends[i].ID < copyBackends[j].ID })
	b, _ := json.Marshal(copyBackends)
	h := sha256.Sum256(b)
	return "backup-inventory/v3/" + hex.EncodeToString(h[:])
}

// Repository blocks are owned by the snapshot engine, not by a single artifact.
// Derive the boundary from durable engine metadata, never a hardcoded directory.
func RepositoryPrefix(artifact model.BackupArtifact, backend model.DataBackend) string {
	if artifact.Kind != model.BackupArtifactKindLonghornSnapshot {
		return ""
	}
	u, err := url.Parse(artifact.Manifest.Metadata["backup_target_url"])
	if err != nil || u.Scheme != "s3" || u.User == nil || u.User.Username() != backend.Bucket || u.RawQuery != "" || u.Fragment != "" {
		return ""
	}
	prefix := strings.TrimPrefix(u.Path, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" || path.Clean(prefix) != prefix || strings.ContainsRune(prefix, '\x00') {
		return ""
	}
	root := strings.Trim(backend.Prefix, "/")
	if root != "" && !strings.HasPrefix(prefix, root+"/") {
		return ""
	}
	return prefix + "/"
}

// Pages contains the metadata and page set that must commit together.
type Pages struct {
	Metadata []byte
	Pages    map[string][]byte
}

// Store commits paged or legacy checkpoints, never both.
type Store interface {
	Backends() ([]model.BackupBackend, error)
	BackendForInventory(id string) (model.BackupBackend, error)
	Artifacts() ([]model.BackupArtifact, error)
	ReadPages(context.Context, string) (Pages, []byte, error)
	UpdatePages(context.Context, string, func(Pages, []byte) (Pages, error), func([]byte) ([]byte, error)) (bool, error)
}
type Page struct {
	Cursor  string
	More    bool
	Objects []backupusage.InventoryObject
}
type ObjectReader struct {
	Namespace backupusage.Namespace
	ListPage  func(context.Context, string) (Page, error)
}

type Service struct {
	store Store
	open  func(model.BackupBackend) (ObjectReader, error)
	cfg   Config
	grace time.Duration
}

func New(db Store, open func(model.BackupBackend) (ObjectReader, error), cfg Config, grace time.Duration) *Service {
	return &Service{store: db, open: open, cfg: cfg.withDefaults(), grace: grace}
}

func (s *Service) RefreshInterval() time.Duration { return s.cfg.RefreshInterval }

func (s *Service) groups() (map[string]*namespaceGroup, error) {
	backends, err := s.store.Backends()
	if err != nil {
		return nil, err
	}
	groups := map[string]*namespaceGroup{}
	for _, b := range backends {
		if b.Provider != model.DataBackendProviderCloudflareR2 {
			continue
		}
		b, err = s.store.BackendForInventory(b.ID)
		if err != nil {
			return nil, err
		}
		key := backupusage.NamespaceOf(model.BackupBackendAsDataBackend(b)).Key()
		if groups[key] == nil {
			groups[key] = &namespaceGroup{}
		}
		groups[key].backends = append(groups[key].backends, b)
	}
	for _, g := range groups {
		g.key = checkpointKey(g.backends)
	}
	return groups, nil
}

func (s *Service) Run(ctx context.Context, onError func(error)) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	next := map[string]time.Time{}
	for {
		if ctx.Err() != nil {
			return
		}
		groups, err := s.groups()
		if err == nil {
			live := map[string]bool{}
			for _, g := range groups {
				live[g.key] = true
				if time.Now().Before(next[g.key]) {
					continue
				}
				var due time.Time
				due, err = s.advance(ctx, g)
				if err != nil {
					next[g.key] = time.Now().Add(s.cfg.RetryInterval)
					break
				}
				next[g.key] = due
			}
			for key := range next {
				if !live[key] {
					delete(next, key)
				}
			}
		}
		if err != nil && ctx.Err() == nil && onError != nil {
			onError(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (s *Service) Advance(ctx context.Context) error {
	groups, err := s.groups()
	if err != nil {
		return err
	}
	for _, g := range groups {
		if _, err = s.advance(ctx, g); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) advance(parent context.Context, g *namespaceGroup) (time.Time, error) {
	cfg := s.cfg
	now := time.Now().UTC()
	var next time.Time
	ctx, cancel := context.WithTimeout(parent, cfg.PageTimeout+5*time.Second)
	defer cancel()
	advance := func(cp *checkpoint) (bool, error) {
		if now.Before(cp.NextAttempt) {
			next = cp.NextAttempt
			return false, nil
		}
		if cp.Current == nil && cp.Complete != nil && cp.Complete.FinishedAt != nil && now.Sub(*cp.Complete.FinishedAt) < cfg.RefreshInterval {
			next = cp.Complete.FinishedAt.Add(cfg.RefreshInterval)
			return false, nil
		}
		backend, err := s.open(g.backends[0])
		if err != nil {
			return false, err
		}
		namespace := backend.Namespace
		if !namespace.Measurable() {
			return false, errors.New("backup namespace is not measurable")
		}
		if cp.Current == nil || now.Sub(cp.Current.StartedAt) > cfg.MaxScanAge {
			cp.Current = &scanState{Generation: model.NewID("inventory"), StartedAt: now, Wanted: map[string]bool{}, References: map[string]backupusage.InventoryObject{}, Totals: map[string]backupusage.InventoryTotals{}}
			owner := g.backends[0].TenantID
			for _, b := range g.backends {
				if b.TenantID != owner {
					owner = ""
					break
				}
			}
			cp.Current.NamespaceOwner = owner
			artifacts, err := s.store.Artifacts()
			if err != nil {
				return false, err
			}
			ids := map[string]bool{}
			for _, b := range g.backends {
				ids[b.ID] = true
			}
			for _, a := range artifacts {
				if !ids[a.BackendID] {
					continue
				}
				if prefix := RepositoryPrefix(a, model.BackupBackendAsDataBackend(g.backends[0])); prefix != "" {
					found := false
					for _, p := range cp.Current.RepositoryPrefixes {
						if p == prefix {
							found = true
							break
						}
					}
					if !found {
						cp.Current.RepositoryPrefixes = append(cp.Current.RepositoryPrefixes, prefix)
					}
				}
				keys, err := backupusage.OwnedObjectKeys(a)
				if err != nil {
					continue
				}
				for _, k := range keys {
					cp.Current.Wanted[objectstorage.PhysicalKey(namespace.Prefix, k)] = true
				}
			}
		}
		scan := cp.Current
		cp.LastAttempt = now
		pageCtx, pageCancel := context.WithTimeout(ctx, cfg.PageTimeout)
		defer pageCancel()
		var pageErr error
		// Equivalent namespace credentials may have different permissions. Try each
		// declared backend within one bounded page budget.
		var response Page
		for _, b := range g.backends {
			candidate, e := s.open(b)
			if e != nil {
				pageErr = e
				continue
			}
			response, pageErr = candidate.ListPage(pageCtx, scan.Cursor)
			if pageErr == nil {
				break
			}
		}
		if pageErr != nil {
			cp.Error = "list_failed"
			if errors.Is(pageErr, context.DeadlineExceeded) {
				cp.Error = "page_timeout"
			}
			cp.NextAttempt = now.Add(cfg.RetryInterval)
			next = cp.NextAttempt
			return true, nil
		}
		nextCursor := response.Cursor
		if response.More && (nextCursor == "" || nextCursor == scan.Cursor) {
			cp.Error = "invalid_pagination"
			cp.NextAttempt = now.Add(cfg.RetryInterval)
			next = cp.NextAttempt
			return true, nil
		}
		objects := response.Objects
		sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
		for _, o := range objects {
			o.ObservedAt = now
			if o.Key <= scan.LastKey {
				continue
			}
			scan.LastKey = o.Key
			scan.Objects++
			if !namespace.ContainsPhysicalKey(o.Key) {
				continue
			}
			owner := ""
			logical, ok := objectstorage.LogicalKey(namespace.Prefix, o.Key)
			if ok {
				parts := strings.Split(logical, "/")
				if len(parts) > 1 && (parts[0] == "apps" || parts[0] == "data-workspaces") {
					owner = parts[1]
				}
			}
			add := func(key string) {
				v := scan.Totals[key]
				v.Count++
				v.Bytes += o.Size
				for _, prefix := range scan.RepositoryPrefixes {
					if !scan.Wanted[o.Key] && strings.HasPrefix(o.Key, prefix) {
						v.RepositoryCount++
						v.RepositoryBytes += o.Size
						scan.Totals[key] = v
						return
					}
				}
				if backupusage.ObjectWithinCleanupGrace(o.LastModified, o.ObservedAt, s.grace) {
					v.ProvisionalCount++
					v.ProvisionalBytes += o.Size
				}
				scan.Totals[key] = v
			}
			add("all")
			if owner != "" {
				add("tenant:" + owner)
			}
			if scan.Wanted[o.Key] {
				scan.References[o.Key] = o
			}
		}
		scan.Pages++
		scan.Cursor = nextCursor
		cp.Error = ""
		cp.NextAttempt = time.Time{}
		if !response.More {
			finished := time.Now().UTC()
			scan.FinishedAt = &finished
			scan.Cursor = ""
			cp.Complete = scan
			cp.Current = nil
			next = scan.FinishedAt.Add(cfg.RefreshInterval)
		}
		return true, nil
	}
	legacy := func(raw []byte) ([]byte, error) {
		cp, err := decodePages(Pages{}, raw)
		if err != nil {
			return nil, err
		}
		dirty, err := advance(&cp)
		if err != nil || !dirty {
			return nil, err
		}
		return json.Marshal(cp)
	}
	_, err := s.store.UpdatePages(ctx, g.key, func(pages Pages, raw []byte) (Pages, error) {
		cp, err := decodePages(pages, raw)
		if err != nil {
			return Pages{}, err
		}
		dirty, err := advance(&cp)
		if err != nil || !dirty {
			return Pages{}, err
		}
		return encodePages(cp)
	}, legacy)
	return next, err
}

// Read returns a detached reconciliation view. It never opens object storage
// or mutates the checkpoint, including when a scan is incomplete.
func (s *Service) Read(ctx context.Context, namespaceKey string) (backupusage.Inventory, error) {
	groups, err := s.groups()
	if err != nil {
		return backupusage.Inventory{}, err
	}
	g := groups[namespaceKey]
	if g == nil {
		return backupusage.Inventory{}, errors.New("backup namespace no longer configured")
	}
	pages, raw, err := s.store.ReadPages(ctx, g.key)
	if err != nil {
		return backupusage.Inventory{}, err
	}
	cp, err := decodePages(pages, raw)
	if err != nil {
		return backupusage.Inventory{}, err
	}
	progress := func(scan *scanState) backupusage.InventoryProgress {
		return backupusage.InventoryProgress{Generation: scan.Generation, Pages: scan.Pages, Objects: scan.Objects}
	}
	view := backupusage.Inventory{LastAttempt: cp.LastAttempt, Error: cp.Error}
	if cp.Current != nil {
		p := progress(cp.Current)
		view.Current = &p
	}
	if scan := cp.Complete; scan != nil {
		view.Complete = &backupusage.InventorySnapshot{
			InventoryProgress: progress(scan),
			NamespaceOwner:    scan.NamespaceOwner, StartedAt: scan.StartedAt, FinishedAt: scan.FinishedAt,
			Wanted: scan.Wanted, References: scan.References, Totals: scan.Totals,
		}
	}
	return view, nil
}
