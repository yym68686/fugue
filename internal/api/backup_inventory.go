package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BackupInventoryConfig struct{ RefreshInterval, PageTimeout, RetryInterval, MaxScanAge time.Duration }

func BackupInventoryConfigFromEnv() BackupInventoryConfig {
	read := func(key string, fallback time.Duration) time.Duration {
		d, e := time.ParseDuration(os.Getenv(key))
		if e != nil || d <= 0 {
			return fallback
		}
		return d
	}
	return BackupInventoryConfig{read("FUGUE_BACKUP_INVENTORY_REFRESH_INTERVAL", 15*time.Minute), read("FUGUE_BACKUP_INVENTORY_PAGE_TIMEOUT", 10*time.Second), read("FUGUE_BACKUP_INVENTORY_RETRY_INTERVAL", 30*time.Second), read("FUGUE_BACKUP_INVENTORY_MAX_SCAN_AGE", 24*time.Hour)}
}
func (c BackupInventoryConfig) normalized() BackupInventoryConfig {
	d := BackupInventoryConfig{15 * time.Minute, 10 * time.Second, 30 * time.Second, 24 * time.Hour}
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
type backupInventoryTotals struct {
	Count            int
	Bytes            int64
	ProvisionalCount int
	ProvisionalBytes int64
}
type backupInventoryScan struct {
	NamespaceOwner string
	Generation     string
	StartedAt      time.Time
	FinishedAt     *time.Time
	Cursor         string
	LastKey        string
	Pages          int
	Objects        int
	Wanted         map[string]bool
	References     map[string]dataObjectInfo
	Totals         map[string]backupInventoryTotals
}
type backupInventoryCheckpoint struct {
	Version     int
	Current     *backupInventoryScan
	Complete    *backupInventoryScan
	LastAttempt time.Time
	NextAttempt time.Time
	Error       string
}
type backupInventoryGroup struct {
	key      string
	backends []model.BackupBackend
}

func backupInventoryKey(backends []model.BackupBackend) string {
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
	return "backup-inventory/v1/" + hex.EncodeToString(h[:])
}
func (s *Server) backupInventoryGroups() (map[string]*backupInventoryGroup, error) {
	backends, err := s.store.ListBackupBackends("", true)
	if err != nil {
		return nil, err
	}
	groups := map[string]*backupInventoryGroup{}
	for _, b := range backends {
		if b.Provider != model.DataBackendProviderCloudflareR2 {
			continue
		}
		b, err = s.store.GetBackupBackendForUse(b.ID, "", true)
		if err != nil {
			return nil, err
		}
		key := backupUsageStorageNamespaceKey(model.BackupBackendAsDataBackend(b))
		if groups[key] == nil {
			groups[key] = &backupInventoryGroup{}
		}
		groups[key].backends = append(groups[key].backends, b)
	}
	for _, g := range groups {
		g.key = backupInventoryKey(g.backends)
	}
	return groups, nil
}

func (s *Server) StartBackgroundBackupInventory(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := s.advanceBackupInventories(ctx); err != nil && ctx.Err() == nil && s.log != nil {
			s.log.Printf("backup inventory checkpoint refresh failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (s *Server) advanceBackupInventories(ctx context.Context) error {
	groups, err := s.backupInventoryGroups()
	if err != nil {
		return err
	}
	for _, g := range groups {
		if err = s.advanceBackupInventory(ctx, g); err != nil {
			return err
		}
	}
	return nil
}
func (s *Server) advanceBackupInventory(parent context.Context, g *backupInventoryGroup) error {
	cfg := s.backupInventoryConfig.normalized()
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(parent, cfg.PageTimeout+5*time.Second)
	defer cancel()
	_, err := s.store.UpdateObservationCheckpoint(ctx, g.key, func(raw []byte) ([]byte, error) {
		cp := backupInventoryCheckpoint{Version: 1}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &cp); err != nil {
				return nil, fmt.Errorf("decode backup inventory checkpoint: %w", err)
			}
			if cp.Version != 1 {
				return nil, fmt.Errorf("unsupported backup inventory checkpoint")
			}
		}
		if now.Before(cp.NextAttempt) {
			return nil, nil
		}
		if cp.Current == nil && cp.Complete != nil && cp.Complete.FinishedAt != nil && now.Sub(*cp.Complete.FinishedAt) < cfg.RefreshInterval {
			return nil, nil
		}
		backend, err := newDataObjectBackend(model.BackupBackendAsDataBackend(g.backends[0]))
		if err != nil {
			return nil, err
		}
		if !backupUsageR2NamespaceIsMeasurable(backend) {
			return nil, errors.New("backup namespace is not measurable")
		}
		if cp.Current == nil || now.Sub(cp.Current.StartedAt) > cfg.MaxScanAge {
			cp.Current = &backupInventoryScan{Generation: model.NewID("inventory"), StartedAt: now, Wanted: map[string]bool{}, References: map[string]dataObjectInfo{}, Totals: map[string]backupInventoryTotals{}}
			owner := g.backends[0].TenantID
			for _, b := range g.backends {
				if b.TenantID != owner {
					owner = ""
					break
				}
			}
			cp.Current.NamespaceOwner = owner
			artifacts, err := s.store.ListBackupUsageArtifacts("", true)
			if err != nil {
				return nil, err
			}
			ids := map[string]bool{}
			for _, b := range g.backends {
				ids[b.ID] = true
			}
			for _, a := range artifacts {
				if !ids[a.Artifact.BackendID] {
					continue
				}
				keys, err := backupArtifactObjectKeysForDeletion(a.Artifact)
				if err != nil {
					continue
				}
				for _, k := range keys {
					cp.Current.Wanted[backend.objectKey(k)] = true
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
		var response *s3.ListObjectsV2Output
		for _, b := range g.backends {
			candidate, e := newDataObjectBackend(model.BackupBackendAsDataBackend(b))
			if e != nil {
				pageErr = e
				continue
			}
			var token *string
			if scan.Cursor != "" {
				token = aws.String(scan.Cursor)
			}
			response, pageErr = candidate.client.ListObjectsV2(pageCtx, &s3.ListObjectsV2Input{Bucket: aws.String(candidate.backend.Bucket), Prefix: aws.String(candidate.objectKey("")), ContinuationToken: token, MaxKeys: aws.Int32(1000)})
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
			return json.Marshal(cp)
		}
		next := aws.ToString(response.NextContinuationToken)
		if aws.ToBool(response.IsTruncated) && (next == "" || next == scan.Cursor) {
			cp.Error = "invalid_pagination"
			cp.NextAttempt = now.Add(cfg.RetryInterval)
			return json.Marshal(cp)
		}
		objects := make([]dataObjectInfo, 0, len(response.Contents))
		for _, o := range response.Contents {
			objects = append(objects, dataObjectInfo{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), LastModified: aws.ToTime(o.LastModified), ObservedAt: now})
		}
		sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
		for _, o := range objects {
			if o.Key <= scan.LastKey {
				continue
			}
			scan.LastKey = o.Key
			scan.Objects++
			if !backupUsagePhysicalKeyInNamespace(backend.backend, o.Key) {
				continue
			}
			owner := ""
			logical, ok := backend.logicalObjectKey(o.Key)
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
				if backupUsageObjectWithinCleanupGrace(o.LastModified, o.ObservedAt) {
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
		scan.Cursor = next
		cp.Error = ""
		cp.NextAttempt = time.Time{}
		if !aws.ToBool(response.IsTruncated) {
			finished := time.Now().UTC()
			scan.FinishedAt = &finished
			scan.Cursor = ""
			cp.Complete = scan
			cp.Current = nil
		}
		return json.Marshal(cp)
	})
	return err
}

func (s *Server) readBackupInventory(ctx context.Context, groupKey string) (backupInventoryCheckpoint, error) {
	groups, err := s.backupInventoryGroups()
	if err != nil {
		return backupInventoryCheckpoint{}, err
	}
	g := groups[groupKey]
	if g == nil {
		return backupInventoryCheckpoint{}, errors.New("backup namespace no longer configured")
	}
	raw, err := s.store.ReadObservationCheckpoint(ctx, g.key)
	if err != nil {
		return backupInventoryCheckpoint{}, err
	}
	var cp backupInventoryCheckpoint
	if len(raw) > 0 {
		err = json.Unmarshal(raw, &cp)
	}
	return cp, err
}
