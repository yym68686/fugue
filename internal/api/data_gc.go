package api

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func dataPhysicalBucket(backend model.DataBackend) string {
	endpoint := strings.TrimRight(strings.ToLower(backend.Endpoint), "/")
	if endpoint == "" {
		endpoint = "https://s3.amazonaws.com"
	}
	if u, err := url.Parse(endpoint); err == nil {
		endpoint = strings.ToLower(u.Scheme+"://"+u.Host) + strings.TrimRight(u.EscapedPath(), "/")
	}
	return endpoint + "/" + backend.Bucket
}

// The backend id and provider label are not physical namespace identities.
// Compare bucket locations and physical keys, including overlapping prefixes.
func (s *Server) dataNamespaceReferences(ctx context.Context, backend model.DataBackend) (map[string]bool, error) {
	refs, err := s.store.ListDataGCReferences(ctx)
	if err != nil {
		return nil, err
	}
	backends, err := s.store.ListDataBackends("", true)
	if err != nil {
		return nil, err
	}
	byID := map[string]model.DataBackend{}
	for _, b := range backends {
		byID[b.ID] = b
	}
	out := map[string]bool{}
	for _, ref := range refs {
		b, ok := byID[ref.BackendID]
		if !ok {
			return nil, errors.New("reference backend is unavailable; refusing data GC")
		}
		digest := strings.ToLower(strings.TrimSpace(ref.SHA256))
		if !dataBackendSupportsDirectObjectStorage(backend) {
			if !dataBackendSupportsDirectObjectStorage(b) {
				out[digest] = true
			}
			continue
		}
		if !dataBackendSupportsDirectObjectStorage(b) || dataPhysicalBucket(b) != dataPhysicalBucket(backend) {
			continue
		}
		key := ref.ObjectKey
		if key == "" {
			key = model.DataObjectKey(digest)
		}
		// objectKey only needs prefix metadata; avoid constructing SDK clients.
		objects := &dataObjectBackend{backend: b}
		out[objects.objectKey(key)] = true
	}
	return out, nil
}

func (s *Server) sweepDataNamespaceGC(ctx context.Context, workspace model.DataWorkspace, days int, dryRun bool) (model.DataGCSweepResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if days <= 0 {
		days = 7
	}
	cutoff := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)
	backend, err := s.store.GetDataBackendForUse(workspace.StorageBackendID, workspace.TenantID, true)
	if err != nil {
		return model.DataGCSweepResult{}, err
	}
	result := model.DataGCSweepResult{WorkspaceID: workspace.ID, BackendID: backend.ID, DryRun: dryRun, RetentionDays: days, Cutoff: cutoff, Candidates: []model.DataGCSweepCandidate{}}
	// Include completed migration sources; deduplicate physical namespaces.
	targets := []model.DataBackend{backend}
	sources, err := s.store.ListDataMigrationSourceIDs(ctx, workspace.ID)
	if err != nil {
		return result, err
	}
	for _, source := range sources {
		b, e := s.store.GetDataBackendForUse(source, workspace.TenantID, true)
		if e != nil {
			return result, e
		}
		targets = append(targets, b)
	}
	seen := map[string]bool{}
	for _, b := range targets {
		direct := dataBackendSupportsDirectObjectStorage(b)
		identity := "local"
		if direct {
			identity = dataPhysicalBucket(b) + "/" + strings.Trim(b.Prefix, "/")
		}
		if seen[identity] {
			continue
		}
		seen[identity] = true
		var objects *dataObjectBackend
		inventory := []dataObjectInfo{}
		if direct {
			objects, err = newDataObjectBackend(b)
			if err != nil {
				return result, err
			}
			inventory, err = objects.listObjects(ctx, "blobs/sha256/")
		} else {
			var blobs []model.DataGCSweepCandidate
			blobs, err = s.store.ListDataBlobDigests()
			for _, v := range blobs {
				inventory = append(inventory, dataObjectInfo{Key: v.Key, Size: v.Size, LastModified: v.LastModified})
			}
		}
		if err != nil {
			return result, err
		}
		result.Scanned += len(inventory)
		refs, e := s.dataNamespaceReferences(ctx, b)
		if e != nil {
			return result, e
		}
		candidates := inventory[:0]
		for _, v := range inventory {
			if !refs[v.Key] && !v.LastModified.IsZero() && !v.LastModified.After(cutoff) {
				candidates = append(candidates, v)
			}
		}
		inventory = candidates
		// Inventory may be slow. Fence reference mutations only during the final
		// reference query and deletion, with a bounded number of objects per lock.
		for start := 0; start < len(inventory); start += 100 {
			end := start + 100
			if end > len(inventory) {
				end = len(inventory)
			}
			acquired, e := s.store.WithAdvisoryLock(ctx, store.DataReferenceLock, func() error {
				refs, e := s.dataNamespaceReferences(ctx, b)
				if e != nil {
					return e
				}
				for _, v := range inventory[start:end] {
					if refs[v.Key] || v.LastModified.IsZero() || v.LastModified.After(cutoff) {
						continue
					}
					reason := "unreferenced"
					if b.ID != backend.ID {
						reason = "old-backend-unreferenced"
					}
					candidate := model.DataGCSweepCandidate{Key: v.Key, Size: v.Size, LastModified: v.LastModified, Reason: reason}
					if !dryRun {
						if objects != nil {
							logical, ok := objects.logicalObjectKey(v.Key)
							if !ok {
								return errors.New("GC object outside backend namespace")
							}
							current, exists, e := objects.headObjectInfo(ctx, logical)
							if e != nil {
								return e
							}
							if !exists || !current.LastModified.Equal(v.LastModified) || current.Size != v.Size || (v.ETag != "" && v.ETag != current.ETag) {
								continue
							}
							if e = objects.deleteObjects(ctx, []string{v.Key}); e != nil {
								return e
							}
						} else if e := s.store.DeleteDataBlobDigest(v.Key); e != nil {
							return e
						}
						result.Deleted++
					}
					result.Candidates = append(result.Candidates, candidate)
					result.DeletedBytes += v.Size
				}
				return nil
			})
			if e != nil {
				return result, e
			}
			if !acquired {
				return result, store.ErrConflict
			}
		}
	}
	return result, nil
}
