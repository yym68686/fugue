package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
	"net/http"
	"sort"
	"strings"
	"time"
)

var errWorkspaceRetirementPending = errors.New("workspace retirement is waiting for lifecycle cleanup or reclamation")

type retirementObject struct {
	backend               model.DataBackend
	key, digest, identity string
	size                  int64
}

func (s *Server) retirementObjects(ctx context.Context, ws model.DataWorkspace) ([]retirementObject, error) {
	refs, err := s.store.ListDataRetirementObjects(ctx, ws)
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
	out := []retirementObject{}
	seen := map[string]bool{}
	for _, ref := range refs {
		id := ref.BackendID
		if id == "" {
			id = s.store.DefaultDataBackendID()
		}
		b, ok := byID[id]
		if !ok {
			matches := 0
			for _, v := range backends {
				if v.Name == id || v.Slug == model.Slugify(id) {
					b = v
					matches++
				}
			}
			if matches != 1 {
				return nil, fmt.Errorf("retirement backend unavailable; refusing reclamation")
			}
			byID[id] = b
		}
		logical := ref.ObjectKey
		digest := strings.ToLower(strings.TrimSpace(ref.SHA256))
		if decoded, e := hex.DecodeString(digest); e != nil || len(decoded) != 32 {
			return nil, errors.New("invalid retirement content digest; refusing reclamation")
		}
		if logical == "" {
			logical = model.DataObjectKey(digest)
		}
		if logical != model.DataObjectKey(digest) {
			return nil, fmt.Errorf("retirement object outside content-addressed blob namespace")
		}
		key := ref.SHA256
		identity := "local/" + key
		if !dataBackendSupportsDirectObjectStorage(b) && b.Provider != model.DataBackendProviderFugueManaged {
			return nil, errors.New("backend does not support safe workspace reclamation")
		}
		if dataBackendSupportsDirectObjectStorage(b) {
			key = (&dataObjectBackend{backend: b}).objectKey(logical)
			identity = dataPhysicalBucket(b) + "/" + key
		}
		if seen[identity] {
			continue
		}
		seen[identity] = true
		out = append(out, retirementObject{backend: b, key: key, digest: ref.SHA256, identity: identity, size: ref.Size})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].identity < out[j].identity })
	return out, nil
}

func (s *Server) handleRetireDataWorkspace(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && !p.HasScope("data.delete") {
		httpx.WriteError(w, 403, "missing data.delete scope")
		return
	}
	ws, ok := s.loadAuthorizedDataWorkspaceForRole(w, r, p, model.DataWorkspaceAccessRoleAdmin)
	if !ok {
		return
	}
	var opt model.DataWorkspaceRetirementOptions
	if err := httpx.DecodeJSON(r, &opt); err != nil {
		httpx.WriteError(w, 400, err.Error())
		return
	}
	if opt.Execute {
		if _, err := s.retirementObjects(r.Context(), ws); err != nil {
			httpx.WriteError(w, 409, err.Error())
			return
		}
		job, err := s.store.BeginDataWorkspaceRetirement(ws, opt, p.ActorType, p.ActorID)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		s.appendAudit(p, "data.workspace.retire", "data_workspace", ws.ID, ws.TenantID, map[string]string{"job_id": job.ID, "cancel_planned_transfers": fmt.Sprint(opt.CancelPlannedTransfers), "revoke_grants": fmt.Sprint(opt.RevokeGrants), "evict_caches": fmt.Sprint(opt.EvictCaches)})
		httpx.WriteJSON(w, 202, job)
		return
	}
	blockers, err := s.store.InspectDataDeletion(ws, nil)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	transfers, err := s.store.ListDataTransferSummaries("", ws.ID, true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	dependencies := blockers
	blockers = store.DataRetirementBlockers(blockers, transfers, opt)
	snapshots, err := s.store.ListDataSnapshots(ws.ID, false)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	objects, err := s.retirementObjects(r.Context(), ws)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	refsByBackend := map[string]map[string]bool{}
	var unique, shared int64
	for _, v := range objects {
		refs, ok := refsByBackend[v.backend.ID]
		if !ok {
			refs, err = s.dataNamespaceReferencesExcept(r.Context(), v.backend, ws.ID)
			if err != nil {
				s.writeStoreError(w, err)
				return
			}
			refsByBackend[v.backend.ID] = refs
		}
		if refs[v.key] {
			shared += v.size
		} else {
			unique += v.size
		}
	}
	plan := map[string]any{"workspace_id": ws.ID, "name": ws.Name, "allowed": len(blockers) == 0, "blockers": blockers, "snapshot_count": len(snapshots), "grace_days": 7, "estimated_unique_bytes": unique, "shared_bytes": shared, "estimate_only": true}
	plan["dependencies"] = dependencies
	if job, e := s.store.DataWorkspaceRetirement(ws.ID); e == nil {
		plan["existing_job_id"] = job.ID
	} else if !errors.Is(e, store.ErrNotFound) {
		s.writeStoreError(w, e)
		return
	}
	httpx.WriteJSON(w, 200, plan)
}

func (s *Server) handleGetDataWorkspaceRetirement(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && !p.HasScope("data.delete") {
		httpx.WriteError(w, 403, "missing data.delete scope")
		return
	}
	j, err := s.store.GetStorageMaintenanceJob(r.PathValue("job_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if j.Kind != "workspace-retire" || j.WorkspaceRetirement == nil {
		httpx.WriteError(w, 404, "retirement task not found")
		return
	}
	ret := j.WorkspaceRetirement
	if !p.IsPlatformAdmin() {
		ws, e := s.store.GetDataWorkspaceForPrincipal(j.ScopeID, p.TenantID, p.ActorType, p.ActorID, false)
		if e == nil {
			if !s.principalAllowsDataWorkspaceRole(w, p, ws, model.DataWorkspaceAccessRoleAdmin) {
				return
			}
		} else if !errors.Is(e, store.ErrNotFound) || !canInspectRetiredDataWorkspace(p, ret) {
			httpx.WriteError(w, 404, "retirement task not found")
			return
		}
	}
	httpx.WriteJSON(w, 200, j)
}

func canInspectRetiredDataWorkspace(p model.Principal, ret *model.DataWorkspaceRetirement) bool {
	if p.TenantID == ret.TenantID {
		return p.AllowsProject(ret.ProjectID)
	}
	return p.ActorID != "" && p.ActorID == ret.ActorID && p.ActorType == ret.ActorType
}

// One bounded pass. References are checked again under the same fence as
// writers. Shared objects survive; only manifest-derived identities are touched.
func (s *Server) runWorkspaceRetirement(ctx context.Context, j *model.StorageMaintenanceJob) (map[string]any, error) {
	ret := j.WorkspaceRetirement
	if ret == nil {
		return nil, store.ErrInvalidInput
	}
	result := map[string]any{"physical_reclamation": "confirmed delete acknowledgements only; pre-existing absent objects are not counted"}
	ws, err := s.store.GetDataWorkspace(j.ScopeID, "", true)
	if errors.Is(err, store.ErrNotFound) && ret.ReclaimAfter != nil && !time.Now().Before(*ret.ReclaimAfter) {
		ret.Phase = "completed"
		ret.MetadataDeleted = true
		return result, nil
	}
	if err != nil {
		return result, err
	}
	objects, err := s.retirementObjects(ctx, ws)
	if err != nil {
		return result, err
	}
	h := sha256.New()
	for _, v := range objects {
		fmt.Fprintf(h, "%q:%d\n", v.identity, v.size)
	}
	inventoryDigest := fmt.Sprintf("%x", h.Sum(nil))
	if ret.InventoryDigest != "" && ret.InventoryDigest != inventoryDigest {
		return result, errors.New("retirement inventory or backend location changed; refusing reclamation")
	}
	ret.InventoryDigest = inventoryDigest
	if len(ret.BackendIDs) == 0 {
		seen := map[string]bool{}
		for _, v := range objects {
			if !seen[v.backend.ID] {
				ret.BackendIDs = append(ret.BackendIDs, v.backend.ID)
				seen[v.backend.ID] = true
			}
		}
		if !seen[ws.StorageBackendID] {
			ret.BackendIDs = append(ret.BackendIDs, ws.StorageBackendID)
		}
		sort.Strings(ret.BackendIDs)
	}
	if ret.Phase == "evicting" {
		updated, e := s.store.ReleaseDataWorkspaceRetirement(*j)
		if errors.Is(e, store.ErrConflict) {
			return result, errWorkspaceRetirementPending
		}
		if e != nil {
			return result, e
		}
		*j = updated
		ret = j.WorkspaceRetirement
	}
	if ret.ReclaimAfter == nil {
		return result, store.ErrInvalidInput
	}
	if time.Now().Before(*ret.ReclaimAfter) {
		return result, errWorkspaceRetirementPending
	}
	ret.Phase = "reclaiming"
	if ret.Cursor < 0 || ret.Cursor > len(objects) {
		return result, errors.New("invalid retirement checkpoint")
	}
	end := min(ret.Cursor+10, len(objects))
	clients := map[string]*dataObjectBackend{}
	acquired, err := s.store.WithAdvisoryLock(ctx, store.DataReferenceLock, func() error {
		// Detect unexpected state drift rather than excluding a new live target
		// reference just because this job owns the retirement freeze.
		blockers, e := s.store.InspectDataDeletion(ws, nil)
		if e != nil {
			return e
		}
		for _, b := range blockers {
			if b.Kind != "retirement" {
				return store.ErrConflict
			}
		}
		refsByBackend := map[string]map[string]bool{}
		for ret.Cursor < end {
			if e := ctx.Err(); e != nil {
				return e
			}
			v := objects[ret.Cursor]
			refs, ok := refsByBackend[v.backend.ID]
			if !ok {
				refs, e = s.dataNamespaceReferencesExcept(ctx, v.backend, ws.ID)
				if e != nil {
					return e
				}
				refsByBackend[v.backend.ID] = refs
			}
			if refs[v.key] {
				ret.SharedIndices = append(ret.SharedIndices, ret.Cursor)
				ret.Cursor++
				continue
			}
			if dataBackendSupportsDirectObjectStorage(v.backend) {
				client := clients[v.backend.ID]
				if client == nil {
					b, e := s.store.GetDataBackendForUse(v.backend.ID, "", true)
					if e != nil {
						return e
					}
					client, e = newDataObjectBackend(b)
					if e != nil {
						return e
					}
					clients[b.ID] = client
				}
				logical, ok := client.logicalObjectKey(v.key)
				if !ok {
					return errors.New("retirement object outside backend namespace")
				}
				before, exists, e := client.headObjectInfo(ctx, logical)
				if e != nil {
					return e
				}
				if !exists {
					ret.Cursor++
					continue
				}
				if before.LastModified.IsZero() {
					return errors.New("retirement object timestamp unavailable")
				}
				if after := before.LastModified.Add(store.DataRetirementGrace); after.After(time.Now()) {
					ret.ReclaimAfter = &after
					ret.Phase = "grace"
					return errWorkspaceRetirementPending
				}
				if before.Size != v.size {
					return errors.New("retirement blob size differs from immutable manifest; refusing deletion")
				}
				current, stillExists, e := client.headObjectInfo(ctx, logical)
				if e != nil {
					return e
				}
				if !stillExists {
					ret.Cursor++
					continue
				}
				if before.ETag == "" || !dataGCObjectIdentityMatches(before, current) {
					return errors.New("retirement object identity changed before deletion; refusing reclamation")
				}
				if e = client.deleteObjects(ctx, []string{v.key}); e != nil {
					return e
				}
				_, exists, e = client.headObjectInfo(ctx, logical)
				if e != nil {
					return e
				}
				if exists {
					return errors.New("retirement delete not yet confirmed absent")
				}
				ret.DeletedObjects++
				ret.DeletedBytes += before.Size
			} else {
				if s.store.DataBlobExists(v.digest) {
					if e = s.store.DeleteDataBlobDigest(v.digest); e != nil {
						return e
					}
					ret.DeletedObjects++
					ret.DeletedBytes += v.size
				}
			}
			ret.Cursor++
		}
		if ret.Cursor == len(objects) {
			// Another retiring workspace may have released its metadata since a
			// previous batch. Revisit formerly shared objects before the last
			// workspace can disappear and strand their namespace.
			for _, index := range ret.SharedIndices {
				if index < 0 || index >= len(objects) {
					return errors.New("invalid shared-object checkpoint")
				}
				v := objects[index]
				refs, ok := refsByBackend[v.backend.ID]
				if !ok {
					refs, e = s.dataNamespaceReferencesExcept(ctx, v.backend, ws.ID)
					if e != nil {
						return e
					}
					refsByBackend[v.backend.ID] = refs
				}
				if !refs[v.key] {
					ret.Cursor = min(ret.Cursor, index)
				}
			}
			if ret.Cursor < len(objects) {
				kept := ret.SharedIndices[:0]
				for _, i := range ret.SharedIndices {
					if i < ret.Cursor {
						kept = append(kept, i)
					}
				}
				ret.SharedIndices = kept
			} else {
				if err := s.store.FinalizeDataWorkspaceRetirementLocked(*j); err != nil {
					return err
				}
				ret.Phase = "completed"
				ret.MetadataDeleted = true
			}
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if !acquired {
		return result, store.ErrConflict
	}
	if ret.Cursor < len(objects) {
		return result, errWorkspaceRetirementPending
	}
	return result, nil
}
