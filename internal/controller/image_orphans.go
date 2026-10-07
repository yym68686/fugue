package controller

import (
	"context"
	"fugue/internal/imagecachepolicy"
	"fugue/internal/model"
	"time"
)

func (s *Service) reconcileImageOrphans(ctx context.Context) error {
	now := time.Now().UTC()
	p, previous, coverage, reason, err := s.Store.ImageOrphanContext(now)
	if err != nil {
		return err
	}
	manifests, err := s.Store.ListImageCacheManifests(model.ImageCacheManifestFilter{IncludeIncomplete: true})
	if err != nil {
		return err
	}
	present := make([]model.ImageCacheManifest, 0, len(manifests))
	absent := []model.ImageOrphanDecision{}
	for _, m := range manifests {
		if m.Present {
			present = append(present, m)
		} else if d := previous[imagecachepolicy.OrphanKey(m)]; d.ID != "" && coverage {
			d.State = "absent"
			d.Reason = "absent_from_complete_inventory"
			absent = append(absent, d)
		}
	}
	manifests = present
	protected, err := s.controllerImageCacheProtectedSet(ctx)
	if err != nil {
		return err
	}
	// Classify without old decisions so revoked protection cannot be hidden by
	// the previous retirement authorization. Propagate aliases and parent graphs.
	protected.orphanCoverage = false
	candidates := make([]model.ImageCachePruneCandidate, 0, len(manifests))
	for _, m := range manifests {
		candidates = append(candidates, s.controllerImageCacheCandidate(m, protected, now))
	}
	// Unknown parents do not prove use. Observe the entire complete orphan
	// graph together, while real protections still propagate through aliases and
	// children. This temporary classification grants no deletion authority.
	observedMissing := make([]bool, len(candidates))
	for i := range candidates {
		if !candidates[i].Protected && candidates[i].Reason == "missing_control_plane_image" {
			observedMissing[i] = true
			candidates[i].Reason = "orphan_retirement"
		}
	}
	candidates = imagecachepolicy.Finalize(manifests, candidates, model.ImageCachePruneModeObserve)
	for i := range candidates {
		if observedMissing[i] && !candidates[i].Protected {
			candidates[i].Reason = "missing_control_plane_image"
		}
	}

	updates := absent
	for i, m := range manifests {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := imagecachepolicy.OrphanKey(m)
		old := previous[key]
		c := candidates[i]
		if old.ID == "" && (c.Reason != "missing_control_plane_image" || !imagecachepolicy.OrphanScope(p, m)) {
			continue
		}
		// A pending prune protects its own targets. Claim-time revalidation excludes
		// that exact task and still checks all other concurrent work.
		if coverage && old.State == "retirement_authorized" && c.SkipReason == "active_task" {
			continue
		}
		d := imagecachepolicy.ObserveOrphan(p, old, m, c, coverage, now)
		if !coverage {
			d.Reason = reason
		}
		updates = append(updates, d)
	}
	return s.Store.SaveImageOrphanDecisions(updates)
}

// Read scheduling policy independently from the binary release. Turning retire
// off takes effect without rolling or restarting the controller.
func (s *Service) runScheduledImageCacheMaintenance(ctx context.Context, last *time.Time) error {
	interval := s.Config.ImageCacheInventoryInterval
	p, err := s.Store.GetImageOrphanPolicy()
	if err != nil {
		return err
	}
	if p.Generation > 0 && p.SweepIntervalSeconds >= 60 {
		interval = min(interval, time.Duration(p.SweepIntervalSeconds)*time.Second)
	}
	now := time.Now()
	if !last.IsZero() && now.Sub(*last) < interval {
		return nil
	}
	*last = now
	return s.runImageCacheStorageMaintenance(ctx)
}
