package imagetracking

import (
	"slices"

	"fugue/internal/model"
)

// Initialize, DeleteTenant and DeleteApp join the enclosing JSON transaction.
// PostgreSQL deletions use the existing foreign-key cascades.
func (r *Rows) Initialize() {
	if r.Trackings == nil {
		r.Trackings = []model.AppImageTracking{}
	}
	if r.Checks == nil {
		r.Checks = []model.AppImageTrackingCheck{}
	}
}

func (r *Rows) DeleteTenant(id string) {
	r.Trackings = slices.DeleteFunc(r.Trackings, func(row model.AppImageTracking) bool { return row.TenantID == id })
	r.Checks = slices.DeleteFunc(r.Checks, func(row model.AppImageTrackingCheck) bool { return row.TenantID == id })
}

func (r *Rows) DeleteApp(id string) {
	r.Trackings = slices.DeleteFunc(r.Trackings, func(row model.AppImageTracking) bool { return row.AppID == id })
	r.Checks = slices.DeleteFunc(r.Checks, func(row model.AppImageTrackingCheck) bool { return row.AppID == id })
}
