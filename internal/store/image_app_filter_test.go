package store

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestImageAppSetFilterPreservesVisibilityAndOrder(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := s.withLockedState(true, func(state *model.State) error {
		for i, appID := range []string{"a", "b", "c", "foreign"} {
			tenantID := "tenant"
			if appID == "foreign" {
				tenantID = "other"
			}
			updated := now.Add(time.Duration(i) * time.Second)
			state.Images = append(state.Images, model.Image{ID: appID, AppID: appID, TenantID: tenantID, UpdatedAt: updated})
			state.ImageLocations = append(state.ImageLocations, model.ImageLocation{ID: appID, AppID: appID, TenantID: tenantID, Status: model.ImageLocationStatusPresent, UpdatedAt: updated})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	appIDs := []string{" b ", "a", "a", "foreign"}
	images, err := s.ListImages(model.ImageFilter{TenantID: "tenant", AppIDs: appIDs})
	if err != nil {
		t.Fatal(err)
	}
	locations, err := s.ListImageLocations(model.ImageLocationFilter{TenantID: "tenant", AppIDs: appIDs})
	if err != nil {
		t.Fatal(err)
	}
	var imageIDs, locationIDs []string
	for _, image := range images {
		imageIDs = append(imageIDs, image.ID)
	}
	for _, location := range locations {
		locationIDs = append(locationIDs, location.ID)
	}
	if !reflect.DeepEqual(imageIDs, []string{"b", "a"}) || !reflect.DeepEqual(locationIDs, imageIDs) {
		t.Fatalf("visibility/order changed: images=%v locations=%v", imageIDs, locationIDs)
	}
	images, err = s.ListImages(model.ImageFilter{PlatformAdmin: true, AppIDs: appIDs, AppID: "a"})
	if err != nil || len(images) != 1 || images[0].ID != "a" {
		t.Fatalf("filters must intersect: %v %v", images, err)
	}
}

func TestImageAppSetSQLRetainsTenantPredicate(t *testing.T) {
	clauses, args := imageFilterClauses(normalizeImageFilter(model.ImageFilter{TenantID: "tenant", AppIDs: []string{" b ", "a", "b"}}))
	if !reflect.DeepEqual(clauses, []string{"tenant_id = $1", "app_id IN ($2, $3)"}) || !reflect.DeepEqual(args, []any{"tenant", "a", "b"}) {
		t.Fatalf("query predicates changed: %v %v", clauses, args)
	}
}
