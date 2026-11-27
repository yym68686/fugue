package imagebilling

import (
	"context"
	"errors"
	"testing"

	"fugue/internal/model"
)

type billingFixture struct {
	reads   int
	writes  int
	last    int64
	readErr error
}

func (r *billingFixture) Apps() ([]model.App, error) {
	r.reads++
	return []model.App{
		{ID: "first", Source: &model.AppSource{ResolvedImageRef: "registry.example/team/app:1"}},
		{ID: "second", Source: &model.AppSource{ResolvedImageRef: "registry.example/team/app:2"}},
	}, nil
}

func (r *billingFixture) RetentionOperations() ([]model.Operation, error) {
	r.reads++
	return nil, r.readErr
}

func (r *billingFixture) CommitStorage(gibibytes int64) error {
	r.writes++
	r.last = gibibytes
	return nil
}

func TestRefreshDeduplicatesLayersAndCommitsExactlyOnce(t *testing.T) {
	r := &billingFixture{}
	inspect := func(_ context.Context, ref string) (bool, map[string]int64, error) {
		return true, map[string]int64{"shared": 1 << 30, ref: 1}, nil
	}
	if err := Refresh(context.Background(), r, inspect, "registry.example", "registry.example"); err != nil {
		t.Fatal(err)
	}
	if r.reads != 2 || r.writes != 1 || r.last != 2 {
		t.Fatalf("want two reads and a single rounded 2 GiB write, got reads=%d writes=%d size=%d", r.reads, r.writes, r.last)
	}
}

func TestRefreshLeavesBillingUntouchedWhenInventoryIsIncomplete(t *testing.T) {
	failure := errors.New("unavailable")
	for _, stage := range []string{"retention", "registry"} {
		t.Run(stage, func(t *testing.T) {
			r := &billingFixture{last: 11}
			if stage == "retention" {
				r.readErr = failure
			}
			inspect := func(context.Context, string) (bool, map[string]int64, error) {
				if stage == "retention" {
					t.Fatal("registry called after incomplete operation inventory")
				}
				return false, nil, failure
			}
			err := Refresh(context.Background(), r, inspect, "registry.example", "registry.example")
			if !errors.Is(err, failure) || r.writes != 0 || r.last != 11 {
				t.Fatalf("refresh used partial inventory: err=%v writes=%d size=%d", err, r.writes, r.last)
			}
		})
	}
}
