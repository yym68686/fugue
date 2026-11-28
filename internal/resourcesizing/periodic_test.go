package resourcesizing

import (
	"errors"
	"slices"
	"testing"

	"fugue/internal/model"
)

func TestPeriodicSweepFiltersPolicyAndRuntimeUnderLock(t *testing.T) {
	automatic := model.AppRightSizingSpec{Mode: model.AppRightSizingModeAuto, WindowHours: 24, MinSamples: 3}
	observe := model.AppRightSizingSpec{Mode: model.AppRightSizingModeRecommend}
	apps := []model.App{
		{ID: "default"},
		{ID: "observed", Spec: model.AppSpec{RightSizing: &observe}},
		{ID: "external", Spec: model.AppSpec{RightSizing: &automatic}},
		{ID: "custom", Spec: model.AppSpec{RightSizing: &automatic}},
	}
	var applied []string
	held := false
	runPass(PeriodicPorts{
		WithLock: func(run func() error) (bool, error) {
			held = true
			defer func() { held = false }()
			return true, run()
		},
		Apps:    func() ([]model.App, error) { return apps, nil },
		Managed: func(app model.App) bool { return app.ID != "external" },
		Apply: func(app model.App, window, samples int) error {
			if !held {
				t.Fatal("applied outside lock")
			}
			applied = append(applied, app.ID)
			if app.ID == "default" {
				if window != 0 || samples != 0 {
					t.Fatal("nil policy must defer to recommendation defaults")
				}
				return errors.New("one app failed")
			}
			if window != 24 || samples != 3 {
				t.Fatalf("custom policy lost: %d hours, %d samples", window, samples)
			}
			return nil
		},
	})
	if !slices.Equal(applied, []string{"default", "custom"}) {
		t.Fatalf("applied to %v", applied)
	}
}

func TestPeriodicSweepDoesNotReadWithoutLock(t *testing.T) {
	runPass(PeriodicPorts{
		WithLock: func(func() error) (bool, error) { return false, nil },
		Apps: func() ([]model.App, error) {
			t.Fatal("listed apps without the scan lock")
			return nil, nil
		},
	})
}
