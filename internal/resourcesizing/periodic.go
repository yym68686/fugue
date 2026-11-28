package resourcesizing

import (
	"context"
	"time"

	"fugue/internal/model"
)

type PeriodicPorts struct {
	WithLock func(func() error) (bool, error)
	Apps     func() ([]model.App, error)
	Managed  func(model.App) bool
	Apply    func(model.App, int, int) error
	Logf     func(string, ...any)
}

// RunPeriodic preserves the scan cadence and lets an admitted pass finish
// before the process closes its Store.
func RunPeriodic(ctx context.Context, ports PeriodicPorts) {
	timer := time.NewTimer(2 * time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			runPass(ports)
			timer.Reset(time.Hour)
		}
	}
}

func runPass(ports PeriodicPorts) {
	logf := ports.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	acquired, err := ports.WithLock(func() error {
		apps, err := ports.Apps()
		if err != nil {
			logf("list apps for right-sizing auto apply failed: %v", err)
			return err
		}
		for _, app := range apps {
			spec := model.AppRightSizingSpec{Mode: model.AppRightSizingModeAuto}
			if app.Spec.RightSizing != nil {
				spec = model.NormalizeAppRightSizingSpec(*app.Spec.RightSizing)
			}
			if spec.Mode != model.AppRightSizingModeAuto || !ports.Managed(app) {
				continue
			}
			if err := ports.Apply(app, spec.WindowHours, spec.MinSamples); err != nil {
				logf("right-sizing auto apply for app=%s failed: %v", app.ID, err)
			}
		}
		return nil
	})
	if err != nil {
		logf("right-sizing auto apply skipped because advisory lock failed: %v", err)
	} else if !acquired {
		logf("right-sizing auto apply skipped because another fugue-api replica holds the advisory lock")
	}
}
