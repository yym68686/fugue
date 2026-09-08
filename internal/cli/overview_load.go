package cli

import (
	"context"
	"errors"
	"fugue/internal/model"
	"strings"
	"sync"
	"time"
)

var errAppOutsideSelection = errors.New("app not found in the selected tenant/project")

type overviewRead struct {
	name  string
	apply func(*appOverviewSnapshot)
	err   error
	empty bool
}

func (c *CLI) loadAppOverviewWithin(client *Client, ref string, budget time.Duration) (appOverviewSnapshot, error) {
	if budget <= 0 {
		budget = 10 * time.Second
	}
	parent := client.context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	scoped := *client
	scoped.context = ctx
	if client.observer != nil {
		var mu sync.Mutex
		scoped.observer = func(r httpObservedRequest) { mu.Lock(); defer mu.Unlock(); client.observer(r) }
	}
	app, err := c.resolveNamedApp(&scoped, ref)
	if err != nil {
		return appOverviewSnapshot{}, err
	}
	app, err = scoped.GetApp(app.ID)
	if err != nil {
		return appOverviewSnapshot{}, err
	}
	jobs := []func() overviewRead{
		func() overviewRead {
			value, err := scoped.ListAppDomains(app.ID)
			return overviewRead{"domains", func(s *appOverviewSnapshot) { s.Domains = value }, err, len(value) == 0}
		},
		func() overviewRead {
			value, err := scoped.ListAppBindings(app.ID)
			return overviewRead{"bindings", func(s *appOverviewSnapshot) { s.Bindings = value.Bindings; s.BackingServices = value.BackingServices }, err, len(value.Bindings) == 0}
		},
		func() overviewRead {
			value, err := scoped.ListOperations(app.ID)
			return overviewRead{"operations", func(s *appOverviewSnapshot) { s.Operations = value }, err, len(value) == 0}
		},
		func() overviewRead {
			value, err := scoped.GetAppImageTracking(app.ID)
			return overviewRead{"image_tracking", func(s *appOverviewSnapshot) { s.ImageTracking = value.Tracking }, err, value.Tracking == nil}
		},
		func() overviewRead {
			value, err := scoped.GetAppImages(app.ID)
			return overviewRead{"images", func(s *appOverviewSnapshot) { s.Images = &value }, err, len(value.Versions) == 0}
		},
		func() overviewRead {
			value, err := scoped.GetAppRuntimePods(app.ID, "app")
			return overviewRead{"pods", func(s *appOverviewSnapshot) { s.PodInventory = &value }, err, len(value.Groups) == 0}
		},
		func() overviewRead {
			value, err := scoped.TryGetAppDiagnosis(app.ID, "app")
			if err == nil && value == nil {
				err = errEvidenceUnavailable
			}
			return overviewRead{"runtime_diagnosis", func(s *appOverviewSnapshot) {
				if value != nil && !strings.EqualFold(value.Category, "available") {
					s.Diagnosis = appDiagnosisToOverviewDiagnosis(value)
				}
			}, err, value == nil}
		},
	}
	work := make(chan int, len(jobs))
	results := make([]overviewRead, len(jobs))
	var workers sync.WaitGroup
	for i := range jobs {
		work <- i
	}
	close(work)
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range work {
				results[index] = jobs[index]()
			}
		}()
	}
	workers.Wait()
	snapshot := appOverviewSnapshot{App: app}
	for _, result := range results {
		snapshot.recordSource(result.name, result.err, result.empty)
		if result.err != nil {
			c.progressf("warning=%s unavailable: %s", result.name, redactDiagnosticString(result.err.Error()))
		} else {
			result.apply(&snapshot)
		}
	}
	runtimeDiagnosis := snapshot.Diagnosis
	diagnosis, err := c.buildAppOverviewDiagnosis(&scoped, snapshot)
	snapshot.recordSource("diagnosis", err, diagnosis == nil)
	if err == nil {
		snapshot.Diagnosis = selectPrimaryOverviewDiagnosis(diagnosis, runtimeDiagnosis)
	}
	return snapshot, nil
}

// Preserve read compatibility while allowing stateful callers to reject fuzzy
// project resolution. IDs always remain an exact targeting escape hatch.
func validateAppSelection(app model.App, tenant, project string) error {
	if (tenant != "" && app.TenantID != tenant) || (project != "" && app.ProjectID != project) {
		return withExitCode(errAppOutsideSelection, ExitCodeNotFound)
	}
	return nil
}
