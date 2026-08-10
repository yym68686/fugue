package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
)

type monitorOutput struct {
	APIVersion          string                              `json:"apiVersion"`
	Kind                string                              `json:"kind"`
	Component           string                              `json:"component"`
	ConfigSHA           string                              `json:"configSha"`
	RecordDigest        string                              `json:"recordDigest"`
	Status              string                              `json:"status"`
	Reason              string                              `json:"reason"`
	ConsecutiveFailures int                                 `json:"consecutiveFailures"`
	Rollback            *declarativerelease.ExecutionResult `json:"rollback,omitempty"`
}

func runInstallMonitorRecord(args []string, output io.Writer) error {
	if len(args) != 3 {
		return errors.New("usage: fugue-declarative-release install-monitor-record PLAN_DIR TERMINAL_RESULT")
	}
	files, err := readPlanDirectory(args[1])
	if err != nil {
		return err
	}
	terminalRaw, err := os.ReadFile(args[2])
	if err != nil {
		return err
	}
	bundle, err := decodeMonitorBundle(files, terminalRaw)
	if err != nil {
		return err
	}
	release, err := selectedRelease(bundle.Plan, bundle.Record.Component)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lease, err := newComponentLeaseCoordinator()
	if err != nil {
		return err
	}
	held, err := lease.acquire(ctx, release, bundle.Record.ConfigSHA)
	if err != nil {
		return err
	}
	store, err := newMonitorStore()
	if err == nil {
		_, err = store.persistVerified(ctx, release, files, bundle.Terminal)
	}
	releaseErr := lease.release(ctx, held)
	if err != nil {
		return err
	}
	if releaseErr != nil {
		return releaseErr
	}
	raw, err := declarativerelease.CanonicalJSON(bundle.Record)
	if err != nil {
		return err
	}
	_, err = output.Write(append(raw, '\n'))
	return err
}

func runEmitMonitorOutput(args []string) error {
	if len(args) != 3 {
		return errors.New("usage: fugue-declarative-release emit-monitor-output REGISTRY GITHUB_OUTPUT")
	}
	registry, err := loadProductionRegistry(args[1])
	if err != nil {
		return err
	}
	type item struct {
		Component   string `json:"component"`
		Concurrency string `json:"concurrency"`
	}
	items := make([]item, 0)
	for _, component := range registry.Components {
		if monitorComponent(component) {
			items = append(items, item{Component: component.ID, Concurrency: component.Concurrency})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Component < items[j].Component })
	matrix, err := json.Marshal(map[string]any{"include": items})
	if err != nil {
		return err
	}
	info, err := os.Lstat(args[2])
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 1<<20 {
		return errors.New("GITHUB_OUTPUT file is invalid")
	}
	file, err := os.OpenFile(args[2], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintf(file, "monitor_count=%d\nmonitor_matrix=%s\n", len(items), matrix)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func runMonitor(args []string, output io.Writer) error {
	if len(args) != 3 {
		return errors.New("usage: fugue-declarative-release monitor REGISTRY COMPONENT")
	}
	registry, err := loadProductionRegistry(args[1])
	if err != nil {
		return err
	}
	component, err := registryComponent(registry, args[2])
	if err != nil {
		return err
	}
	if !monitorComponent(component) {
		return errors.New("component is not enrolled in continuous rollback")
	}
	store, err := newMonitorStore()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	snapshot, err := store.load(ctx, component.Workload.Namespace, component.ID)
	if err != nil {
		return err
	}
	recorded, err := selectedRelease(snapshot.Bundle.Plan, component.ID)
	if err != nil {
		return err
	}
	healthRelease, err := monitorHealthRelease(recorded, component)
	if err != nil {
		return err
	}
	cluster, err := newKubectlCluster()
	if err != nil {
		return err
	}
	if snapshot.State.RollbackStatus == "lkg-restored" {
		return verifyMonitoredLKG(ctx, output, store, snapshot, cluster, healthRelease)
	}
	observation, healthErr := cluster.CheckHealthyOnce(ctx, healthRelease, snapshot.Bundle.Prepared.Forward, snapshot.Bundle.Forward)
	if healthErr != nil && observation.ConfigSHA != "" && observation.ConfigSHA != snapshot.Bundle.Record.ConfigSHA {
		return errors.New("monitor record is stale relative to the live production config SHA")
	}
	localRelease := healthRelease
	localRelease.Health = filterPublicRouteProbes(healthRelease.Health)
	_, localErr := cluster.CheckHealthyOnce(ctx, localRelease, snapshot.Bundle.Prepared.Forward, snapshot.Bundle.Forward)
	healthy := healthErr == nil
	next, threshold, err := declarativerelease.NewMonitorObservationState(snapshot.Bundle.Record, snapshot.State, healthy, localErr != nil, errorString(healthErr), time.Now())
	if err != nil {
		return err
	}
	snapshot, err = store.updateState(ctx, snapshot, next)
	if err != nil {
		return err
	}
	if healthy {
		return writeMonitorOutput(output, monitorOutput{APIVersion: "release.fugue.dev/v1", Kind: "DeclarativeComponentMonitorResult", Component: component.ID, ConfigSHA: snapshot.Bundle.Record.ConfigSHA, RecordDigest: snapshot.Bundle.Record.RecordDigest, Status: "healthy", Reason: "all component and public route probes passed"})
	}
	// A public-route-only failure is important evidence but is not sufficient
	// attribution to roll back API, Control, and Worker simultaneously. The
	// component-local health/authority contract must fail in the same check.
	if localErr == nil {
		return writeMonitorOutput(output, monitorOutput{APIVersion: "release.fugue.dev/v1", Kind: "DeclarativeComponentMonitorResult", Component: component.ID, ConfigSHA: snapshot.Bundle.Record.ConfigSHA, RecordDigest: snapshot.Bundle.Record.RecordDigest, Status: "degraded", Reason: "public route canary failed without component-local causal evidence", ConsecutiveFailures: next.ConsecutiveFailures})
	}
	if !threshold {
		return writeMonitorOutput(output, monitorOutput{APIVersion: "release.fugue.dev/v1", Kind: "DeclarativeComponentMonitorResult", Component: component.ID, ConfigSHA: snapshot.Bundle.Record.ConfigSHA, RecordDigest: snapshot.Bundle.Record.RecordDigest, Status: "degraded", Reason: errorString(healthErr), ConsecutiveFailures: next.ConsecutiveFailures})
	}
	return rollbackMonitoredComponent(ctx, output, store, snapshot, cluster, healthRelease)
}

func rollbackMonitoredComponent(ctx context.Context, output io.Writer, store *monitorStore, snapshot monitorSnapshot, cluster *kubectlCluster, healthRelease declarativerelease.PlanRelease) error {
	lease, err := newComponentLeaseCoordinator()
	if err != nil {
		return err
	}
	held, err := lease.acquire(ctx, healthRelease, snapshot.Bundle.Record.ConfigSHA)
	if err != nil {
		return err
	}
	fresh, loadErr := store.load(ctx, snapshot.Namespace, snapshot.Bundle.Record.Component)
	if loadErr != nil || fresh.Bundle.Record.RecordDigest != snapshot.Bundle.Record.RecordDigest || fresh.State.ConsecutiveFailures < declarativerelease.MonitorFailureThreshold {
		_ = lease.release(ctx, held)
		return errors.New("component monitor state changed before rollback")
	}
	observation, healthErr := cluster.CheckHealthyOnce(ctx, healthRelease, fresh.Bundle.Prepared.Forward, fresh.Bundle.Forward)
	if healthErr == nil {
		next, _, stateErr := declarativerelease.NewMonitorState(fresh.Bundle.Record, fresh.State, true, "", time.Now())
		if stateErr == nil {
			_, stateErr = store.updateState(ctx, fresh, next)
		}
		releaseErr := lease.release(ctx, held)
		if stateErr != nil {
			return stateErr
		}
		if releaseErr != nil {
			return releaseErr
		}
		return writeMonitorOutput(output, monitorOutput{APIVersion: "release.fugue.dev/v1", Kind: "DeclarativeComponentMonitorResult", Component: fresh.Bundle.Record.Component, ConfigSHA: fresh.Bundle.Record.ConfigSHA, RecordDigest: fresh.Bundle.Record.RecordDigest, Status: "healthy", Reason: "component recovered before rollback CAS"})
	}
	if observation.ConfigSHA != "" && observation.ConfigSHA != fresh.Bundle.Record.ConfigSHA {
		_ = lease.release(ctx, held)
		return errors.New("live component identity changed before rollback")
	}
	rollback := declarativerelease.RestoreMonitoredLKG(ctx, cluster, fresh.Bundle.Plan, fresh.Bundle.Prepared, fresh.Bundle.Forward, fresh.Bundle.LKG, healthRelease)
	next := fresh.State
	next.LastCheckedAt = time.Now().UTC().Format(time.RFC3339Nano)
	next.LastReason = rollback.Reason
	if rollback.Status == "compensated" {
		next.ConsecutiveFailures = 0
		next.RollbackStatus = "lkg-restored"
	}
	updated, stateErr := store.updateState(ctx, fresh, next)
	releaseErr := lease.release(ctx, held)
	if stateErr != nil {
		return stateErr
	}
	if releaseErr != nil {
		return releaseErr
	}
	result := monitorOutput{APIVersion: "release.fugue.dev/v1", Kind: "DeclarativeComponentMonitorResult", Component: updated.Bundle.Record.Component, ConfigSHA: updated.Bundle.Record.ConfigSHA, RecordDigest: updated.Bundle.Record.RecordDigest, Status: rollback.Status, Reason: rollback.Reason, Rollback: &rollback}
	if err := writeMonitorOutput(output, result); err != nil {
		return err
	}
	if rollback.Status != "compensated" {
		return fmt.Errorf("continuous component rollback ended with status=%s reason=%s", rollback.Status, rollback.Reason)
	}
	return nil
}

func verifyMonitoredLKG(ctx context.Context, output io.Writer, store *monitorStore, snapshot monitorSnapshot, cluster *kubectlCluster, healthRelease declarativerelease.PlanRelease) error {
	if !snapshot.Bundle.Prepared.LKG.Present {
		return errors.New("monitor state claims rollback for a component without a present LKG")
	}
	_, err := cluster.CheckHealthyOnce(ctx, healthRelease, snapshot.Bundle.Prepared.LKG, snapshot.Bundle.LKG)
	if err != nil {
		return fmt.Errorf("verify continuously restored LKG: %w", err)
	}
	next := snapshot.State
	next.ConsecutiveFailures = 0
	next.LastReason = "continuous rollback LKG remains healthy"
	next.LastCheckedAt = time.Now().UTC().Format(time.RFC3339Nano)
	next.LastHealthyAt = next.LastCheckedAt
	if _, err := store.updateState(ctx, snapshot, next); err != nil {
		return err
	}
	return writeMonitorOutput(output, monitorOutput{APIVersion: "release.fugue.dev/v1", Kind: "DeclarativeComponentMonitorResult", Component: snapshot.Bundle.Record.Component, ConfigSHA: snapshot.Bundle.Record.ConfigSHA, RecordDigest: snapshot.Bundle.Record.RecordDigest, Status: "lkg-healthy", Reason: "continuous rollback LKG remains healthy"})
}

func monitorComponent(component declarativerelease.Component) bool {
	if component.Delivery != nil && component.Delivery.Writer == "guardian" {
		return false
	}
	return component.ID == "api" || component.Family == "edge"
}

func registryComponent(registry declarativerelease.Registry, id string) (declarativerelease.Component, error) {
	for _, component := range registry.Components {
		if component.ID == id {
			return component, nil
		}
	}
	return declarativerelease.Component{}, fmt.Errorf("component %q is absent from the production registry", id)
}

func monitorHealthRelease(recorded declarativerelease.PlanRelease, current declarativerelease.Component) (declarativerelease.PlanRelease, error) {
	if recorded.ComponentID != current.ID || recorded.ManifestPath != current.ManifestPath || recorded.Artifact != current.Artifact ||
		recorded.Workload != current.Workload || recorded.Concurrency != current.Concurrency ||
		!reflect.DeepEqual(recorded.ArtifactTargets, current.ArtifactTargets) || !reflect.DeepEqual(recorded.Transition, current.Transition) ||
		!reflect.DeepEqual(recorded.ManifestVariables, current.ManifestVariables) {
		return declarativerelease.PlanRelease{}, errors.New("current monitor contract changes immutable release mechanics relative to the verified record")
	}
	recorded.Health = append([]declarativerelease.HealthProbe(nil), current.Health...)
	return recorded, nil
}

func filterPublicRouteProbes(probes []declarativerelease.HealthProbe) []declarativerelease.HealthProbe {
	result := make([]declarativerelease.HealthProbe, 0, len(probes))
	for _, probe := range probes {
		if probe.Type != "public-route-http" {
			result = append(result, probe)
		}
	}
	return result
}

func writeMonitorOutput(output io.Writer, value monitorOutput) error {
	raw, err := declarativerelease.CanonicalJSON(value)
	if err != nil {
		return err
	}
	_, err = output.Write(append(raw, '\n'))
	return err
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}
