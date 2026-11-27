package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	"github.com/jackc/pgx/v5/stdlib"
	"k8s.io/client-go/rest"
)

type nodeWorkerShadowReport = productioncontrol.FrontWorkerShadow
type nodeWorkerRollbackObservation = productioncontrol.FrontWorkerRollbackObservation

func runNodeWorkerShadow(ctx context.Context, args []string, in io.Reader, out io.Writer) error {
	if len(args) != 0 || in == nil || out == nil {
		return errors.New("node-worker-shadow requires one canonical stdin journal")
	}
	raw, err := readClaimStdin(in)
	if err != nil {
		return err
	}
	journal, err := edgegroupfront.ReadNodeWorkerJournal(raw)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	report, err := observeNodeWorkerShadow(ctx, journal, true)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

func runApprovedNodeWorkerShadow(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 3 || out == nil {
		return errors.New("usage: fugue-release-guardian node-worker-approved-shadow JOURNAL_CONFIG INTENT_DIGEST APPROVAL_DIGEST")
	}
	cfg, keys, cluster, err := productioncontrol.LoadJournalEndpoint(args[0])
	if err != nil {
		return err
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = make(map[string]string)
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	db.SetMaxOpenConns(1)
	j, err := productioncontrol.NewJournal(db, keys)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	approved, err := j.ReadApprovedFrontWorker(ctx, cluster, args[1], args[2])
	if err != nil {
		return err
	}
	fresh, err := observeNodeWorkerShadow(ctx, approved.Journal, false)
	if err != nil {
		return err
	}
	if err := productioncontrol.MatchFreshFrontWorkerShadow(approved, fresh); err != nil {
		return err
	}
	if _, err := j.ReadApprovedFrontWorker(ctx, cluster, args[1], args[2]); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(fresh)
}

func observeNodeWorkerShadow(ctx context.Context, journal edgegroupfront.NodeWorkerJournal, requireRecentJournal bool) (nodeWorkerShadowReport, error) {
	config, err := loadKubeConfig()
	if err != nil {
		return nodeWorkerShadowReport{}, err
	}
	reader := rest.CopyConfig(config)
	reader.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return nodeWorkerShadowObjects{next: next}
	})
	targets, store, front, err := loadNodeWorkerAuthority(reader, config, journal.Recipient.Group)
	if err != nil {
		return nodeWorkerShadowReport{}, err
	}
	component, err := store.FindNodeWorkerComponent(ctx, targets, journal.Recipient.Group)
	if err != nil {
		return nodeWorkerShadowReport{}, err
	}
	return front.shadowNodeWorker(ctx, store, component, journal, requireRecentJournal)
}

func (a *frontAuthorityActivator) shadowNodeWorker(ctx context.Context, store *releaseguardian.AuthorityStore,
	component releaseguardian.NodeWorkerComponent, journal edgegroupfront.NodeWorkerJournal, requireRecentJournal bool) (nodeWorkerShadowReport, error) {
	if requireRecentJournal && !a.recentNodeWorkerJournal(journal) {
		return nodeWorkerShadowReport{}, errors.New("node Worker shadow requires a recent journal")
	}
	selection, err := journal.SelectionPlan()
	if err != nil {
		return nodeWorkerShadowReport{}, err
	}
	if err := store.CheckNodeWorkerShadowFence(ctx, journal.Recipient.Group); err != nil {
		return nodeWorkerShadowReport{}, err
	}
	observer := *a
	observer.executor = nodeWorkerShadowExec{next: a.executor, namespace: a.config.Namespace}
	preflight, err := observer.preflightNodeWorker(ctx, store, component, journal)
	if err != nil {
		return nodeWorkerShadowReport{}, err
	}
	rollback, err := observer.observeNodeWorkerRollback(ctx, journal, preflight)
	if err != nil {
		return nodeWorkerShadowReport{}, err
	}
	retirement := preflight.retirement
	freshRetirement, err := observer.loadWorkerRetirement(ctx, journal)
	if err != nil || (retirement == nil) != (freshRetirement == nil) ||
		(retirement != nil && *retirement != *freshRetirement) {
		return nodeWorkerShadowReport{}, errors.Join(errors.New("node Worker retirement changed during shadow observation"), err)
	}
	if err := store.CheckNodeWorkerShadowFence(ctx, journal.Recipient.Group); err != nil {
		return nodeWorkerShadowReport{}, err
	}
	if requireRecentJournal && !a.recentNodeWorkerJournal(journal) {
		return nodeWorkerShadowReport{}, errors.New("node Worker journal expired during shadow observation")
	}
	var retirementObservation *productioncontrol.FrontWorkerRetirement
	if retirement != nil {
		retirementObservation = &productioncontrol.FrontWorkerRetirement{
			Schema: retirement.Schema, JournalDigest: retirement.JournalDigest, DaemonSet: retirement.DaemonSet,
			DaemonSetUID: string(retirement.DaemonSetUID), Generation: retirement.Generation,
		}
	}
	return nodeWorkerShadowReport{
		Schema: productioncontrol.FrontWorkerShadowSchema, Namespace: a.config.Namespace,
		GroupFence: releaseguardian.FrontRepairFenceName(journal.Recipient.Group), Component: component.Component,
		LeaseName: component.LeaseName, Journal: journal, Selection: selection,
		Rollback: rollback, Retirement: retirementObservation, ObservedAt: a.now().UTC(),
	}, nil
}

func (a *frontAuthorityActivator) observeNodeWorkerRollback(ctx context.Context, j edgegroupfront.NodeWorkerJournal,
	preflight nodeWorkerPreflight) (nodeWorkerRollbackObservation, error) {
	if strings.TrimSpace(a.config.RouteHost) == "" || !strings.HasPrefix(a.config.RoutePath, "/") ||
		!exactSHA256Digest(a.config.RouteBodyDigest) {
		return nodeWorkerRollbackObservation{}, errors.New("node Worker rollback observation requires the configured TLS route canary")
	}
	prior := preflight.runtime.prior
	// Use the same signed-publication and TLS checks as Guardian's rollback.
	if err := a.verifyLocalWorkerServing(ctx, j, prior, j.Expected.BundleGeneration); err != nil {
		return nodeWorkerRollbackObservation{}, errors.Join(errors.New("node Worker shadow could not prove the retained listener can serve"), err)
	}
	if err := a.requireSameNodeWorkerRuntime(ctx, j, preflight.activation, preflight.runtime, true); err != nil {
		return nodeWorkerRollbackObservation{}, err
	}
	return nodeWorkerRollbackObservation{
		RetainedWorkerUID: string(prior.pod.UID), RetainedTarget: prior.target, CanaryBodyDigest: a.config.RouteBodyDigest,
		CanaryHost: a.config.RouteHost, CanaryPath: a.config.RoutePath,
	}, nil
}

// Separate the GET-only object client from the two explicitly permitted Pod
// observations. Kubernetes uses POST to establish those exec streams.
type nodeWorkerShadowObjects struct{ next http.RoundTripper }

func (c nodeWorkerShadowObjects) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || req.Header.Get("Upgrade") != "" {
		return nil, errors.New("node Worker shadow only accepts plain Kubernetes GET requests")
	}
	return c.next.RoundTrip(req)
}

type nodeWorkerShadowExec struct {
	next      podCommandExecutor
	namespace string
}

func (e nodeWorkerShadowExec) Exec(ctx context.Context, namespace, pod, container string, command ...string) ([]byte, error) {
	if e.next == nil || namespace != e.namespace || pod == "" || container != "edge-front" || len(command) != 2 {
		return nil, errors.New("node Worker shadow exec is outside its observation scope")
	}
	switch {
	case command[0] == frontActivationCASBinary && command[1] == "--worker-recipient":
	case command[0] == "cat" && command[1] == edgegroupfront.NativeStatePath:
	default:
		return nil, errors.New("node Worker shadow only accepts the Front recipient and activation reads")
	}
	return e.next.Exec(ctx, namespace, pod, container, command...)
}
