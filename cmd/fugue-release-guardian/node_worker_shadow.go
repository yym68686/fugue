package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	"k8s.io/client-go/rest"
)

const nodeWorkerShadowSchema = "fugue.release-guardian.node-worker-shadow.v1"

type nodeWorkerShadowReport struct {
	Schema     string                                 `json:"schema"`
	Namespace  string                                 `json:"namespace"`
	GroupFence string                                 `json:"group_fence"`
	Component  string                                 `json:"component"`
	LeaseName  string                                 `json:"component_lease"`
	Journal    edgegroupfront.NodeWorkerJournal       `json:"journal"`
	Selection  edgegroupfront.NodeWorkerSelectionPlan `json:"selection"`
	Retirement *workerRetirementPlan                  `json:"retirement,omitempty"`
	ObservedAt time.Time                              `json:"observed_at"`
}

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
	config, err := loadKubeConfig()
	if err != nil {
		return err
	}
	reader := rest.CopyConfig(config)
	reader.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return nodeWorkerShadowObjects{next: next}
	})
	targets, store, front, err := loadNodeWorkerAuthority(reader, config, journal.Recipient.Group)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	component, err := store.FindNodeWorkerComponent(ctx, targets, journal.Recipient.Group)
	if err != nil {
		return err
	}
	report, err := front.shadowNodeWorker(ctx, store, component, journal)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

func (a *frontAuthorityActivator) shadowNodeWorker(ctx context.Context, store *releaseguardian.AuthorityStore,
	component releaseguardian.NodeWorkerComponent, journal edgegroupfront.NodeWorkerJournal) (nodeWorkerShadowReport, error) {
	if !a.recentNodeWorkerJournal(journal) {
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
	retirement, err := observer.preflightNodeWorker(ctx, store, component, journal)
	if err != nil {
		return nodeWorkerShadowReport{}, err
	}
	freshRetirement, err := observer.loadWorkerRetirement(ctx, journal)
	if err != nil || (retirement == nil) != (freshRetirement == nil) ||
		(retirement != nil && *retirement != *freshRetirement) {
		return nodeWorkerShadowReport{}, errors.Join(errors.New("node Worker retirement changed during shadow observation"), err)
	}
	if err := store.CheckNodeWorkerShadowFence(ctx, journal.Recipient.Group); err != nil {
		return nodeWorkerShadowReport{}, err
	}
	if !a.recentNodeWorkerJournal(journal) {
		return nodeWorkerShadowReport{}, errors.New("node Worker journal expired during shadow observation")
	}
	return nodeWorkerShadowReport{
		Schema: nodeWorkerShadowSchema, Namespace: a.config.Namespace,
		GroupFence: releaseguardian.FrontRepairFenceName(journal.Recipient.Group), Component: component.Component,
		LeaseName: component.LeaseName, Journal: journal, Selection: selection, Retirement: retirement, ObservedAt: a.now().UTC(),
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
