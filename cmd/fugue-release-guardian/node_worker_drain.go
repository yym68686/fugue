package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
)

func (a *frontAuthorityActivator) refreshNodeWorkerDrain(ctx context.Context, store *releaseguardian.AuthorityStore,
	fence releaseguardian.NodeWorkerFence, state edgegroupfront.ActivationState) (bool, error) {
	j := fence.Journal
	r, err := a.observeNodeWorkerRuntime(ctx, j, true, true)
	if err != nil {
		return false, err
	}
	if err := a.proveNodeWorkerDrainServing(ctx, j, state, r, false); err != nil {
		return false, err
	}
	grant, err := store.ProjectNodeWorkerDrain(ctx, fence, a.now())
	if err != nil {
		return false, err
	}
	return nodeWorkerDrainComplete(ctx, repairHealthEndpoint(r.front.Status.PodIP, frontHealthPort, "/metrics"), grant)
}

func (a *frontAuthorityActivator) proveNodeWorkerDrainServing(ctx context.Context, j edgegroupfront.NodeWorkerJournal,
	state edgegroupfront.ActivationState, r nodeWorkerRuntime, excluded bool) error {
	if r.probe.UID == "" {
		return errors.New("bounded Worker drain lost the pinned probe transport")
	}
	if err := a.verifyLocalWorkerServing(ctx, j, r.next, j.Candidate.BundleGeneration); err != nil {
		return err
	}
	// New TLS handshakes through both declared listeners. The same physical
	// Front and exact selected Worker binding are verified independently.
	for _, port := range []int{443, int(r.probe.Spec.Ports[0].Port)} {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		status, payload, err := requestPublicRoute(ctx, canaryProbe{
			Address: net.JoinHostPort(r.front.Status.HostIP, strconv.Itoa(port)), Host: a.config.RouteHost, Path: a.config.RoutePath,
		})
		cancel()
		if err != nil || status != http.StatusOK || shaDigest(payload) != a.config.RouteBodyDigest {
			return errors.Join(fmt.Errorf("bounded Worker drain lost serving health on ingress port %d", port), err)
		}
	}
	fresh, err := a.observeNodeWorkerRuntimeWithRetirement(ctx, j, true, true, excluded)
	if err != nil || !r.sameBinding(fresh, true) {
		return errors.Join(errors.New("bounded Worker drain physical binding changed"), err)
	}
	selected, err := a.readNodeWorkerSelection(ctx, j, fresh)
	if err != nil || selected != state {
		return errors.Join(errors.New("bounded Worker drain serving pointer changed"), err)
	}
	return nil
}

func nodeWorkerDrainComplete(ctx context.Context, addr string, grant edgegroupfront.WorkerDrainGrant) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr, nil)
	if err != nil {
		return false, err
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	reply, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer reply.Body.Close()
	const limit = 1 << 20
	body, err := io.ReadAll(io.LimitReader(reply.Body, limit+1))
	if err != nil || len(body) > limit || reply.StatusCode != http.StatusOK {
		return false, errors.Join(errors.New("invalid Front Worker drain metrics response"), err)
	}
	return nodeWorkerDrainMetricsMatch(body, grant), nil
}

func nodeWorkerDrainMetricsMatch(body []byte, grant edgegroupfront.WorkerDrainGrant) bool {
	want := map[string]string{
		"capability":            "1",
		"last_generation":       strconv.FormatUint(grant.Serving.Generation, 10),
		"last_deadline_seconds": strconv.FormatInt(grant.Deadline.Unix(), 10),
		"last_remaining":        "0",
		"last_completed":        "1",
	}
	seen := map[string]bool{}
	scan := bufio.NewScanner(bytes.NewReader(body))
	for scan.Scan() {
		line, ok := strings.CutPrefix(scan.Text(), "fugue_edge_front_worker_drain_")
		if !ok {
			continue
		}
		key, value, _ := strings.Cut(line, " ")
		if expected, tracked := want[key]; tracked {
			if seen[key] || value != expected {
				return false
			}
			seen[key] = true
		}
	}
	return scan.Err() == nil && len(seen) == len(want)
}
