package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/frontnetwork"
	"fugue/internal/releaseguardian"
	"fugue/internal/routeprobe"
)

type nodeWorkerIngressFact struct {
	Connection releaseguardian.NodeWorkerConnection `json:"connection"`
	Source     string                               `json:"source"`
}

type nodeWorkerIngressObservation struct {
	Public nodeWorkerIngressFact  `json:"public"`
	Probe  *nodeWorkerIngressFact `json:"probe,omitempty"`
	Route  routeprobe.Proof       `json:"route"`
}

var nodeWorkerInventoryClient = &http.Client{
	Transport:     &http.Transport{DisableKeepAlives: true},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func readNodeWorkerInventory(ctx context.Context, podIP string) (frontnetwork.LiveConnections, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	address, err := netip.ParseAddr(podIP)
	if err != nil || address.Zone() != "" || address.IsUnspecified() || address.IsMulticast() {
		return frontnetwork.LiveConnections{}, errors.New("invalid Front connection inventory endpoint")
	}
	endpoint := repairHealthEndpoint(address.String(), frontHealthPort, "/edge/tcp-connections")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return frontnetwork.LiveConnections{}, err
	}
	response, err := nodeWorkerInventoryClient.Do(request)
	if err != nil {
		return frontnetwork.LiveConnections{}, err
	}
	defer response.Body.Close()
	const bound = 4 << 20
	raw, err := io.ReadAll(io.LimitReader(response.Body, bound+1))
	if err != nil || len(raw) > bound || response.StatusCode != http.StatusOK {
		return frontnetwork.LiveConnections{}, errors.Join(errors.New("Front connection inventory is unreadable or oversized"), err)
	}
	return decodeNodeWorkerInventory(raw)
}

func decodeNodeWorkerInventory(raw []byte) (frontnetwork.LiveConnections, error) {
	var view frontnetwork.LiveConnections
	if json.Unmarshal(raw, &view) != nil || view.Count != len(view.Active) || view.Count > 16384 {
		return frontnetwork.LiveConnections{}, errors.New("Front connection inventory is incomplete")
	}
	return view, nil
}

func nodeWorkerEndpoint(raw string) netip.AddrPort {
	parsed, err := netip.ParseAddrPort(raw)
	if err != nil || parsed.Addr().Zone() != "" || parsed.Port() == 0 {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(parsed.Addr().Unmap(), parsed.Port())
}

func nodeWorkerInventoryFact(view frontnetwork.LiveConnections, frontUID string, peer, listener netip.AddrPort) (releaseguardian.NodeWorkerConnection, bool, error) {
	var ref releaseguardian.NodeWorkerConnection
	for _, item := range view.Active {
		if nodeWorkerEndpoint(item.DownstreamRemote) != peer {
			continue
		}
		if ref.ID != "" || item.Protocol != "https" || nodeWorkerEndpoint(item.DownstreamLocal) != listener ||
			item.ID == "" || item.StartedAt.IsZero() {
			return releaseguardian.NodeWorkerConnection{}, false, errors.New("held socket has an ambiguous or invalid Front connection")
		}
		ref = releaseguardian.NodeWorkerConnection{
			FrontUID: frontUID, ID: item.ID, Slot: item.Slot, Target: item.Target, StartedAt: item.StartedAt,
		}
	}
	if ref.ID == "" {
		return ref, false, nil
	}
	for _, item := range view.Active {
		if item.ID == ref.ID && nodeWorkerEndpoint(item.DownstreamRemote) != peer {
			return releaseguardian.NodeWorkerConnection{}, false, errors.New("Front reported the same connection ID for two sockets")
		}
	}
	return ref, true, nil
}

func observeNodeWorkerIngressFact(ctx context.Context, runtime nodeWorkerRuntime, j edgegroupfront.NodeWorkerJournal,
	socket *nodeWorkerTLS, slot, target string) (nodeWorkerIngressFact, error) {
	view, err := readNodeWorkerInventory(ctx, runtime.front.Status.PodIP)
	if err != nil {
		return nodeWorkerIngressFact{}, err
	}
	address, err := netip.ParseAddr(runtime.front.Status.PodIP)
	if err != nil {
		return nodeWorkerIngressFact{}, err
	}
	listener := netip.AddrPortFrom(address.Unmap(), 443)
	peer := netip.AddrPortFrom(socket.source.Addr().Unmap(), socket.source.Port())
	frontUID := string(runtime.front.UID)
	ref, found, err := nodeWorkerInventoryFact(view, frontUID, peer, listener)
	if err != nil {
		return nodeWorkerIngressFact{}, err
	}
	source := "socket"
	if !found {
		destination := netip.AddrPortFrom(socket.destination.Addr().Unmap(), socket.destination.Port())
		translated, err := readNodeWorkerTranslation(ctx, workerSocketTuple{source: peer, destination: destination}, listener)
		if err != nil {
			return nodeWorkerIngressFact{}, errors.Join(errors.New("cannot attribute the held socket to the pinned Front"), err)
		}
		fresh, err := readNodeWorkerInventory(ctx, runtime.front.Status.PodIP)
		if err != nil {
			return nodeWorkerIngressFact{}, err
		}
		ref, found, err = nodeWorkerInventoryFact(fresh, frontUID, translated, listener)
		if err != nil {
			return nodeWorkerIngressFact{}, err
		}
		source = "kernel_conntrack"
	}
	if !found || !ref.Valid(j, time.Now().UTC()) || ref.Slot != slot || ref.Target != target {
		return nodeWorkerIngressFact{}, errors.New("held TLS socket did not reach the exact physical Worker listener")
	}
	return nodeWorkerIngressFact{Connection: ref, Source: source}, nil
}

func (a *frontAuthorityActivator) observeNodeWorkerIngress(ctx context.Context, j edgegroupfront.NodeWorkerJournal,
	preflight nodeWorkerPreflight) (nodeWorkerIngressObservation, error) {
	runtime := preflight.runtime
	if len(runtime.service.Spec.ExternalIPs) != 1 {
		return nodeWorkerIngressObservation{}, errors.New("public Front Service lacks an exact address")
	}
	ip, err := netip.ParseAddr(runtime.service.Spec.ExternalIPs[0])
	if err != nil {
		return nodeWorkerIngressObservation{}, err
	}
	host, path, body := a.config.RouteHost, a.config.RoutePath, a.config.RouteBodyDigest
	retained := j.Expected.ActiveSlot
	retainedTarget := runtime.prior.target
	public, err := openNodeWorkerTLS(ctx, netip.AddrPortFrom(ip, 443), host, nil)
	if err != nil {
		return nodeWorkerIngressObservation{}, fmt.Errorf("open public Front TLS: %w", err)
	}
	defer public.close()
	before, err := public.proof(ctx, path, j.Recipient.Node, j.Recipient.Group)
	if err != nil {
		return nodeWorkerIngressObservation{}, err
	}
	if err := public.serving(ctx, path, body); err != nil {
		return nodeWorkerIngressObservation{}, err
	}
	fact, err := observeNodeWorkerIngressFact(ctx, runtime, j, public, retained, retainedTarget)
	if err != nil {
		return nodeWorkerIngressObservation{}, err
	}
	result := nodeWorkerIngressObservation{Public: fact, Route: before}
	if j.ProbeServiceUID != "" {
		if len(runtime.probe.Spec.Ports) != 1 || runtime.probe.Spec.Ports[0].Port != 15443 {
			return nodeWorkerIngressObservation{}, errors.New("approved Front probe listener is missing")
		}
		probe, err := openNodeWorkerTLS(ctx, netip.AddrPortFrom(ip, uint16(runtime.probe.Spec.Ports[0].Port)), host, nil)
		if err != nil {
			return nodeWorkerIngressObservation{}, fmt.Errorf("open independent Front probe TLS: %w", err)
		}
		defer probe.close()
		p, err := probe.proof(ctx, path, j.Recipient.Node, j.Recipient.Group)
		if err != nil || !sameNodeWorkerProof(before, p) {
			return nodeWorkerIngressObservation{}, errors.Join(errors.New("Front public and probe route proofs differ"), err)
		}
		if err := probe.serving(ctx, path, body); err != nil {
			return nodeWorkerIngressObservation{}, err
		}
		probeFact, err := observeNodeWorkerIngressFact(ctx, runtime, j, probe, retained, retainedTarget)
		if err != nil || probeFact.Connection.ID == fact.Connection.ID {
			return nodeWorkerIngressObservation{}, errors.Join(errors.New("independent Front probe did not have its own confirmed TLS socket"), err)
		}
		result.Probe = &probeFact
	}
	after, err := public.proof(ctx, path, j.Recipient.Node, j.Recipient.Group)
	if err != nil || !sameNodeWorkerProof(before, after) {
		return nodeWorkerIngressObservation{}, errors.Join(errors.New("Front route proof changed during the ingress observation"), err)
	}
	again, err := observeNodeWorkerIngressFact(ctx, runtime, j, public, retained, retainedTarget)
	if err != nil || !fact.Connection.Same(again.Connection) || fact.Source != again.Source {
		return nodeWorkerIngressObservation{}, errors.Join(errors.New("original public Front socket changed during observation"), err)
	}
	return result, nil
}
