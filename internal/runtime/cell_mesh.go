package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"sort"
	"strings"
	"time"
)

type CommandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

func defaultCommandRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

func (s *AgentService) BuildCellSnapshot(ctx context.Context) CellSnapshot {
	now := time.Now().UTC()
	pending, _ := s.CellStore.CountPendingCompletions()
	routeCount, _ := s.CellStore.CountRoutes()
	observedPeers, _ := s.CellStore.CountPeerObservations()
	mesh, peers := s.detectCellMesh(ctx)
	endpoint := strings.TrimSpace(s.Config.RuntimeEndpoint)
	if endpoint == "" && mesh.IP != "" {
		endpoint = mesh.IP
	}
	return CellSnapshot{
		RuntimeID:          strings.TrimSpace(s.Config.RuntimeID),
		RuntimeName:        strings.TrimSpace(s.Config.RuntimeName),
		MachineName:        strings.TrimSpace(s.Config.MachineName),
		MachineFingerprint: strings.TrimSpace(s.Config.MachineFingerprint),
		Endpoint:           endpoint,
		Mesh:               mesh,
		Peers:              peers,
		ObservedPeerCount:  observedPeers,
		RouteCount:         routeCount,
		OutboxPending:      pending,
		ObservedAt:         now,
	}
}

func (s *AgentService) RefreshCellSnapshot(ctx context.Context) (CellSnapshot, error) {
	if err := s.ensureCellStore(); err != nil {
		return CellSnapshot{}, err
	}
	snapshot := s.BuildCellSnapshot(ctx)
	if err := s.CellStore.SaveSnapshot(snapshot); err != nil {
		return CellSnapshot{}, err
	}
	return snapshot, nil
}

func (s *AgentService) detectCellMesh(ctx context.Context) (CellMesh, []CellPeer) {
	runner := s.CommandRunner
	if runner == nil {
		runner = defaultCommandRunner
	}
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	output, err := runner(probeCtx, "tailscale", "ip", "-4")
	ip := firstUsableIPLine(string(output))
	if err != nil || ip == "" {
		return CellMesh{}, nil
	}
	mesh := CellMesh{
		Provider:        "tailscale",
		IP:              ip,
		DiscoverySource: "tailscale",
	}

	statusCtx, statusCancel := context.WithTimeout(ctx, 3*time.Second)
	defer statusCancel()
	statusOutput, statusErr := runner(statusCtx, "tailscale", "status", "--json")
	if statusErr != nil {
		return mesh, nil
	}
	hostname, peers := parseTailscaleStatus(statusOutput)
	if hostname != "" {
		mesh.Hostname = hostname
	}
	return mesh, peers
}

func firstUsableIPLine(raw string) string {
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if parsed := net.ParseIP(line); parsed != nil {
			return line
		}
	}
	return ""
}

type tailscaleStatusJSON struct {
	Self struct {
		HostName     string   `json:"HostName"`
		TailscaleIPs []string `json:"TailscaleIPs"`
	} `json:"Self"`
	Peer map[string]struct {
		HostName     string   `json:"HostName"`
		TailscaleIPs []string `json:"TailscaleIPs"`
		Online       bool     `json:"Online"`
		LastSeen     string   `json:"LastSeen"`
	} `json:"Peer"`
}

func parseTailscaleStatus(data []byte) (string, []CellPeer) {
	var status tailscaleStatusJSON
	if err := json.Unmarshal(data, &status); err != nil {
		return "", nil
	}
	peers := make([]CellPeer, 0, len(status.Peer))
	for _, peer := range status.Peer {
		ip := firstString(peer.TailscaleIPs)
		if ip == "" {
			continue
		}
		cellPeer := CellPeer{
			Hostname: strings.TrimSpace(peer.HostName),
			IP:       ip,
			Online:   peer.Online,
		}
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(peer.LastSeen)); err == nil {
			parsed = parsed.UTC()
			cellPeer.LastSeen = &parsed
		}
		peers = append(peers, cellPeer)
	}
	sort.Slice(peers, func(i, j int) bool {
		if peers[i].Hostname == peers[j].Hostname {
			return peers[i].IP < peers[j].IP
		}
		return peers[i].Hostname < peers[j].Hostname
	})
	return strings.TrimSpace(status.Self.HostName), peers
}

func firstString(values []string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func CellSnapshotRuntimeLabels(snapshot CellSnapshot) map[string]string {
	labels := map[string]string{}
	if snapshot.ObservedAt.IsZero() &&
		strings.TrimSpace(snapshot.Mesh.Provider) == "" &&
		strings.TrimSpace(snapshot.Mesh.IP) == "" &&
		strings.TrimSpace(snapshot.Mesh.Hostname) == "" &&
		len(snapshot.Peers) == 0 &&
		snapshot.ObservedPeerCount == 0 &&
		snapshot.RouteCount == 0 &&
		snapshot.OutboxPending == 0 {
		return labels
	}
	if value := strings.TrimSpace(snapshot.Mesh.Provider); value != "" {
		labels[CellRuntimeLabelMeshProvider] = value
	}
	if value := strings.TrimSpace(snapshot.Mesh.IP); value != "" {
		labels[CellRuntimeLabelMeshIP] = value
	}
	if value := strings.TrimSpace(snapshot.Mesh.Hostname); value != "" {
		labels[CellRuntimeLabelMeshHostname] = value
	}
	if value := strings.TrimSpace(snapshot.Mesh.DiscoverySource); value != "" {
		labels[CellRuntimeLabelDiscoverySource] = value
	}
	labels[CellRuntimeLabelPeerCount] = fmt.Sprintf("%d", len(snapshot.Peers))
	labels[CellRuntimeLabelObservedPeers] = fmt.Sprintf("%d", snapshot.ObservedPeerCount)
	labels[CellRuntimeLabelRouteCount] = fmt.Sprintf("%d", snapshot.RouteCount)
	labels[CellRuntimeLabelOutboxPending] = fmt.Sprintf("%d", snapshot.OutboxPending)
	if !snapshot.ObservedAt.IsZero() {
		labels[CellRuntimeLabelObservedAt] = snapshot.ObservedAt.UTC().Format(time.RFC3339)
	}
	return labels
}
