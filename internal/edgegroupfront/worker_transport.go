package edgegroupfront

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"strconv"
)

// A slot is a logical role, not ownership of every connection already on
// its hostPort. This immutable, node-scoped declaration allows *one* new
// Worker incarnation to overlap both retained ones. Only the usual local
// activation selects an incarnation. No caller, request header or activation
// document can supply an address or redirect a connection off the node.
type workerImagePin struct {
	Slot   string `json:"slot"`
	Source string `json:"source"`
	Image  string `json:"image"`
}

type workerTransportDeclaration struct {
	Schema    string           `json:"schema"`
	Group     string           `json:"group"`
	Node      string           `json:"node"`
	Retained  []workerImagePin `json:"retained"`
	Candidate workerImagePin   `json:"candidate,omitzero"`
	HTTP      uint16           `json:"candidate_http_port,omitempty"`
	HTTPS     uint16           `json:"candidate_https_port,omitempty"`
}

func (p workerImagePin) valid() bool {
	return (p.Slot == "a" || p.Slot == "b") && activationCommitPattern.MatchString(p.Source) && activationDigestPattern.MatchString(p.Image)
}

func (cfg *Config) prepareWorkerTransport() error {
	// Registered overlap listener pair. A release cannot use a valid
	// activation to forward client traffic to some unrelated node service.
	return cfg.bindWorkerTransport(38080, 38443)
}

func (cfg *Config) bindWorkerTransport(httpPort, httpsPort uint16) error {
	if cfg.WorkerTransport == "" {
		return nil
	}
	fail := errors.New("invalid node-local Worker transport declaration")
	if len(cfg.WorkerTransport) > 4096 || !cfg.RequireActivationState || cfg.ActiveSlotFile == "" {
		return fail
	}
	var v workerTransportDeclaration
	b := []byte(cfg.WorkerTransport)
	if json.Unmarshal(b, &v) != nil {
		return fail
	}
	overlap := v.Schema == "fugue.worker-transport/v1"
	switch v.Schema {
	case "fugue.worker-transport/v1":
		if v.HTTP != httpPort || v.HTTPS != httpsPort || v.HTTP < 1024 || v.HTTPS < 1024 ||
			v.HTTP == v.HTTPS || !v.Candidate.valid() {
			return fail
		}
	case "fugue.worker-transport-fixed/v1":
		// Pin the two existing Workers without declaring phantom listeners.
		if v.Candidate != (workerImagePin{}) || v.HTTP != 0 || v.HTTPS != 0 {
			return fail
		}
	default:
		return fail
	}
	// The renderer emits this canonical layout. Reject duplicate/unknown
	// fields rather than allowing two decoders to choose different ports.
	wire, err := json.Marshal(v)
	var compact bytes.Buffer
	if err != nil || json.Compact(&compact, b) != nil || !bytes.Equal(wire, compact.Bytes()) {
		return fail
	}
	ip, err := netip.ParseAddr(cfg.NodeHost)
	if err != nil || ip.Zone() != "" || ip.IsUnspecified() || ip.IsMulticast() || ip.Is4In6() || !activationGroupPattern.MatchString(cfg.EdgeGroupID) ||
		v.Group != cfg.EdgeGroupID || v.Node == "" || v.Node != cfg.EdgeID || len(v.Retained) != 2 ||
		v.Retained[0].Slot != "a" || v.Retained[1].Slot != "b" {
		return fail
	}
	taken := map[uint16]bool{}
	if overlap {
		taken[v.HTTP], taken[v.HTTPS] = true, true
	}
	lookup := map[workerImagePin]SlotTargets{}
	for _, p := range v.Retained {
		if !p.valid() || (overlap && p.Image == v.Candidate.Image) {
			return fail
		}
		t := cfg.Slots[p.Slot]
		for _, addr := range []string{t.HTTPAddress, t.HTTPSAddress} {
			ep, e := netip.ParseAddrPort(addr)
			if e != nil || ep.Addr() != ip || ep.Port() < 1024 || taken[ep.Port()] {
				return fail
			}
			taken[ep.Port()] = true
		}
		lookup[p] = t
	}
	if overlap {
		port := func(n uint16) string { return net.JoinHostPort(ip.String(), strconv.Itoa(int(n))) }
		lookup[v.Candidate] = SlotTargets{HTTPAddress: port(v.HTTP), HTTPSAddress: port(v.HTTPS)}
	}
	cfg.workerTargets = lookup
	return nil
}

// Once selected, handleTCPConnection keeps the actual upstream socket.
// A changed declaration or local CAS can affect new connections only.
// In overlap mode an unreadable/unknown release never defaults to the old
// physical port of a reused slot. Existing sessions need no state read.
func (s *Service) selectedWorker(cfg Config) (string, *ActivationState, SlotTargets, bool) {
	if cfg.WorkerTransport == "" {
		slot, a := s.activeSlotWithActivation(cfg)
		t, ok := cfg.Slots[slot]
		return slot, a, t, ok
	}
	a, found, e := ReadActivationState(cfg.ActiveSlotFile)
	if e != nil || !found || a.GroupID != cfg.EdgeGroupID {
		return "", nil, SlotTargets{}, false
	}
	t, ok := cfg.workerTargets[workerImagePin{a.ActiveSlot, a.WorkerSourceCommit, a.WorkerImageDigest}]
	return a.ActiveSlot, &a, t, ok
}
