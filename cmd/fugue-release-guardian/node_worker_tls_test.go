package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/routeproof"
	"fugue/internal/trafficbinding"
)

func TestNodeWorkerTLSProofUsesOneAuthenticatedConnection(t *testing.T) {
	const node, group, host = "fixture-node", "edge-group-fixture", "example.com"
	digest := shaDigest([]byte("route"))
	binding := model.TrafficReleaseBinding{
		Schema: trafficbinding.Schema, ReleaseSetID: "set-a", ReleaseSetGeneration: "set-v1",
		RouteArtifactID: "artifact-a", RouteArtifactGeneration: "artifact-v1", RouteArtifactSequence: 1,
		ReleaseID: "release-a", ReleaseChannel: "full", FencingToken: 1, ScopeKey: "test-scope", CompilerVersion: "v1",
		ReleaseSetDigest: digest, RouteArtifactDigest: digest, IntentDigest: digest,
		PolicyDigest: digest, InputSnapshotDigest: digest, ProjectionDigest: digest,
	}
	data, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan string, 3)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != host {
			http.Error(w, "wrong Host", http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/close" {
			w.Header().Set("Connection", "close")
			_, _ = io.WriteString(w, "closing")
			return
		}
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, "canary")
			return
		}
		nonce := r.Header.Get(routeproof.NonceHeader)
		received <- nonce
		if r.Header.Get(routeproof.RequestHeader) != "1" || !routeproof.ValidNonce(nonce) {
			http.Error(w, "wrong challenge", http.StatusBadRequest)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set(routeproof.NonceHeader, nonce)
		w.Header().Set(routeproof.DigestHeader, digest)
		w.Header().Set(routeproof.VersionHeader, "routes-v1")
		w.Header().Set(routeproof.ExpiryHeader, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano))
		w.Header().Set(routeproof.EdgeHeader, node)
		w.Header().Set(routeproof.GroupHeader, group)
		w.Header().Set(routeproof.TrafficHeader, base64.RawURLEncoding.EncodeToString(data))
		w.WriteHeader(http.StatusNoContent)
	}))
	var opened atomic.Int32
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
	}
	server.StartTLS()
	defer server.Close()
	ctx := context.Background()
	target := netip.MustParseAddrPort(server.Listener.Addr().String())
	roots := server.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	s, err := openNodeWorkerTLS(ctx, target, host, roots)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	before, err := s.proof(ctx, "/ready", node, group)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.serving(ctx, "/ready", shaDigest([]byte("canary"))); err != nil {
		t.Fatal(err)
	}
	after, err := s.proof(ctx, "/ready", node, group)
	if err != nil || !sameNodeWorkerProof(before, after) || <-received == <-received {
		t.Fatalf("proofs did not use independent challenges on the same socket: %v", err)
	}
	if _, err := s.proof(ctx, "/ready", "other-node", group); err == nil {
		t.Fatal("accepted a route proved for another node")
	}
	if err := s.serving(ctx, "/close", shaDigest([]byte("closing"))); err == nil {
		t.Fatal("accepted a server-closed connection")
	}
	if err := s.serving(ctx, "/ready", shaDigest([]byte("canary"))); err == nil || opened.Load() != 1 {
		t.Fatal("the held connection was silently replaced")
	}
}

func TestNodeWorkerConntrackBindsBothTCPDirectionsAndKernelState(t *testing.T) {
	for _, tc := range []struct{ src, dst, front, translated string }{
		{"192.0.2.2:50000", "192.0.2.7:15443", "10.0.0.2:443", "10.0.0.1:40000"},
		{"[2001:db8::2]:50000", "[2001:db8::7]:15443", "[fd00::2]:443", "[fd00::1]:40000"},
	} {
		t.Run(tc.src, func(t *testing.T) {
			original := workerSocketTuple{netip.MustParseAddrPort(tc.src), netip.MustParseAddrPort(tc.dst)}
			front, translated := netip.MustParseAddrPort(tc.front), netip.MustParseAddrPort(tc.translated)
			const sequence = 17
			query, err := workerCTQuery(original, sequence)
			if err != nil {
				t.Fatal(err)
			}
			if binary.NativeEndian.Uint16(query[4:6]) != workerCTGet || binary.NativeEndian.Uint16(query[6:8]) != 1 {
				t.Fatal("observation attempted an operation other than exact CT_GET")
			}
			buildReply := func(reply workerSocketTuple, state byte, zone uint16) []byte {
				t.Helper()
				wire, err := workerCTQuery(reply, sequence)
				if err != nil {
					t.Fatal(err)
				}
				raw := append([]byte(nil), query...)
				raw = workerCTAttr(raw, 2|workerNested, wire[24:])
				tcp := workerCTAttr(nil, 1, []byte{state})
				raw = workerCTAttr(raw, 4|workerNested, workerCTAttr(nil, 1|workerNested, tcp))
				raw = workerCTAttr(raw, 18, binary.BigEndian.AppendUint16(nil, zone))
				binary.NativeEndian.PutUint32(raw, uint32(len(raw)))
				binary.NativeEndian.PutUint16(raw[4:], workerCTReply)
				binary.NativeEndian.PutUint16(raw[6:], 2)
				return raw
			}
			raw := buildReply(workerSocketTuple{front, translated}, 3, 0)
			if got, err := workerCTTranslation(raw, sequence, original, front); err != nil || got != translated {
				t.Fatalf("conntrack reply did not identify the translated peer: %v %v", got, err)
			}
			for _, invalid := range [][]byte{
				raw[:len(raw)-1], buildReply(workerSocketTuple{original.destination, translated}, 3, 0),
				buildReply(workerSocketTuple{front, translated}, 1, 0), buildReply(workerSocketTuple{front, translated}, 3, 4),
			} {
				if _, err := workerCTTranslation(invalid, sequence, original, front); err == nil {
					t.Fatal("accepted invalid, unestablished or wrong-destination conntrack evidence")
				}
			}
			if _, err := workerCTTranslation(raw, sequence+1, original, front); err == nil {
				t.Fatal("accepted a stale conntrack reply")
			}
		})
	}
}

func TestNodeWorkerInventoryBindsDownstreamPeerAndFrontListener(t *testing.T) {
	now := time.Now().UTC()
	peer, listener := netip.MustParseAddrPort("192.0.2.2:50000"), netip.MustParseAddrPort("10.0.0.2:443")
	item := frontnetwork.LiveConnection{ID: "old", Protocol: "https", Slot: "a", Target: "192.0.2.7:18443",
		DownstreamRemote: peer.String(), DownstreamLocal: listener.String(), StartedAt: now}
	view := frontnetwork.LiveConnections{Count: 1, Active: []frontnetwork.LiveConnection{item}}
	ref, found, err := nodeWorkerInventoryFact(view, "front", peer, listener)
	if err != nil || !found || ref.ID != item.ID || ref.Target != item.Target {
		t.Fatalf("exact socket did not bind to Front: %v %t %v", ref, found, err)
	}
	view.Active[0].DownstreamLocal = "10.0.0.3:443"
	if _, _, err := nodeWorkerInventoryFact(view, "front", peer, listener); err == nil {
		t.Fatal("accepted another Front listener")
	}
	view.Active = []frontnetwork.LiveConnection{item, item}
	view.Count = 2
	if _, _, err := nodeWorkerInventoryFact(view, "front", peer, listener); err == nil {
		t.Fatal("accepted ambiguous connection facts")
	}
}
