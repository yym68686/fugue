package api

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestLegacyDNSHTTPRetiredWithStoredBundleAndAmbientBusinessConfiguration(t *testing.T) {
	state, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	server.dnsStaticRecords = []model.EdgeDNSRecord{{Name: "ambient.example.test", Type: "A", Values: []string{"192.0.2.9"}, TTL: 60}}
	path := "/v1/edge/dns?token=edge-secret&dns_node_id=dns-a&edge_group_id=edge-group-a&zone=example.test&answer_ip=192.0.2.8&ttl=60"
	check := func() {
		t.Helper()
		r := performJSONRequest(t, server, http.MethodGet, path, "", nil)
		if r.Code != http.StatusGone || r.Header().Get("Cache-Control") != "no-store" || r.Header().Get("X-Fugue-DNS-Bundle-Version") != "" || r.Header().Get("ETag") != "" {
			t.Fatal("legacy bundle still served", r.Code, r.Body.String())
		}
		for _, host := range []string{app.Route.Hostname, "ambient.example.test", "old.example.test"} {
			if strings.Contains(r.Body.String(), host) {
				t.Fatal("retired endpoint leaked serving configuration", host)
			}
		}
	}
	check()
	now := time.Now().UTC()
	options := edgeDNSBundleOptions{DNSNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", AnswerIPs: []string{"192.0.2.8"}, TTL: 60}
	bundle := signEdgeDNSBundle(model.EdgeDNSBundle{Version: "old-version", Generation: "old-generation", GeneratedAt: now, DNSNodeID: options.DNSNodeID, EdgeGroupID: options.EdgeGroupID, Zone: options.Zone, Records: []model.EdgeDNSRecord{{Name: "old.example.test", Type: "A", Values: options.AnswerIPs, TTL: 60, Status: model.EdgeRouteStatusActive}}}, server.bundleKeyring(), time.Minute)
	publishFullEdgeDNSArtifactForTest(t, state, server, newEdgeDNSBundleArtifact(options, bundle, now))
	before, _, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindDNSAnswerBundle, edgeDNSBundleArtifactScopeKey(options), "full")
	if err != nil || !found {
		t.Fatal(err)
	}
	check()
	after, _, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindDNSAnswerBundle, edgeDNSBundleArtifactScopeKey(options), "full")
	if err != nil || !found || after.ID != before.ID || after.ContentHash != before.ContentHash {
		t.Fatal("retirement changed stored migration evidence", err)
	}
	r := performJSONRequest(t, server, http.MethodGet, strings.Replace(path, "token=edge-secret", "token=invalid", 1), "", nil)
	if r.Code == http.StatusGone || r.Code == http.StatusOK {
		t.Fatal("retired endpoint bypassed authorization", r.Code)
	}
}
