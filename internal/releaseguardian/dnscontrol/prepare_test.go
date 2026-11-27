package dnscontrol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func TestHoldDeltaPreparationPreservesUnrelatedJSON(t *testing.T) {
	now := time.Now().UTC()
	s, _, _, _ := testSources(t, now)
	change := HoldChange{EdgeID: "edge-a", After: &platformproducer.OfflineEdge{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "maintenance"}}
	req, err := planNextDNS(s.PreviousDNS, s.Static, s.Producer.Artifact, change)
	if err != nil {
		t.Fatal(err)
	}
	again, err := planNextDNS(s.PreviousDNS, s.Static, s.Producer.Artifact, change)
	if err != nil || !reflect.DeepEqual(req, again) {
		t.Fatalf("same changes did not reuse exact draft generation: %v", err)
	}
	a, b := req.Content, s.PreviousDNS.Content
	for k, v := range b {
		if k != "generation" && k != "offline_edges" && !reflect.DeepEqual(a[k], v) {
			t.Fatalf("mutated unrelated field %s", k)
		}
	}
	if _, present := b["offline_edges"]; present {
		t.Fatal("preparation modified the original immutable source")
	}
	change.Before = change.After
	change.After = nil
	if _, err := planNextDNS(s.PreviousDNS, s.Static, s.Producer.Artifact, change); err == nil {
		t.Fatal("removing an absent hold accepted")
	}
	for _, raw := range []string{
		`{"edgeId":"edge-a","before":null}`,
		`{"edgeId":"edge-a","before":null,"before":null,"after":null}`,
		`{"edgeId":"edge-a","before":{"edge_id":"edge-b","edge_group_id":"edge-group-a","public_ipv4":"8.8.8.8","reason":"maintenance"},"after":null}`,
		`{"edgeId":"edge-a","before":null,"after":null}`,
	} {
		if _, err := DecodeHoldChange([]byte(raw)); err == nil {
			t.Fatalf("unreviewable change accepted: %s", raw)
		}
	}
}

func TestPrepareDNSDraftNeverPublishesAndReusesStoredGenerations(t *testing.T) {
	now := time.Now().UTC()
	s, b, a, _ := testSources(t, now)
	change := HoldChange{EdgeID: "edge-a", After: &platformproducer.OfflineEdge{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "maintenance"}}
	mu := sync.Mutex{}
	artifacts := map[string]model.PlatformArtifact{s.Static.ID: s.Static, s.PreviousDNS.ID: s.PreviousDNS}
	creates, validates := 0, 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		emit := func(value any) {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(value); err != nil {
				t.Error(err)
			}
		}
		state := func(p Publication, lkg *model.PlatformLKGSnapshot) {
			emit(model.PlatformStateArtifactResponse{Artifact: &p.Artifact, Release: &p.Release, Generation: p.Artifact.Generation, LKG: lkg})
		}
		const artifact = "/v1/admin/artifacts/"
		path := r.URL.Path
		switch r.Method {
		case http.MethodGet:
			if r.Header.Get("Authorization") != "Bearer fixture-reader" {
				t.Error("reader credential crossed boundary")
				http.Error(w, "wrong identity", 403)
				return
			}
			switch path {
			case "/v1/platform-state/artifacts/policy_snapshot":
				state(s.Producer, nil)
			case "/v1/platform-state/artifacts/release_set":
				state(s.Full, &s.LKG)
			case "/v1/admin/platform-config/routes/project":
				id := r.URL.Query().Get("producer_policy_artifact_id")
				if id == s.Producer.Artifact.ID {
					emit(b)
					return
				}
				if next, ok := artifacts[id]; ok && next.ScopeKey == platformproducer.Scope && next.Status == model.PlatformArtifactStatusValidated {
					emit(a)
					return
				}
				http.NotFound(w, r)
			case "/v1/admin/artifacts":
				q := r.URL.Query()
				var found []model.PlatformArtifact
				for _, v := range artifacts {
					if v.ArtifactKind == q.Get("kind") && v.ScopeKey == q.Get("scope") && v.Generation == q.Get("generation") {
						found = append(found, v)
					}
				}
				emit(model.PlatformArtifactListResponse{Artifacts: found})
			default:
				if strings.HasPrefix(path, artifact) {
					if a, ok := artifacts[strings.TrimPrefix(path, artifact)]; ok {
						emit(model.PlatformArtifactResponse{Artifact: a})
						return
					}
				}
				http.NotFound(w, r)
			}
		case http.MethodPost:
			if r.Header.Get("Authorization") != "Bearer fixture-drafter" {
				t.Error("writer credential crossed boundary")
				http.Error(w, "wrong identity", 403)
				return
			}
			switch {
			case path == "/v1/admin/artifacts":
				var req model.PlatformArtifactCreateRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					http.Error(w, "decode", 400)
					return
				}
				creates++
				v := testArtifact(t, "draft-"+req.Generation, req.ArtifactKind, req.Scope.Key, req.Generation, int64(10+creates), req.Content)
				v.Status = model.PlatformArtifactStatusDraft
				artifacts[v.ID] = v
				emit(model.PlatformArtifactResponse{Artifact: v})
			case strings.HasPrefix(path, artifact) && strings.HasSuffix(path, "/validate"):
				id := strings.TrimSuffix(strings.TrimPrefix(path, artifact), "/validate")
				current, exists := artifacts[id]
				if !exists {
					http.NotFound(w, r)
					return
				}
				var req model.PlatformArtifactValidateRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.DryRun {
					t.Errorf("wrong validation request: %v", err)
					http.Error(w, "validate", 400)
					return
				}
				validates++
				current.Status = model.PlatformArtifactStatusValidated
				artifacts[id] = current
				emit(model.PlatformArtifactValidationResponse{Artifact: current, Pass: true})
			default:
				t.Errorf("draft preparer attempted a publication: %s", path)
				http.Error(w, "forbidden", 403)
			}
		default:
			t.Errorf("unexpected method %s", r.Method)
			http.Error(w, "forbidden", 403)
		}
	}))
	defer server.Close()
	reader, err := NewReader(server.URL, "fixture-reader")
	if err != nil {
		t.Fatal(err)
	}
	reader.client.Transport = server.Client().Transport
	preparer, err := NewDraftPreparer(reader, "fixture-drafter")
	if err != nil {
		t.Fatal(err)
	}
	preparer.client.Transport = server.Client().Transport
	first, err := preparer.Prepare(context.Background(), change, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	replay, err := preparer.Prepare(context.Background(), change, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if artifactRef(first.Sources.NextProducer) != artifactRef(replay.Sources.NextProducer) || artifactRef(first.Sources.NextDNS) != artifactRef(replay.Sources.NextDNS) {
		t.Fatal("prepared different targets after restart")
	}
	mu.Lock()
	defer mu.Unlock()
	if creates != 2 || validates != 2 {
		t.Fatalf("expected two immutable draft writes, got %d creates and %d validates", creates, validates)
	}
	if first.Sources.Producer.Release.ID != s.Producer.Release.ID {
		t.Fatal("draft changed the selected producer")
	}
	next, _ := platformconfig.Digest(first.Sources.NextDNS.Content)
	if next != first.Sources.NextDNS.ContentHash {
		t.Fatal("prepared artifact contents drifted")
	}
}
