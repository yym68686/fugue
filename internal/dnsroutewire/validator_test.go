package dnsroutewire

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestValidatorReusePreservesExactV1BytesAndRejectsNativeMutations(t *testing.T) {
	for _, change := range []string{"content", "metadata", "scope", "provenance", "lane", "selection", "lkg", "release", "producer"} {
		t.Run(change, func(t *testing.T) {
			r := model.PlatformConsumerDNSRouteSourcesResponse{Snapshot: model.PlatformDNSRouteSourceSnapshot{Scopes: []model.PlatformDNSRouteSourceScope{{Lanes: []model.PlatformDNSRouteSourceLane{{Version: 1}}, Publications: []model.PlatformDNSRouteSourcePublication{{Route: model.PlatformArtifact{ContentHash: "claimed", Content: map[string]any{"nested": []any{map[string]any{"value": "original"}}}, Metadata: map[string]string{"owner": "original"}}}}}}}}
			before, err := Validator(r, nil)
			want, wantErr := validatorUncached(model.PlatformConsumerRetainedDNSRouteSourcesResponse{PlatformConsumerDNSRouteSourcesResponse: r})
			if err != nil || wantErr != nil || before != want {
				t.Fatal("cache changed the established v1 wire validator", err, wantErr)
			}
			pub := &r.Snapshot.Scopes[0].Publications[0]
			switch change {
			case "content":
				pub.Route.Content["nested"].([]any)[0].(map[string]any)["value"] = "changed"
			case "metadata":
				pub.Route.Metadata["owner"] = "changed"
			case "scope":
				pub.Route.ScopeKey = "changed"
			case "provenance":
				pub.Route.Provenance.Signature = "changed"
			case "lane":
				r.Snapshot.Scopes[0].Lanes[0].Version++
			case "selection":
				pub.Selections = []string{"lkg"}
			case "lkg":
				pub.LKG = &model.PlatformLKGSnapshot{ID: "changed"}
			case "release":
				pub.Release.FencingToken++
			case "producer":
				pub.ProducerPolicy.Content = map[string]any{"policy": "changed"}
			}
			after, err := Validator(r, nil)
			want, wantErr = validatorUncached(model.PlatformConsumerRetainedDNSRouteSourcesResponse{PlatformConsumerDNSRouteSourcesResponse: r})
			if err != nil || wantErr != nil || after == before || after != want {
				t.Fatal("actual response mutation was hidden by reuse", err, wantErr)
			}
		})
	}
	// A successfully cached response cannot mask later unsupported or oversized
	// content, even if all claimed hashes and mutable envelope fields stay put.
	r := model.PlatformConsumerDNSRouteSourcesResponse{Snapshot: model.PlatformDNSRouteSourceSnapshot{Scopes: []model.PlatformDNSRouteSourceScope{{Publications: []model.PlatformDNSRouteSourcePublication{{Route: model.PlatformArtifact{Content: map[string]any{"body": "small"}}}}}}}}
	if _, err := Validator(r, nil); err != nil {
		t.Fatal(err)
	}
	r.Snapshot.Scopes[0].Publications[0].Route.Content["body"] = make(chan int)
	if _, err := Validator(r, nil); err == nil {
		t.Fatal("unsupported content reused a validator")
	}
	r.Snapshot.Scopes[0].Publications[0].Route.Content["body"] = strings.Repeat("x", 16<<20)
	if _, err := Validator(r, nil); err == nil {
		t.Fatal("oversized content reused a validator")
	}
}

func BenchmarkSourceValidator(b *testing.B) {
	content := map[string]any{}
	for i := range 1024 {
		content[fmt.Sprint(i)] = map[string]any{"route": strings.Repeat("value", 40), "nested": []any{true, float64(i)}}
	}
	raw, _ := json.Marshal(content)
	var native map[string]any
	_ = json.Unmarshal(raw, &native)
	r := model.PlatformConsumerDNSRouteSourcesResponse{Snapshot: model.PlatformDNSRouteSourceSnapshot{Scopes: []model.PlatformDNSRouteSourceScope{{Publications: []model.PlatformDNSRouteSourcePublication{{Route: model.PlatformArtifact{Content: native}}}}}}}
	for _, warm := range []bool{false, true} {
		b.Run(fmt.Sprintf("reuse=%t", warm), func(b *testing.B) {
			if _, err := Validator(r, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var err error
				if warm {
					_, err = Validator(r, nil)
				} else {
					_, err = validatorUncached(model.PlatformConsumerRetainedDNSRouteSourcesResponse{PlatformConsumerDNSRouteSourcesResponse: r})
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestValidatorBindsActualContentsAndFreshAuthority(t *testing.T) {
	response := model.PlatformConsumerDNSRouteSourcesResponse{Snapshot: model.PlatformDNSRouteSourceSnapshot{
		ObservedAt: time.Now(), SelectionDigest: "claimed-same", Scopes: []model.PlatformDNSRouteSourceScope{{Publications: []model.PlatformDNSRouteSourcePublication{{Route: model.PlatformArtifact{ContentHash: "claimed-same", Content: map[string]any{"value": "original"}}}}}},
	}}
	first, err := Validator(response, nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Snapshot.ObservedAt = time.Now().Add(time.Second)
	if next, _ := Validator(response, nil); next != first {
		t.Fatal("time change invalidated pure content reuse")
	}
	response.Snapshot.Scopes[0].Publications[0].Route.Content["value"] = "changed"
	if next, _ := Validator(response, nil); next == first {
		t.Fatal("claimed digests hid actual content mutation")
	}
	response.Snapshot.Scopes[0].Publications[0].Route.Content["value"] = "original"
	response.Assignment.FencingToken++
	if next, _ := Validator(response, nil); next == first {
		t.Fatal("validator ignored assignment fencing")
	}
	response.Assignment.FencingToken--
	if next, _ := Validator(response, &model.PlatformLKGSnapshot{ID: "current-lkg"}); next == first {
		t.Fatal("validator ignored retained LKG")
	}
}
