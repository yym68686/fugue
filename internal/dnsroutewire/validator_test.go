package dnsroutewire

import (
	"testing"
	"time"

	"fugue/internal/model"
)

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
