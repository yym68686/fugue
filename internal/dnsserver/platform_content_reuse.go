package dnsserver

import (
	"reflect"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
)

// Serving artifact trees are immutable. Reuse backing storage only after
// comparing the complete newly downloaded content, never its claimed digest.
// Every envelope, release, signature and runtime fact is still checked normally.
// The fresh transport response and assignment must succeed before this runs.
func reuseDNSArtifactContent(next *model.PlatformArtifact, previous model.PlatformArtifact) bool {
	if next.ID == previous.ID && next.ContentHash == previous.ContentHash && equalDNSJSON(next.Content, previous.Content, 0) {
		next.Content = previous.Content
		return true
	}
	return false
}

// Native JSON trees from a bounded fresh decode cannot contain cycles. Compare
// them without reflect.DeepEqual's per-node visited pairs and boxed map values.
// Keep its exact semantics for unusual in-process values and excessive depth;
// this is storage reuse only, never a substitute for signature validation.
func equalDNSJSON(a, b any, depth int) bool {
	if depth > 128 {
		return reflect.DeepEqual(a, b)
	}
	switch x := a.(type) {
	case nil:
		return b == nil
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	case string:
		y, ok := b.(string)
		return ok && x == y
	case float64:
		y, ok := b.(float64)
		return ok && x == y
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) || (x == nil) != (y == nil) {
			return false
		}
		if reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer() {
			return true
		}
		for key, value := range x {
			other, exists := y[key]
			if !exists || !equalDNSJSON(value, other, depth+1) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) || (x == nil) != (y == nil) {
			return false
		}
		if reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer() {
			return true
		}
		for i, value := range x {
			if !equalDNSJSON(value, y[i], depth+1) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}

func reuseDNSRouteSourceContent(next *model.PlatformDNSRouteSourceSnapshot, previous *dnsroutesource.Context) {
	if previous == nil {
		return
	}
	// The snapshot verifier bounds publication count. This index owns no data
	// beyond this call and cannot retain superseded artifacts between refreshes.
	contents := map[string]model.PlatformArtifact{}
	for _, scope := range previous.Snapshot.Scopes {
		for _, pub := range scope.Publications {
			for _, a := range []model.PlatformArtifact{pub.Parent, pub.Route, pub.TLS, pub.ProducerPolicy} {
				contents[a.ID] = a
			}
		}
	}
	for i := range next.Scopes {
		for j := range next.Scopes[i].Publications {
			pub := &next.Scopes[i].Publications[j]
			for _, a := range []*model.PlatformArtifact{&pub.Parent, &pub.Route, &pub.TLS, &pub.ProducerPolicy} {
				if previous, ok := contents[a.ID]; ok {
					reuseDNSArtifactContent(a, previous)
				}
			}
		}
	}
}
