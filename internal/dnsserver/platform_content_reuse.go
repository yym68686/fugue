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
func reuseDNSArtifactContent(next *model.PlatformArtifact, previous model.PlatformArtifact) {
	if next.ID == previous.ID && next.ContentHash == previous.ContentHash && reflect.DeepEqual(next.Content, previous.Content) {
		next.Content = previous.Content
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
