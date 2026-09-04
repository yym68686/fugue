package edge

import (
	"fmt"
	"io"
	"log"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestRouteIndexPublishesRouteAndAttestationTogether(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{EdgeGroupID: "edge-group-default"}, log.New(io.Discard, "", 0))
	bundle := testBundle("publication-a")
	publicationA := routePublicationMetadata{CandidateRecord: testEdgeRouteDigestA, ReleaseRecord: testEdgeRouteDigestB, WorkerSlot: model.EdgeSlotA}
	service.recordSyncSuccessWithPublication(bundle, "a", bundle.GeneratedAt, false, publicationA)
	route, ok, _, version, gotPublication := service.routeForRequestWithBundle("demo.fugue.pro", "/")
	if !ok || route.Hostname != "demo.fugue.pro" || version != "publication-a" || gotPublication != publicationA {
		t.Fatalf("first atomic route snapshot mismatch: route=%+v ok=%t version=%q publication=%+v", route, ok, version, gotPublication)
	}

	bundle.Version = "publication-b"
	publicationB := routePublicationMetadata{CandidateRecord: testEdgeRouteDigestB, ReleaseRecord: testEdgeRouteDigestA, WorkerSlot: model.EdgeSlotB}
	service.recordSyncSuccessWithPublication(bundle, "b", bundle.GeneratedAt, false, publicationB)
	_, ok, _, version, gotPublication = service.routeForRequestWithBundle("demo.fugue.pro", "/")
	if !ok || version != "publication-b" || gotPublication != publicationB {
		t.Fatalf("second atomic route snapshot mismatch: ok=%t version=%q publication=%+v", ok, version, gotPublication)
	}
}

func BenchmarkRouteForRequest(b *testing.B) {
	for _, routeCount := range []int{1, 100, 10_000} {
		b.Run(fmt.Sprintf("routes_%d", routeCount), func(b *testing.B) {
			service := NewService(config.EdgeConfig{EdgeGroupID: "edge-group-default"}, log.New(io.Discard, "", 0))
			bundle := testBundle("benchmark")
			bundle.Routes = make([]model.EdgeRouteBinding, 0, routeCount)
			for index := 0; index < routeCount; index++ {
				route := testBundle("benchmark").Routes[0]
				route.Hostname = fmt.Sprintf("host-%05d.example.test", index)
				route.PathPrefix = "/"
				bundle.Routes = append(bundle.Routes, route)
			}
			service.recordSyncSuccess(bundle, "benchmark", bundle.GeneratedAt, false)
			target := fmt.Sprintf("host-%05d.example.test", routeCount-1)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, ok, _ := service.routeForRequest(target, "/assets/app.js"); !ok {
						b.Fatal("benchmark route was not found")
					}
				}
			})
		})
	}
}
