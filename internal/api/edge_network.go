package api

import (
	"time"

	"fugue/internal/model"
)

func sanitizeEdgeNetworkSamples(req edgeHeartbeatRequest, servingActive *bool, now time.Time) []model.EdgeNetworkSample {
	if servingActive == nil || !*servingActive || len(req.NetworkSamples) > 128 {
		return nil
	}
	samples := []model.EdgeNetworkSample{}
	for _, sample := range req.NetworkSamples {
		if !model.EdgeNetworkServiceSource(sample.Source) && sample.Source != "public_front_tcp_info_v1" {
			continue
		}
		if sample.EdgeID != req.EdgeID || sample.EdgeGroupID != req.EdgeGroupID || sample.BundleVersion != req.RouteBundleVersion || sample.ObservedAt.After(now) || sample.ObservedAt.Before(now.Add(-time.Hour)) || model.ValidateEdgeNetworkSample(sample) != nil {
			continue
		}
		samples = append(samples, sample)
	}
	return samples
}
