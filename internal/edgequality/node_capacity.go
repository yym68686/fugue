package edgequality

import (
	"errors"
	"net/netip"
	"strings"
	"time"

	"fugue/internal/model"
)

type NodeCapacitySample struct {
	ID          string                        `json:"id"`
	EdgeID      string                        `json:"edge_id"`
	EdgeGroupID string                        `json:"edge_group_id"`
	Address     string                        `json:"address"`
	Capacity    model.EdgeNetworkNodeCapacity `json:"capacity"`
}

func ValidateNodeCapacitySample(sample NodeCapacitySample, now time.Time) error {
	for _, identity := range []string{sample.ID, sample.EdgeID, sample.EdgeGroupID} {
		if identity == "" || len(identity) > 128 || strings.ContainsAny(identity, " \t\r\n\x00") {
			return errors.New("invalid physical-node capacity identity")
		}
	}
	address, err := netip.ParseAddr(sample.Address)
	if err != nil || !address.IsGlobalUnicast() || address.IsPrivate() || address.Is4In6() || address.String() != sample.Address ||
		model.ValidateEdgeNetworkNodeCapacity(&sample.Capacity) != nil || sample.Capacity.CPUObservedAt.After(now) || sample.Capacity.MemoryObservedAt.After(now) {
		return errors.New("invalid physical-node capacity facts")
	}
	return nil
}

func NodeCapacityObservation(snapshot Snapshot, candidate Candidate, sample NodeCapacitySample) (Observation, bool) {
	value, err := model.EdgeNetworkNodeUtilization(&sample.Capacity)
	if err != nil || ValidateNodeCapacitySample(sample, snapshot.CapturedAt) != nil || !sample.Capacity.ValidUntil.After(snapshot.CapturedAt) || candidate.EdgeID != sample.EdgeID || candidate.EdgeGroupID != sample.EdgeGroupID ||
		!proofFresh(candidate, snapshot.Policy, snapshot.CapturedAt) {
		return Observation{}, false
	}
	return Observation{ID: "node-capacity:" + sample.EdgeID + ":" + sample.ID, NodeCapacityID: sample.ID,
		EdgeID: sample.EdgeID, Hostname: snapshot.Hostname, TrafficClass: snapshot.TrafficClass, Scope: snapshot.Scope,
		RouteGeneration: candidate.RouteGeneration, ObservedAt: sample.Capacity.ObservedAt,
		CapacitySource: sample.Capacity.Source, CapacityUtilization: &value}, true
}

func NodeCapacityObservationMatches(snapshot Snapshot, observation Observation, sample NodeCapacitySample) bool {
	if observation.NodeCapacityID != sample.ID || observation.ID != "node-capacity:"+sample.EdgeID+":"+sample.ID || observation.RouteWitnessID != "" ||
		observation.EdgeID != sample.EdgeID || observation.Hostname != snapshot.Hostname || observation.TrafficClass != snapshot.TrafficClass || observation.Scope != snapshot.Scope ||
		observation.ClientSource != "" || observation.ServiceSource != "" || observation.ClientCohort != "" ||
		observation.ClientNetworkMS != nil || observation.ServiceNetworkMS != nil || observation.UploadBPS != nil || observation.DownloadBPS != nil || observation.ClientFailureRate != nil || observation.ServiceFailureRate != nil || observation.ClientRetransmissionRate != nil {
		return false
	}
	for _, candidate := range snapshot.Candidates {
		if candidate.EdgeID != sample.EdgeID {
			continue
		}
		derived, ok := NodeCapacityObservation(snapshot, candidate, sample)
		return ok && observation.RouteGeneration == derived.RouteGeneration && observation.ObservedAt.Equal(derived.ObservedAt) &&
			observation.CapacitySource == derived.CapacitySource && observation.CapacityUtilization != nil && *observation.CapacityUtilization == *derived.CapacityUtilization
	}
	return false
}
