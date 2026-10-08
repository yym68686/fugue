package frontnetwork

import (
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/model"
)

type LiveConnections struct {
	Count  int              `json:"count"`
	Active []LiveConnection `json:"active"`
}

type LiveConnection struct {
	ID               string                     `json:"id"`
	Protocol         string                     `json:"protocol,omitempty"`
	Slot             string                     `json:"slot,omitempty"`
	Target           string                     `json:"target,omitempty"`
	DownstreamRemote string                     `json:"downstream_remote,omitempty"`
	DownstreamLocal  string                     `json:"downstream_local,omitempty"`
	UpstreamLocal    string                     `json:"upstream_local,omitempty"`
	StartedAt        time.Time                  `json:"started_at"`
	ElapsedMS        int64                      `json:"elapsed_ms"`
	ProxyProtocol    bool                       `json:"proxy_protocol"`
	TCPInfo          map[string]json.RawMessage `json:"client_tcp_info,omitempty"`
}

func SampleFromLiveConnections(query Request, connections LiveConnections, observed time.Time) (model.EdgeClientNetworkSample, error) {
	if ValidateRequest(query) != nil || connections.Count != len(connections.Active) || connections.Count > 1024 || observed.IsZero() {
		return model.EdgeClientNetworkSample{}, errors.New("invalid bounded Front connection inventory")
	}
	peer, _ := ParseRemote(query.RemoteAddr)
	var matched *LiveConnection
	for index := range connections.Active {
		connection := &connections.Active[index]
		remote, err := ParseRemote(connection.DownstreamRemote)
		if err != nil || remote != peer || connection.Protocol != "https" || connection.Slot != query.Slot || !connection.ProxyProtocol {
			continue
		}
		if matched != nil {
			return model.EdgeClientNetworkSample{}, errors.New("ambiguous Front connection")
		}
		matched = connection
	}
	if matched == nil {
		return model.EdgeClientNetworkSample{}, errors.New("exact Front connection absent")
	}
	sample := model.EdgeClientNetworkSample{ConnectionID: matched.ID, Slot: matched.Slot, Scope: Scope(peer.Addr()), StartedAt: matched.StartedAt, ObservedAt: observed}
	if json.Unmarshal(matched.TCPInfo["tcp_info_available"], &sample.TCPInfoAvailable) != nil {
		return model.EdgeClientNetworkSample{}, errors.New("missing TCP_INFO availability")
	}
	if sample.TCPInfoAvailable {
		var state uint8
		if json.Unmarshal(matched.TCPInfo["tcp_state"], &state) != nil || state != 1 {
			return model.EdgeClientNetworkSample{}, errors.New("public TCP socket is not established")
		}
		for key, target := range map[string]**float64{"tcp_rtt_us": &sample.RTTMS, "tcp_min_rtt_us": &sample.MinRTTMS, "tcp_rttvar_us": &sample.RTTVarianceMS} {
			var micros uint32
			if json.Unmarshal(matched.TCPInfo[key], &micros) != nil {
				return model.EdgeClientNetworkSample{}, errors.New("missing kernel RTT field")
			}
			if micros > 0 {
				value := float64(micros) / 1000
				*target = &value
			}
		}
		for key, target := range map[string]any{"tcp_segs_out": &sample.SegmentsOut, "tcp_total_retrans": &sample.RetransmittedSegments, "tcp_bytes_sent": &sample.BytesSent, "tcp_bytes_retrans": &sample.BytesRetransmitted} {
			if json.Unmarshal(matched.TCPInfo[key], target) != nil {
				return model.EdgeClientNetworkSample{}, errors.New("missing kernel counter")
			}
		}
	}
	return sample, model.ValidateEdgeClientNetworkSample(&sample)
}
