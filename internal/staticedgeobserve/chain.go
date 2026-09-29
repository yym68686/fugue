package staticedgeobserve

// Chain joins authenticated identities, never wall-clock differences. A link
// proves parentage; it does not prove one-way network delay across hosts.
type Link struct {
	Parent    string `json:"parent"`
	Child     string `json:"child"`
	RequestID string `json:"request_id"`
}
type Chain struct {
	Links                      []Link   `json:"links"`
	Missing                    []string `json:"missing"`
	Complete                   bool     `json:"complete"`
	CrossHostDurationsComputed bool     `json:"cross_host_durations_computed"`
}

func Join(results []Result) Chain {
	out := Chain{Links: []Link{}, Missing: []string{}, Complete: true}
	spans := map[string]Record{}
	ambiguous := map[string]bool{}
	for _, result := range results {
		if result.Truncated || result.RecordsDropped > 0 || result.DiskErrors > 0 || !result.DiskScanComplete {
			out.Complete = false
			out.Missing = append(out.Missing, result.NodeID+": collection or query incomplete")
		}
		for _, r := range result.Records {
			if prev, ok := spans[r.SpanID]; ok && (prev.ProcessID != r.ProcessID || prev.NodeID != r.NodeID || prev.RequestID != r.RequestID) {
				ambiguous[r.SpanID] = true
				continue
			}
			spans[r.SpanID] = r
		}
	}
	for _, r := range spans {
		if !r.Finished || r.EventsDropped+r.SnapshotsDropped > 0 || (r.HTTP2 != nil && r.HTTP2.Dropped > 0) {
			out.Complete = false
			out.Missing = append(out.Missing, r.SpanID+": terminal or loss boundary")
		}
		if r.ParentSpanID == "" {
			continue
		}
		p, ok := spans[r.ParentSpanID]
		trusted := r.Correlation == "authenticated_parent" || (r.Correlation == "local_parent" && p.NodeID == r.NodeID && p.ProcessID == r.ProcessID)
		if !ok || p.RequestID != r.RequestID || !trusted || ambiguous[r.SpanID] || ambiguous[p.SpanID] {
			out.Complete = false
			out.Missing = append(out.Missing, r.SpanID+": missing or ambiguous trusted parent")
			continue
		}
		out.Links = append(out.Links, Link{Parent: p.SpanID, Child: r.SpanID, RequestID: r.RequestID})
	}
	if len(spans) == 0 {
		out.Complete = false
		out.Missing = append(out.Missing, "no retained observations")
	}
	return out
}
