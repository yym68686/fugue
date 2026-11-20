// Package relayroute contains the bounded, replayable origin path selector.
package relayroute

import "time"

type State string

const (
	Unknown    State = "unknown"
	Healthy    State = "healthy"
	Suspect    State = "suspect"
	Degraded   State = "degraded"
	Recovering State = "recovering"
)

const (
	EvidenceTTL  = 45 * time.Second
	RecoveryHold = 60 * time.Second
)

type Sample struct {
	At time.Time
	// Round identifies one probe sweep; aliases of a Service are not votes.
	Round  uint64
	Failed bool
	RTT    time.Duration
}

type Health struct {
	State   State
	At      time.Time
	Samples int
	RTT     time.Duration
}

type Window struct {
	rows     [3]Sample
	last     uint64
	n        int
	badAt    time.Time
	recovery int
}

func (w *Window) Add(x Sample, now time.Time) {
	if x.Round == 0 || x.Round <= w.last || x.At.IsZero() ||
		x.At.After(now) || now.Sub(x.At) >= EvidenceTTL ||
		(w.n > 0 && x.At.Before(w.rows[0].At)) {
		return
	}
	w.last = x.Round
	copy(w.rows[1:], w.rows[:len(w.rows)-1])
	w.rows[0] = x
	w.n = min(w.n+1, len(w.rows))
	if x.Failed {
		w.recovery = 0
	} else if !w.badAt.IsZero() {
		w.recovery = min(w.recovery+1, 3)
	}
	if w.snapshot(now).State == Degraded && x.Failed {
		w.badAt = now
	}
	if !w.badAt.IsZero() && w.recovery == 3 && now.Sub(w.badAt) >= RecoveryHold {
		w.badAt = time.Time{}
		w.recovery = 0
	}
}

func (w Window) snapshot(now time.Time) Health {
	h := Health{State: Unknown}
	bad := 0
	for _, x := range w.rows[:w.n] {
		if x.At.After(now) || now.Sub(x.At) >= EvidenceTTL {
			continue
		}
		// Advertise the oldest evidence contributing to the result, not
		// just the most recent successful probe. A cached remote verdict
		// must not outlive a required sample.
		h.At = x.At
		h.Samples++
		if x.Failed {
			bad++
		} else if h.RTT == 0 {
			h.RTT = x.RTT
		}
	}
	switch {
	case bad >= 2:
		h.State = Degraded
	case h.Samples == 0:
	case !w.badAt.IsZero() && (w.recovery < 3 || now.Sub(w.badAt) < RecoveryHold):
		h.State = Recovering
	case bad > 0:
		h.State = Suspect
	case h.Samples >= 2:
		h.State = Healthy
	}
	return h
}

func (w Window) Health(now time.Time) Health { return w.snapshot(now) }

type Candidate struct {
	ID           string
	Link, Origin Health
	Capacity     int
}

type Plan struct{ Peer, Reason string }

func fresh(h Health, now time.Time) bool {
	return h.Samples >= 2 && !h.At.IsZero() && !h.At.After(now) && now.Sub(h.At) < EvidenceTTL
}

func Select(now time.Time, local Health, candidates []Candidate) Plan {
	if local.State != Degraded && local.State != Recovering || !fresh(local, now) {
		return Plan{Reason: "direct_not_proven_degraded"}
	}
	var best Candidate
	for _, c := range candidates {
		if c.ID == "" || c.Capacity <= 0 || c.Link.State != Healthy || c.Origin.State != Healthy ||
			!fresh(c.Link, now) || !fresh(c.Origin, now) {
			continue
		}
		cost, prior := cost(c), cost(best)
		if best.ID == "" || cost < prior || cost == prior && (c.Capacity > best.Capacity ||
			c.Capacity == best.Capacity && c.ID < best.ID) {
			best = c
		}
	}
	if best.ID == "" {
		return Plan{Reason: "no_fresh_authorized_peer"}
	}
	return Plan{Peer: best.ID, Reason: "degraded_origin_healthy_single_hop"}
}

func cost(c Candidate) time.Duration {
	if c.Link.RTT <= 0 || c.Origin.RTT <= 0 {
		return time.Hour
	}
	return c.Link.RTT + c.Origin.RTT
}
