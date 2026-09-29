package staticedgemanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	c "fugue/internal/staticedgecontract"
	o "fugue/internal/staticedgeobserve"
)

func (m *Manager) observationQuery(ctx context.Context, req c.Request) c.Response {
	if m.cfg.ObservationSocket == "" {
		return m.fail(req, 503, errors.New("independent collector not configured; serving is unaffected"))
	}
	client, e := o.UnixClient(m.cfg.ObservationSocket, 3*time.Second)
	if e != nil {
		return m.fail(req, 503, errors.New("collector unavailable"))
	}
	defer client.CloseIdleConnections()
	method, path := http.MethodGet, "/status"
	var body []byte
	if req.Operation == "request-query" {
		if req.ObservationQuery == nil || req.ObservationQuery.Validate() != nil {
			return m.fail(req, 400, errors.New("valid bounded observation_query required"))
		}
		method, path = http.MethodPost, "/query"
		body, _ = json.Marshal(req.ObservationQuery)
	}
	r, e := http.NewRequestWithContext(ctx, method, "http://localhost"+path, bytes.NewReader(body))
	if e != nil {
		return m.fail(req, 503, errors.New("collector query unavailable"))
	}
	r.Header.Set("Content-Type", "application/json")
	resp, e := client.Do(r)
	if e != nil {
		return m.fail(req, 503, errors.New("collector unavailable; no cause inferred"))
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return m.fail(req, 503, errors.New("collector could not provide evidence"))
	}
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 3<<20+1))
	if e != nil || len(raw) > 3<<20 {
		return m.fail(req, 503, errors.New("collector reply exceeded evidence budget"))
	}
	out := m.base(req)
	if req.Operation == "observability-status" {
		if json.Unmarshal(raw, &out.ObservationStatus) != nil || out.ObservationStatus["node_id"] != m.cfg.EdgeID {
			return m.fail(req, 503, errors.New("collector identity mismatch"))
		}
	} else {
		var v o.Result
		if json.Unmarshal(raw, &v) != nil || v.NodeID != m.cfg.EdgeID || v.Schema != o.Schema {
			return m.fail(req, 503, errors.New("collector identity or schema mismatch"))
		}
		out.Observations = &v
	}
	return out
}
