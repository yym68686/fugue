package diagnosticprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"fugue/internal/livediagnostics"
)

func (k *kubeReader) objectChanges(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	name, namespace := parameter(c.ObjectName, req), parameter(c.Namespace, req)
	if name == "" || namespace == "" || namespace == "*" || c.CaptureSeconds < 1 || c.CaptureSeconds > 5 {
		return nil, errors.New("object watch requires an exact namespaced object and 1-5 second window")
	}
	c.Group = parameter(c.Group, req)
	initial, err := k.objects(ctx, req, c)
	if err != nil {
		return nil, err
	}
	object, ok := initial.(map[string]any)
	if !ok {
		return nil, errors.New("initial watch object unavailable")
	}
	meta, _ := object["metadata"].(map[string]any)
	rv, _ := meta["resourceVersion"].(string)
	if rv == "" {
		return nil, errors.New("initial object has no resource version")
	}
	version := c.Version
	if version == "" {
		version = "v1"
	}
	endpoint := "/api/" + version
	if c.Group != "" {
		endpoint = "/apis/" + c.Group + "/" + version
	}
	endpoint += "/namespaces/" + url.PathEscape(namespace) + "/" + parameter(c.Resource, req)
	query := url.Values{"watch": {"true"}, "fieldSelector": {"metadata.name=" + name}, "resourceVersion": {rv}, "timeoutSeconds": {fmt.Sprint(c.CaptureSeconds)}}
	r, err := http.NewRequestWithContext(ctx, "GET", k.base+endpoint+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", "Bearer "+k.token)
	if k.requests >= 240 {
		return nil, errors.New("collector HTTP request budget exhausted")
	}
	k.requests++
	resp, err := k.client.Do(r)
	if err != nil {
		return nil, fmt.Errorf("object watch: %s", safeText(err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("object watch returned HTTP %d", resp.StatusCode)
	}
	bounded := &io.LimitedReader{R: resp.Body, N: 2 << 20}
	decoder := json.NewDecoder(bounded)
	decoder.UseNumber()
	rows := []any{}
	result := map[string]any{"initial": initial, "events": rows, "initial_resource_version": rv, "capture_seconds": c.CaptureSeconds}
	for i := 0; i < 64; i++ {
		var event struct {
			Type   string         `json:"type"`
			Object map[string]any `json:"object"`
		}
		err := decoder.Decode(&event)
		if errors.Is(err, io.EOF) {
			if bounded.N == 0 {
				return partialValue{Value: result, Gaps: []string{"object watch byte budget exhausted"}, Truncated: true}, nil
			}
			return result, nil
		}
		if err != nil {
			return partialValue{Value: result, Gaps: []string{"object watch ended before its source window: " + boundedError(err)}}, nil
		}
		if event.Type == "ERROR" {
			return partialValue{Value: result, Gaps: []string{"Kubernetes watch source returned an error event"}}, nil
		}
		metadata, _ := event.Object["metadata"].(map[string]any)
		if metadata["name"] != name || metadata["namespace"] != namespace {
			return nil, errors.New("watch object identity does not match selected object")
		}
		switch event.Type {
		case "ADDED", "MODIFIED", "DELETED":
		default:
			return nil, errors.New("unexpected Kubernetes watch event")
		}
		rows = append(rows, map[string]any{"type": event.Type, "object": projectConfiguredObject(event.Object, c)})
		result["events"] = rows
	}
	return partialValue{Value: result, Gaps: []string{"object watch event budget exhausted"}, Truncated: true}, nil
}
