// Package diagnosticprobe implements an independently released collector pack.
// The package ABI is intentionally independent from Fugue serving workloads.
// The Fugue API treats this pack's configuration as an opaque signed payload.
package diagnosticprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

type partialValue struct {
	Value     any
	Gaps      []string
	Truncated bool
}

type Config struct {
	Collectors []Collector `json:"collectors"`
}
type Collector struct {
	Name            string            `json:"name"`
	Kind            string            `json:"kind"`
	IntervalSeconds int               `json:"interval_seconds,omitempty"`
	Group           string            `json:"group,omitempty"`
	Version         string            `json:"version,omitempty"`
	Resource        string            `json:"resource,omitempty"`
	Namespace       string            `json:"namespace,omitempty"`
	ObjectName      string            `json:"object_name,omitempty"`
	Selector        string            `json:"selector,omitempty"`
	FieldSelector   string            `json:"field_selector,omitempty"`
	AnnotationKeys  []string          `json:"annotation_keys,omitempty"`
	Fields          []string          `json:"fields,omitempty"`
	Container       string            `json:"container,omitempty"`
	SinceSeconds    int               `json:"since_seconds,omitempty"`
	SinceTime       string            `json:"since_time,omitempty"`
	Match           []string          `json:"match,omitempty"`
	Service         *Service          `json:"service,omitempty"`
	Path            string            `json:"path,omitempty"`
	Queries         map[string]string `json:"queries,omitempty"`
	RequiredQueries []string          `json:"required_queries,omitempty"`
	Unit            string            `json:"unit,omitempty"`
	CaptureSeconds  int               `json:"capture_seconds,omitempty"`
	PolicyPath      string            `json:"policy_path,omitempty"`
}
type Service struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Port      string `json:"port"`
}

func Collect(parent context.Context, req livediagnostics.ProbeRequest) (livediagnostics.ProbeReport, error) {
	if req.Protocol != livediagnostics.CatalogProtocol || req.DurationSeconds < 5 || req.DurationSeconds > 360 || req.MaxOutputBytes < 64<<10 || req.MaxOutputBytes > 4<<20 {
		return livediagnostics.ProbeReport{}, errors.New("unsupported probe protocol or budget")
	}
	var cfg Config
	if err := livediagnostics.DecodeStrict(req.Config, &cfg); err != nil {
		return livediagnostics.ProbeReport{}, err
	}
	if len(cfg.Collectors) == 0 || len(cfg.Collectors) > 12 {
		return livediagnostics.ProbeReport{}, errors.New("collector count must be between 1 and 12")
	}
	seen := map[string]bool{}
	for _, c := range cfg.Collectors {
		if c.Name == "" || len(c.Name) > 80 || seen[c.Name] || c.IntervalSeconds < 0 || c.IntervalSeconds > 360 || len(c.Queries) > 12 || len(c.Fields) > 32 || len(c.Match) > 16 || len(c.AnnotationKeys) > 16 {
			return livediagnostics.ProbeReport{}, errors.New("invalid collector configuration or budget")
		}
		for _, name := range c.RequiredQueries {
			if _, ok := c.Queries[name]; !ok {
				return livediagnostics.ProbeReport{}, fmt.Errorf("required query %q is not configured", name)
			}
		}
		seen[c.Name] = true
	}
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(parent, time.Duration(req.DurationSeconds)*time.Second)
	defer cancel()
	report := livediagnostics.ProbeReport{ProbeImage: req.ProbeImage, Schema: "fugue.diagnostic.probe_report.v1", SessionID: req.SessionID, ProbeID: req.ProbeID, ProbeDigest: req.ProbeDigest, CatalogDigest: req.CatalogDigest, Target: req.Target, StartedAt: started, Quality: livediagnostics.EvidenceQuality{Status: "complete", Gaps: []string{}}, Evidence: []livediagnostics.Evidence{}}
	next := make([]time.Time, len(cfg.Collectors))
	visited := make([]bool, len(cfg.Collectors))
	remaining := req.MaxOutputBytes - (32 << 10)
	attempts := 0
	client := &kubeReader{}
	for ctx.Err() == nil && attempts < 120 {
		for i, c := range cfg.Collectors {
			if ctx.Err() != nil {
				break
			}
			if !next[i].IsZero() && time.Now().Before(next[i]) {
				continue
			}
			attempts++
			visited[i] = true
			observed := time.Now().UTC()
			e := livediagnostics.Evidence{Name: c.Name, Source: c.Kind, ObservedAt: observed, Status: "complete"}
			budget := 8 * time.Second
			if c.Kind == "process-cpu-profile" || c.Kind == "perf-capture-check" {
				budget = time.Duration(req.DurationSeconds-1) * time.Second
			}
			budgetCtx, stop := context.WithTimeout(ctx, budget)
			value, err := collectOne(budgetCtx, req, c, client)
			stop()
			if partial, ok := value.(partialValue); ok {
				value = partial.Value
				e.Status = "degraded"
				report.Quality.Status = "degraded"
				report.Quality.Truncated = report.Quality.Truncated || partial.Truncated
				for _, gap := range partial.Gaps {
					appendGap(&report, c.Name+": "+gap)
				}
			}
			if err != nil {
				e.Status = "unavailable"
				e.Error = boundedError(err)
				report.Quality.Status = "degraded"
				appendGap(&report, c.Name+": "+e.Error)
			} else {
				data, err := json.Marshal(value)
				if err != nil {
					e.Status = "unavailable"
					e.Error = "collector returned invalid JSON"
					report.Quality.Status = "degraded"
					appendGap(&report, c.Name+": invalid JSON")
				} else {
					e.Data = data
				}
			}
			encoded, _ := json.Marshal(e)
			if len(encoded) > remaining {
				report.Quality.Truncated = true
				report.Quality.Status = "degraded"
				appendGap(&report, "output byte budget exhausted")
				report.FinishedAt = time.Now().UTC()
				return report, nil
			}
			remaining -= len(encoded) + 1
			report.Evidence = append(report.Evidence, e)
			interval := c.IntervalSeconds
			if interval == 0 {
				interval = 10
			}
			if interval < 5 {
				interval = 5
			}
			next[i] = time.Now().Add(time.Duration(interval) * time.Second)
		}
		wait := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !wait.Stop() {
				<-wait.C
			}
		case <-wait.C:
		}
	}
	if attempts >= 120 {
		report.Quality.Status = "degraded"
		appendGap(&report, "request count budget exhausted")
	}
	if parent.Err() != nil {
		report.Quality.Status = "degraded"
		appendGap(&report, "session canceled")
	}
	for i, observed := range visited {
		if !observed {
			report.Quality.Status = "degraded"
			appendGap(&report, cfg.Collectors[i].Name+": not sampled before the session deadline")
		}
	}
	report.FinishedAt = time.Now().UTC()
	appendWindowSummary(&report)
	return report, nil
}
func collectOne(ctx context.Context, req livediagnostics.ProbeRequest, c Collector, k *kubeReader) (any, error) {
	switch c.Kind {
	case "node-snapshot":
		return nodeSnapshot(req)
	case "process-scheduling":
		return processSchedulingAt(ctx, req, hostProc)
	case "process-identity":
		return processIdentities(ctx, req, hostProc)
	case "process-cpu-profile":
		return processCPUProfile(ctx, req, c)
	case "perf-capture-check":
		return perfCaptureCheck(ctx, req)
	case "host-journal":
		return hostJournal(ctx, req, c)
	case "kubernetes-audit":
		return hostKubernetesAudit(ctx, req, c)
	case "kubernetes-objects":
		return k.objects(ctx, req, c)
	case "kubernetes-logs":
		return k.logs(ctx, req, c)
	case "prometheus":
		return k.prometheus(ctx, req, c)
	case "service-json":
		return k.serviceJSON(ctx, req, c)
	case "runtime-json":
		return runtimeJSON(ctx, req, c)
	default:
		return nil, fmt.Errorf("collector %q is not supported by this package image", c.Kind)
	}
}
func boundedError(err error) string {
	s := err.Error()
	if len(s) > 400 {
		s = s[:400]
	}
	return s
}
func appendGap(r *livediagnostics.ProbeReport, g string) {
	for _, old := range r.Quality.Gaps {
		if old == g {
			return
		}
	}
	if len(r.Quality.Gaps) < 32 {
		r.Quality.Gaps = append(r.Quality.Gaps, g)
	}
}
func parameter(s string, req livediagnostics.ProbeRequest) string {
	s = strings.ReplaceAll(s, "{{node}}", req.Target.Node)
	s = strings.ReplaceAll(s, "{{namespace}}", req.Target.Namespace)
	s = strings.ReplaceAll(s, "{{pod}}", req.Target.Pod)
	for k, v := range req.Parameters {
		s = strings.ReplaceAll(s, "{{param."+k+"}}", v)
	}
	return s
}
