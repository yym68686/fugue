package diagnosticprobe

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

type auditEvent struct {
	Stage string `json:"stage"`
	ID    string `json:"auditID"`
	Verb  string `json:"verb"`
	URI   string `json:"requestURI"`
	Agent string `json:"userAgent"`
	User  struct {
		Name string `json:"username"`
	} `json:"user"`
	Object struct {
		Resource    string `json:"resource"`
		Subresource string `json:"subresource"`
		Namespace   string `json:"namespace"`
	} `json:"objectRef"`
	Response struct {
		Code int `json:"code"`
	} `json:"responseStatus"`
	Received time.Time `json:"requestReceivedTimestamp"`
	Finished time.Time `json:"stageTimestamp"`
}

type auditGroup struct {
	User           string         `json:"user"`
	Agent          string         `json:"agent"`
	Verb           string         `json:"verb"`
	Resource       string         `json:"resource"`
	Subresource    string         `json:"subresource"`
	Code           int            `json:"code"`
	Count          int            `json:"count"`
	DurationMillis float64        `json:"duration_total_ms"`
	MaxMillis      float64        `json:"duration_max_ms"`
	Options        map[string]int `json:"query_options"`
}

func hostKubernetesAudit(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	return hostKubernetesAuditAt(ctx, req, c, filepath.Join(hostProc, "1/root"))
}

func hostKubernetesAuditAt(ctx context.Context, req livediagnostics.ProbeRequest, c Collector, root string) (any, error) {
	if req.Target.Type != livediagnostics.TargetNodeProcess || !strings.HasPrefix(c.Path, "/var/log/") || filepath.Clean(c.Path) != c.Path || c.SinceSeconds < 1 || c.SinceSeconds > 86400 {
		return nil, errors.New("audit reader requires a process target, log path pattern and a lookback within 24 hours")
	}
	paths, err := filepath.Glob(filepath.Join(root, c.Path))
	if err != nil {
		return nil, err
	}
	files := []struct {
		path string
		info os.FileInfo
	}{}
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err == nil && info.Mode().IsRegular() {
			files = append(files, struct {
				path string
				info os.FileInfo
			}{path, info})
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].info.ModTime().After(files[j].info.ModTime()) })
	until := time.Now().UTC()
	since := until.Add(-time.Duration(c.SinceSeconds) * time.Second)
	groups := map[string]*auditGroup{}
	sources := []any{}
	gaps := []string{}
	seen := map[string]bool{}
	var oldest time.Time
	remaining := int64(32 << 20)
	matched := 0
	for index, source := range files {
		if index >= 4 || remaining <= 0 || ctx.Err() != nil {
			gaps = append(gaps, "audit source budget exhausted")
			break
		}
		if source.info.ModTime().Before(since) {
			continue
		}
		f, err := os.Open(source.path)
		if err != nil {
			gaps = append(gaps, "audit file unavailable: "+boundedError(err))
			continue
		}
		limit := min(remaining, source.info.Size())
		offset := source.info.Size() - limit
		_, err = f.Seek(offset, io.SeekStart)
		if err != nil {
			f.Close()
			gaps = append(gaps, "audit file seek failed")
			continue
		}
		scanner := bufio.NewScanner(io.LimitReader(f, limit))
		scanner.Buffer(make([]byte, 64<<10), 1<<20)
		if offset > 0 {
			scanner.Scan()
		}
		rows := 0
		for scanner.Scan() {
			if ctx.Err() != nil {
				gaps = append(gaps, "audit parsing deadline reached")
				break
			}
			var event auditEvent
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				gaps = append(gaps, "audit source contains an incomplete or invalid entry")
				break
			}
			rows++
			if event.Finished.IsZero() {
				gaps = append(gaps, "audit entry has no stage timestamp")
				continue
			}
			if oldest.IsZero() || event.Finished.Before(oldest) {
				oldest = event.Finished
			}
			if event.Stage != "ResponseComplete" || event.Finished.Before(since) || event.Finished.After(until) {
				continue
			}
			if event.ID == "" || seen[event.ID] {
				continue
			}
			if len(seen) >= 30000 {
				gaps = append(gaps, "audit event budget exhausted")
				break
			}
			seen[event.ID] = true
			if !aggregateAuditEvent(groups, event) {
				gaps = append(gaps, "audit group budget exhausted")
				continue
			}
			matched++
		}
		if scanner.Err() != nil {
			gaps = append(gaps, "audit scanner byte limit or read error")
		}
		f.Close()
		remaining -= limit
		sources = append(sources, map[string]any{"file": filepath.Base(source.path), "size_bytes": source.info.Size(), "read_offset": offset, "read_limit_bytes": limit, "entries": rows})
		if !oldest.IsZero() && !oldest.After(since) {
			break
		}
	}
	if len(sources) == 0 {
		gaps = append(gaps, "no readable audit files matched")
	}
	if oldest.IsZero() || oldest.After(since) {
		gaps = append(gaps, "audit source does not cover the entire requested window")
	}
	rows := make([]*auditGroup, 0, len(groups))
	for _, group := range groups {
		rows = append(rows, group)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Count == rows[j].Count {
			return rows[i].User+rows[i].Resource+rows[i].Verb < rows[j].User+rows[j].Resource+rows[j].Verb
		}
		return rows[i].Count > rows[j].Count
	})
	result := map[string]any{"since": since, "until": until, "oldest_observed": oldest, "completed_requests": matched, "groups": rows, "sources": sources, "note": "ResponseComplete only; watch durations include stream lifetime; overlapping windows must not be summed"}
	if len(gaps) > 0 {
		return partialValue{Value: result, Gaps: gaps, Truncated: true}, nil
	}
	return result, nil
}

func aggregateAuditEvent(groups map[string]*auditGroup, e auditEvent) bool {
	bound := func(s string) string {
		s = safeText(s)
		if len(s) > 240 {
			s = s[:240]
		}
		return s
	}
	user, agent := bound(e.User.Name), bound(e.Agent)
	key := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%d", user, agent, e.Verb, e.Object.Resource, e.Object.Subresource, e.Response.Code)
	g := groups[key]
	if g == nil {
		if len(groups) >= 256 {
			return false
		}
		g = &auditGroup{User: user, Agent: agent, Verb: e.Verb, Resource: e.Object.Resource, Subresource: e.Object.Subresource, Code: e.Response.Code, Options: map[string]int{}}
		groups[key] = g
	}
	g.Count++
	if !e.Received.IsZero() && !e.Finished.Before(e.Received) {
		ms := float64(e.Finished.Sub(e.Received)) / float64(time.Millisecond)
		g.DurationMillis += ms
		g.MaxMillis = max(g.MaxMillis, ms)
	}
	if uri, err := url.Parse(e.URI); err == nil {
		q := uri.Query()
		for _, key := range []string{"resourceVersion", "resourceVersionMatch", "limit", "watch", "labelSelector", "fieldSelector"} {
			if !q.Has(key) {
				continue
			}
			value := q.Get(key)
			label := key + "=present"
			if value == "" {
				label = key + "=empty"
			} else if key == "resourceVersion" && value == "0" {
				label = key + "=0"
			} else if key == "watch" && value == "true" {
				label = key + "=true"
			}
			g.Options[label]++
		}
	}
	return true
}
