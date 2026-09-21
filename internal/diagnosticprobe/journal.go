package diagnosticprobe

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

var journalUnitPattern = regexp.MustCompile(`^[a-zA-Z0-9_@.:-]{1,120}\.service$`)

func hostJournal(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	c.Unit = parameter(c.Unit, req)
	if req.Target.Type != livediagnostics.TargetNodeProcess || !journalUnitPattern.MatchString(c.Unit) || c.SinceSeconds < 1 || c.SinceSeconds > 86400 {
		return nil, errors.New("journal observation requires a process target, explicit service unit and a lookback within 24 hours")
	}
	until := time.Now().UTC()
	since := until.Add(-time.Duration(c.SinceSeconds) * time.Second)
	results := []any{}
	gaps := []string{}
	for _, path := range []string{"var/log/journal", "run/log/journal"} {
		dir := filepath.Join(hostProc, "1/root", path)
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			gaps = append(gaps, "journal directory unavailable: "+path)
			continue
		}
		if len(entries) == 0 {
			continue
		}
		raw, truncated, err := diagnosticCommand(ctx, 2<<20, "journalctl", "--directory="+dir, "--unit="+c.Unit, "--since="+since.Format(time.RFC3339), "--until="+until.Format(time.RFC3339), "--lines=2000", "--reverse", "--output=json", "--no-pager", "--quiet")
		if err != nil {
			gaps = append(gaps, fmt.Sprintf("journal reader for %s failed: %v", path, err))
			continue
		}
		value, parseErr := journalEvidence(raw, c.Match)
		if parseErr != nil {
			gaps = append(gaps, parseErr.Error())
		}
		value["directory"] = path
		if truncated || value["entries"] == 2000 {
			gaps = append(gaps, "journal entry or byte limit reached; source coverage is partial")
		}
		results = append(results, value)
	}
	if len(results) == 0 {
		gaps = append(gaps, "no readable host journal source")
	}
	result := map[string]any{"unit": c.Unit, "since": since, "until": until, "sources": results, "entry_limit_per_source": 2000, "byte_limit_per_source": 2 << 20}
	if len(gaps) > 0 {
		return partialValue{Value: result, Gaps: gaps, Truncated: strings.Contains(strings.Join(gaps, " "), "limit reached")}, nil
	}
	return result, nil
}

func journalEvidence(raw []byte, patterns []string) (map[string]any, error) {
	counts := map[string]int{}
	examples := []any{}
	rows := 0
	var oldest, newest time.Time
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 4096), 256<<10)
	var parseErr error
	for scanner.Scan() {
		var entry map[string]json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			parseErr = errors.New("journal contains a partial or invalid JSON entry")
			break
		}
		var message, timestamp, boot string
		if json.Unmarshal(entry["MESSAGE"], &message) != nil || json.Unmarshal(entry["__REALTIME_TIMESTAMP"], &timestamp) != nil {
			parseErr = errors.New("journal contains an unsupported message or timestamp")
			continue
		}
		_ = json.Unmarshal(entry["_BOOT_ID"], &boot)
		micros, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil {
			parseErr = errors.New("journal contains an invalid timestamp")
			continue
		}
		ts := time.UnixMicro(micros).UTC()
		rows++
		if oldest.IsZero() || ts.Before(oldest) {
			oldest = ts
		}
		if ts.After(newest) {
			newest = ts
		}
		matched := len(patterns) == 0
		for _, pattern := range patterns {
			if strings.Contains(message, pattern) {
				counts[pattern]++
				matched = true
			}
		}
		if matched && len(examples) < 40 {
			safe := safeText(message)
			if len(safe) > 2000 {
				safe = safe[:2000]
			}
			examples = append(examples, map[string]any{"at": ts, "boot_id": boot, "message": safe})
		}
	}
	if scanner.Err() != nil {
		parseErr = errors.New("journal entry exceeds the parser byte budget")
	}
	return map[string]any{"entries": rows, "oldest_observed": oldest, "newest_observed": newest, "pattern_counts": counts, "examples": examples}, parseErr
}
