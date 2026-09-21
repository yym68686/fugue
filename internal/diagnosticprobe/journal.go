package diagnosticprobe

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	since, until, err := journalWindow(c, req, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	if req.Target.Type != livediagnostics.TargetNodeProcess || !journalUnitPattern.MatchString(c.Unit) {
		return nil, errors.New("journal observation requires a process target, explicit service unit and a lookback within 24 hours")
	}
	if literal := optionalJournalParameter(c.MatchParam, req); literal != "" {
		c.Match = []string{literal}
	}
	filter, err := journalFilterArguments(c.Match)
	if err != nil {
		return nil, err
	}
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
		args := []string{"--directory=" + dir, "--unit=" + c.Unit, "--since=" + journalTimestamp(since), "--until=" + journalTimestamp(until), "--lines=2000", "--reverse", "--output=json", "--output-fields=MESSAGE,__REALTIME_TIMESTAMP,_BOOT_ID", "--no-pager", "--quiet"}
		args = append(args, filter...)
		filtered := len(filter) > 0
		raw, stderr, truncated, err := diagnosticCommandEvidence(ctx, 2<<20, "journalctl", args...)
		// journalctl returns 1 when a grep query has no matches. Other
		// failures, including stderr and cancellation, remain unavailable.
		if ctx.Err() == nil && journalNoMatches(err, raw, stderr, filtered) {
			err = nil
		}
		if err != nil {
			gaps = append(gaps, fmt.Sprintf("journal reader for %s failed: %v", path, err))
			continue
		}
		value, parseErr := journalEvidence(raw, c.Match)
		if parseErr != nil {
			gaps = append(gaps, parseErr.Error())
		}
		value["directory"] = path
		value["filtered_by_match"] = filtered
		value["match_literals"] = c.Match
		value["coverage_scope"] = "retained journal records in the requested window matching the stated literals; rotation and unlogged events are not reconstructable"
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

func optionalJournalParameter(value string, req livediagnostics.ProbeRequest) string {
	value = parameter(value, req)
	if strings.HasPrefix(value, "{{param.") && strings.HasSuffix(value, "}}") {
		return ""
	}
	return value
}

func journalWindow(c Collector, req livediagnostics.ProbeRequest, now time.Time) (time.Time, time.Time, error) {
	seconds, err := configuredSinceSeconds(c, req)
	if err != nil || seconds < 1 || seconds > 86400 {
		return time.Time{}, time.Time{}, errors.New("journal lookback must be between 1 and 86400 seconds")
	}
	until := now
	if value := optionalJournalParameter(c.UntilTime, req); value != "" {
		until, err = time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("journal end must be an RFC3339 timestamp")
		}
	}
	since := until.Add(-time.Duration(seconds) * time.Second)
	if value := optionalJournalParameter(c.SinceTime, req); value != "" {
		since, err = time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("journal start must be an RFC3339 timestamp")
		}
	}
	if until.After(now) || since.Before(now.Add(-24*time.Hour)) || !since.Before(until) {
		return time.Time{}, time.Time{}, errors.New("journal window must be ordered and entirely within the past 24 hours")
	}
	// Journal timestamps have microsecond precision. Round the lower bound up
	// so records before the requested/24-hour boundary cannot enter the report.
	if !since.Equal(since.Truncate(time.Microsecond)) {
		since = since.Truncate(time.Microsecond).Add(time.Microsecond)
	}
	return since.UTC(), until.UTC().Truncate(time.Microsecond), nil
}

func journalTimestamp(at time.Time) string {
	return "@" + strconv.FormatInt(at.Unix(), 10) + "." + fmt.Sprintf("%06d", at.Nanosecond()/1000)
}

func journalFilterArguments(patterns []string) ([]string, error) {
	if len(patterns) > 16 {
		return nil, errors.New("journal filter exceeds literal count limit")
	}
	escaped := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		if pattern == "" || len(pattern) > 256 || strings.ContainsAny(pattern, "\x00\r\n") {
			return nil, errors.New("journal filter requires nonempty bounded single-line literals")
		}
		escaped = append(escaped, regexp.QuoteMeta(pattern))
	}
	if len(escaped) == 0 {
		return nil, nil
	}
	return []string{"--grep=" + strings.Join(escaped, "|"), "--case-sensitive=yes"}, nil
}

func journalNoMatches(err error, raw []byte, stderr string, filtered bool) bool {
	var exitErr *exec.ExitError
	return filtered && len(raw) == 0 && strings.TrimSpace(stderr) == "" && errors.As(err, &exitErr) && exitErr.ExitCode() == 1
}

func journalEvidence(raw []byte, patterns []string) (map[string]any, error) {
	counts := map[string]int{}
	examples := []any{}
	byPattern := map[string][]any{}
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
		safe := safeText(message)
		if len(safe) > 2000 {
			safe = safe[:2000]
		}
		example := map[string]any{"at": ts, "boot_id": boot, "message": safe}
		for _, pattern := range patterns {
			if strings.Contains(message, pattern) {
				counts[pattern]++
				matched = true
				if len(byPattern[pattern]) < 8 {
					byPattern[pattern] = append(byPattern[pattern], example)
				}
			}
		}
		if matched && len(examples) < 40 {
			examples = append(examples, example)
		}
	}
	if scanner.Err() != nil {
		parseErr = errors.New("journal entry exceeds the parser byte budget")
	}
	return map[string]any{"entries": rows, "oldest_observed": oldest, "newest_observed": newest, "pattern_counts": counts, "examples": examples, "examples_by_pattern": byPattern}, parseErr
}
