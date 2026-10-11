package diagnosticprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

// This is the only SQL the collector can execute. Neither catalog parameters
// nor target data can introduce SQL, credentials, literals or application rows.
const postgresObservationSQL = `SELECT json_build_object('observed_at',clock_timestamp(),'server_started_at',pg_postmaster_start_time(),'query_text_limit',pg_size_bytes(current_setting('track_activity_query_size'))::int,'backends',coalesce(json_agg(row_to_json(a)),'[]'::json)) FROM (SELECT pid,backend_start,query_start,state,client_addr,wait_event_type,wait_event,md5(query) AS query_fingerprint,octet_length(query) AS observed_query_bytes,CASE WHEN state='active' THEN split_part(ltrim(query), ' ', 1) ELSE NULL END AS command FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid() ORDER BY pid LIMIT 129) a;`

type pgBackendObservation struct {
	PID          int        `json:"pid"`
	BackendStart time.Time  `json:"backend_start"`
	QueryStart   *time.Time `json:"query_start"`
	State        string     `json:"state"`
	Client       *string    `json:"client_addr"`
	WaitType     *string    `json:"wait_event_type"`
	Wait         *string    `json:"wait_event"`
	Fingerprint  string     `json:"query_fingerprint"`
	QueryBytes   int        `json:"observed_query_bytes"`
	Command      *string    `json:"command"`
}
type pgActivityObservation struct {
	At            time.Time              `json:"observed_at"`
	ServerStarted time.Time              `json:"server_started_at"`
	QueryLimit    int                    `json:"query_text_limit"`
	Backends      []pgBackendObservation `json:"backends"`
}
type pgProcessMemory struct {
	HostPID     int    `json:"host_pid"`
	DatabasePID int    `json:"database_pid"`
	StartTicks  string `json:"start_ticks"`
	RSS         uint64 `json:"rss_bytes"`
	Anonymous   uint64 `json:"anonymous_bytes"`
}
type pgMemorySample struct {
	AccountingCgroup string                 `json:"accounting_cgroup"`
	MissingProcesses int                    `json:"missing_processes"`
	At               time.Time              `json:"observed_at"`
	Current          uint64                 `json:"memory_current"`
	Limit            uint64                 `json:"memory_limit"`
	Events           map[string]uint64      `json:"memory_events"`
	MemoryStat       map[string]uint64      `json:"memory_stat_bytes,omitempty"`
	MemoryStatError  string                 `json:"memory_stat_error,omitempty"`
	Processes        []pgProcessMemory      `json:"processes"`
	Activity         *pgActivityObservation `json:"activity,omitempty"`
	ActivityError    string                 `json:"activity_error,omitempty"`
}

func postgresMemoryWindow(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	target := podRuntimeTarget{Namespace: parameter(c.Namespace, req), Name: parameter(c.ObjectName, req), UID: parameter(c.PodUID, req)}
	container := parameter(c.Container, req)
	if req.Target.Type != livediagnostics.TargetNode || !podUIDPattern.MatchString(target.UID) || !podNamePattern.MatchString(target.Name) || !podNamePattern.MatchString(target.Namespace) || strings.Contains(target.Namespace, ".") || len(target.Namespace) > 63 || !podNamePattern.MatchString(container) || c.IntervalSeconds < req.DurationSeconds {
		return nil, errors.New("PostgreSQL window requires one capture for an exact Pod UID and container on a node")
	}
	if c.CaptureSeconds < 5 || c.CaptureSeconds > 300 || c.CaptureSeconds+5 > req.DurationSeconds {
		return nil, errors.New("PostgreSQL capture requires 5-300 seconds and five seconds finalization budget")
	}
	if c.RuntimeBinary != "/var/lib/rancher/k3s/data/current/bin/crictl" && c.RuntimeBinary != "/usr/local/bin/crictl" && c.RuntimeBinary != "/usr/bin/crictl" {
		return nil, errors.New("unsupported CRI reader binary")
	}
	if c.Path != "/run/k3s/containerd/containerd.sock" && c.Path != "/run/containerd/containerd.sock" {
		return nil, errors.New("unsupported CRI reader socket")
	}
	root := filepath.Join(hostProc, "1/root")
	binary, err := criReaderBinary(root, c.RuntimeBinary)
	if err != nil {
		return nil, err
	}
	socket := filepath.Join(root, c.Path)
	base := []string{"--config=/dev/null", "--runtime-endpoint=unix://" + socket, "--timeout=2s"}
	read := func(ctx context.Context, value any, args ...string) error {
		raw, _, truncated, err := diagnosticCommandEvidence(ctx, 256<<10, binary, append(append([]string{}, base...), args...)...)
		if err != nil || truncated {
			return errors.New("bounded CRI observation unavailable")
		}
		return json.Unmarshal(raw, value)
	}
	facts, err := capturePodRuntimeFacts(ctx, target, read, hostProc)
	if err != nil {
		return nil, err
	}
	var selected *criContainerFact
	for i := range facts.Containers {
		f := &facts.Containers[i]
		if f.Name == container && f.State == "CONTAINER_RUNNING" {
			if selected != nil {
				return nil, errors.New("multiple running container identities")
			}
			selected = f
		}
	}
	if selected == nil || !selected.ProcessPresent {
		return nil, errors.New("target database container is not running")
	}
	pid := int(selected.PID)
	stat, err := readBounded(filepath.Join(hostProc, strconv.Itoa(pid), "stat"), 16<<10)
	if err != nil {
		return nil, err
	}
	process, err := parseProcessStat(pid, stat)
	if err != nil {
		return nil, err
	}
	cg, err := postgresCgroup(pid, selected.ID)
	if err != nil {
		return nil, err
	}
	boot, err := readBounded(filepath.Join(hostProc, "sys/kernel/random/boot_id"), 80)
	if err != nil {
		return nil, err
	}
	samples := []pgMemorySample{}
	gaps := []string{}
	started := time.Now()
	nextSQL := time.Time{}
	used := 0
	dense := false
	denseUntil := time.Time{}
	for time.Since(started) < time.Duration(c.CaptureSeconds)*time.Second && ctx.Err() == nil && len(samples) < 300 {
		stat, readErr := readBounded(filepath.Join(hostProc, strconv.Itoa(pid), "stat"), 16<<10)
		nowProcess, parseErr := parseProcessStat(pid, stat)
		if readErr != nil || parseErr != nil || nowProcess.StartTicks != process.StartTicks {
			gaps = append(gaps, "container process ended or changed; pre-exit samples retained")
			break
		}
		sample, err := readPostgresMemory(cg, selected.ID)
		if err != nil {
			gaps = append(gaps, "memory source became unavailable; pre-exit samples retained")
			break
		}
		if sample.MissingProcesses > 0 && len(gaps) == 0 {
			gaps = append(gaps, "some process lifetimes changed during memory sampling")
		}
		if sample.MemoryStatError != "" && len(gaps) == 0 {
			gaps = append(gaps, sample.MemoryStatError)
		}
		high := sample.Limit > 0 && float64(sample.Current)/float64(sample.Limit) >= 0.8
		if high && !dense {
			dense = true
			denseUntil = time.Now().Add(60 * time.Second)
		}
		interval := 5 * time.Second
		if dense && time.Now().Before(denseUntil) {
			interval = time.Second
		}
		if time.Now().After(nextSQL) {
			call, stop := context.WithTimeout(ctx, 2*time.Second)
			argv := append(append([]string{}, base...), "exec", "--sync", selected.ID, "env", "PGOPTIONS=-c statement_timeout=500 -c lock_timeout=100 -c default_transaction_read_only=on -c application_name=fugue-live-diagnostics", "psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-c", postgresObservationSQL)
			raw, _, truncated, readErr := diagnosticCommandEvidence(call, 128<<10, binary, argv...)
			stop()
			activity, observationErr := decodePostgresObservation(raw, truncated, readErr)
			if observationErr != nil {
				sample.ActivityError = observationErr.Error()
				if len(gaps) == 0 {
					gaps = append(gaps, sample.ActivityError)
				}
			} else {
				for i := range activity.Backends {
					b := &activity.Backends[i]
					if b.Command != nil {
						b.Command = databaseQueryCommand(*b.Command)
					}
				}
				sample.Activity = &activity
			}
			nextSQL = time.Now().Add(interval)
			if observationErr != nil {
				// Preserve memory samples without repeatedly opening failing SQL
				// observations during a high-water window.
				nextSQL = time.Now().Add(15 * time.Second)
			}
		}
		raw, _ := json.Marshal(sample)
		if used+len(raw) > 2<<20 {
			gaps = append(gaps, "memory window output budget reached")
			break
		}
		used += len(raw)
		samples = append(samples, sample)
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	if ctx.Err() != nil {
		gaps = append(gaps, "observation deadline reached")
	}
	if len(samples) >= 300 {
		gaps = append(gaps, "sample count budget reached")
	}
	result := map[string]any{"schema": "fugue.diagnostic.postgres_memory.v1", "target": target, "container": container, "container_id": selected.ID, "boot_id": strings.TrimSpace(boot), "container_process": process, "cgroup": cg, "started_at": started.UTC(), "finished_at": time.Now().UTC(), "samples": samples, "attribution": "coincident memory and SQL fingerprints; idle retained memory is not attributed to the last SQL", "sampling": "5s baseline; 1s for at most 60s after 80% of the observed memory limit; one SQL observation per sample", "query_fingerprint_basis": "md5 of pg_stat_activity query, potentially truncated by query_text_limit; no SQL text or parameters exported"}
	if len(gaps) > 0 {
		return partialValue{Value: result, Gaps: gaps}, nil
	}
	return result, nil
}

func decodePostgresObservation(raw []byte, truncated bool, commandErr error) (pgActivityObservation, error) {
	var activity pgActivityObservation
	if commandErr != nil {
		return activity, errors.New("database observation command failed; SQL text and stderr withheld")
	}
	if truncated {
		return activity, errors.New("database observation output exceeded byte budget")
	}
	if json.Unmarshal(raw, &activity) != nil {
		return activity, errors.New("database observation JSON is invalid")
	}
	if activity.At.IsZero() || activity.ServerStarted.IsZero() || activity.QueryLimit < 1 {
		return activity, errors.New("database observation lifecycle or query-size metadata missing")
	}
	if len(activity.Backends) > 128 {
		return activity, errors.New("database observation backend count exceeded budget")
	}
	return activity, nil
}

func postgresMemoryStat(raw string) (map[string]uint64, error) {
	values := map[string]uint64{}
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "anon", "file", "shmem", "file_mapped", "file_dirty", "file_writeback", "kernel", "slab", "sock", "inactive_anon", "active_anon", "inactive_file", "active_file":
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return nil, errors.New("memory.stat byte counter is invalid")
			}
			values[fields[0]] = n
		}
	}
	if _, ok := values["anon"]; !ok {
		return nil, errors.New("memory.stat anonymous bytes unavailable")
	}
	if _, ok := values["file"]; !ok {
		return nil, errors.New("memory.stat file bytes unavailable")
	}
	return values, nil
}

func postgresCgroup(pid int, id string) (string, error) {
	return postgresCgroupAt("/sys/fs/cgroup", pid, id)
}
func postgresCgroupAt(root string, pid int, id string) (string, error) {
	if !criIDPattern.MatchString(id) || pid <= 1 {
		return "", errors.New("invalid frozen cgroup identity")
	}
	found := ""
	visited := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if path == root {
				return err
			}
			return nil
		}
		visited++
		if visited > 20000 {
			return errors.New("cgroup inventory budget exceeded")
		}
		if !entry.IsDir() || !strings.Contains(entry.Name(), id) {
			return nil
		}
		raw, err := readBounded(filepath.Join(path, "cgroup.procs"), 8192)
		if err != nil {
			return nil
		}
		for _, value := range strings.Fields(raw) {
			if value == strconv.Itoa(pid) {
				if _, err := os.Stat(filepath.Join(path, "memory.current")); err == nil {
					found = path
					return filepath.SkipAll
				}
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if found == "" {
		return "", errors.New("frozen container cgroup and PID membership unavailable")
	}
	return found, nil
}

func readPostgresMemory(cg, id string) (pgMemorySample, error) {
	sample := pgMemorySample{At: time.Now().UTC(), Events: map[string]uint64{}, Processes: []pgProcessMemory{}}
	// Pod-level limits may be stricter than the child container. Record the
	// closest constrained accounting domain instead of adding process RSS.
	accounting := cg
	for path := cg; path != "/sys/fs/cgroup" && strings.HasPrefix(path, "/sys/fs/cgroup/"); path = filepath.Dir(path) {
		raw, err := readBounded(filepath.Join(path, "memory.max"), 80)
		if err != nil {
			continue
		}
		limit, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err == nil && (sample.Limit == 0 || limit < sample.Limit) {
			sample.Limit = limit
			accounting = path
		}
	}
	sample.AccountingCgroup = accounting
	raw, err := readBounded(filepath.Join(accounting, "memory.current"), 80)
	if err != nil {
		return sample, err
	}
	sample.Current, err = strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return sample, err
	}
	stat, statErr := readBounded(filepath.Join(accounting, "memory.stat"), 16<<10)
	if statErr == nil {
		sample.MemoryStat, statErr = postgresMemoryStat(stat)
	}
	if statErr != nil {
		sample.MemoryStatError = "memory.stat breakdown unavailable; total usage cannot identify anonymous memory"
	}
	raw, err = readBounded(filepath.Join(accounting, "memory.events"), 1024)
	if err != nil {
		return sample, err
	}
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				sample.Events[fields[0]] = n
			}
		}
	}
	raw, err = readBounded(filepath.Join(cg, "cgroup.procs"), 8192)
	if err != nil {
		return sample, err
	}
	pids := strings.Fields(raw)
	if len(pids) > 256 {
		return sample, errors.New("process set exceeds observation budget")
	}
	for _, v := range pids {
		pid, err := strconv.Atoi(v)
		if err != nil {
			continue
		}
		beforeStat, beforeErr := readBounded(filepath.Join(hostProc, v, "stat"), 16384)
		before, parseErr := parseProcessStat(pid, beforeStat)
		group, groupErr := readBounded(filepath.Join(hostProc, v, "cgroup"), 8192)
		if beforeErr != nil || parseErr != nil || groupErr != nil || !strings.Contains(group, id) {
			sample.MissingProcesses++
			continue
		}
		status, err := readBounded(filepath.Join(hostProc, v, "status"), 8192)
		if err != nil {
			continue
		}
		p := pgProcessMemory{HostPID: pid}
		name := ""
		for _, line := range strings.Split(status, "\n") {
			f := strings.Fields(line)
			if len(f) < 2 {
				continue
			}
			switch f[0] {
			case "Name:":
				name = f[1]
			case "NSpid:":
				p.DatabasePID, _ = strconv.Atoi(f[len(f)-1])
			case "VmRSS:":
				p.RSS, _ = strconv.ParseUint(f[1], 10, 64)
				p.RSS *= 1024
			case "RssAnon:":
				p.Anonymous, _ = strconv.ParseUint(f[1], 10, 64)
				p.Anonymous *= 1024
			}
		}
		if name != "postgres" {
			continue
		}
		s, err := readBounded(filepath.Join(hostProc, v, "stat"), 16384)
		if err != nil {
			continue
		}
		f, err := parseProcessStat(pid, s)
		if err != nil {
			continue
		}
		if f.StartTicks != before.StartTicks {
			sample.MissingProcesses++
			continue
		}
		p.StartTicks = f.StartTicks
		sample.Processes = append(sample.Processes, p)
	}
	sort.Slice(sample.Processes, func(i, j int) bool { return sample.Processes[i].HostPID < sample.Processes[j].HostPID })
	if len(sample.Processes) == 0 {
		return sample, fmt.Errorf("no PostgreSQL processes in frozen container %s", id)
	}
	return sample, nil
}
