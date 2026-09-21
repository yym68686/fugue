package diagnosticprobe

import (
	"context"
	"debug/buildinfo"
	"debug/elf"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"fugue/internal/livediagnostics"
)

// Inspect the actual executable of a frozen target, rather than guessing why
// a previous profiler did not resolve symbols. No command line or environment
// values are exported. The collector is independently versioned with its pack.
func processIdentities(ctx context.Context, req livediagnostics.ProbeRequest, procRoot string) (any, error) {
	if req.ContainerID == "" && req.Target.ProcessName == "" {
		return nil, errors.New("executable identity requires an exact process or container target")
	}
	value, err := processSchedulingAt(ctx, req, procRoot)
	if err != nil {
		return nil, err
	}
	if partial, ok := value.(partialValue); ok {
		value = partial.Value // This collector reports executable evidence, not IO counters.
	}
	inventory := value.(map[string]any)["processes"].([]processFact)
	identities := []any{}
	gaps := []string{}
	for i, process := range inventory {
		if i >= 16 {
			gaps = append(gaps, "executable identity target limit reached")
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		root := filepath.Join(procRoot, strconv.Itoa(process.PID))
		identity, err := executableIdentity(filepath.Join(root, "exe"))
		if err != nil {
			gaps = append(gaps, "executable metadata unavailable for PID "+strconv.Itoa(process.PID)+": "+boundedError(err))
			identities = append(identities, map[string]any{"pid": process.PID, "start_ticks": process.StartTicks, "security": processSecurity(root), "executable_error": boundedError(err)})
			continue
		}
		identity["pid"], identity["start_ticks"], identity["cgroup"] = process.PID, process.StartTicks, process.Cgroup
		maps, err := readBounded(filepath.Join(root, "maps"), 128<<10)
		if err != nil {
			gaps = append(gaps, "process address maps unavailable: "+boundedError(err))
		} else {
			identity["maps"] = maps
		}
		stat, err := readBounded(filepath.Join(root, "stat"), 16<<10)
		current, parseErr := parseProcessStat(process.PID, stat)
		if err != nil || parseErr != nil || current.StartTicks != process.StartTicks {
			gaps = append(gaps, "process identity changed during executable observation")
			continue
		}
		identities = append(identities, identity)
	}
	result := map[string]any{"executables": identities, "target_processes": len(inventory), "observer_security": processSecurity("/proc/self")}
	if len(gaps) > 0 {
		return partialValue{Value: result, Gaps: gaps, Truncated: len(inventory) > 16}, nil
	}
	return result, nil
}

func processSecurity(root string) map[string]any {
	result := map[string]any{}
	if value, err := readBounded(filepath.Join(root, "status"), 32<<10); err == nil {
		for _, line := range strings.Split(value, "\n") {
			key, data, _ := strings.Cut(line, ":")
			switch key {
			case "Uid", "Gid", "CapEff", "CapPrm", "CapBnd", "NoNewPrivs", "Seccomp", "TracerPid":
				result[key] = strings.TrimSpace(data)
			}
		}
	}
	for _, name := range []string{"attr/current", "uid_map", "gid_map"} {
		if value, err := readBounded(filepath.Join(root, name), 4096); err == nil {
			result[name] = strings.TrimSpace(value)
		}
	}
	return result
}

func executableIdentity(filename string) (map[string]any, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	metadata, err := elf.NewFile(file)
	if err != nil {
		return nil, err
	}
	sections := []map[string]any{}
	for _, section := range metadata.Sections {
		if strings.Contains(section.Name, "gopclntab") || strings.Contains(section.Name, "gosymtab") || strings.HasPrefix(section.Name, ".note.") || section.Name == ".text" {
			row := map[string]any{"name": section.Name, "address": section.Addr, "offset": section.Offset, "size": section.Size}
			if strings.HasPrefix(section.Name, ".note.") && section.Size <= 4096 {
				if data, err := section.Data(); err == nil {
					row["bytes_hex"] = hex.EncodeToString(data)
				}
			}
			sections = append(sections, row)
		}
	}
	result := map[string]any{"file_size": info.Size(), "elf_type": metadata.Type.String(), "machine": metadata.Machine.String(), "sections": sections}
	if link, err := os.Readlink(filename); err == nil {
		result["executable_link"] = link
	}
	if build, err := buildinfo.Read(file); err == nil {
		result["go_version"] = build.GoVersion
		result["module_path"] = build.Path
	} else {
		result["go_build_info_error"] = boundedError(err)
	}
	return result, nil
}
