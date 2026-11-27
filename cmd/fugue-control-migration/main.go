// fugue-control-migration inspects the legacy control plane without
// production credentials or a network client.
package main

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

const inventoryPath = "docs/architecture/production-control-migration.tsv"

type surface struct {
	kind, id string
}

func (s surface) String() string { return s.kind + "\t" + s.id }

func sortSurfaces(items []surface) {
	slices.SortFunc(items, func(a, b surface) int {
		return cmp.Or(cmp.Compare(a.kind, b.kind), cmp.Compare(a.id, b.id))
	})
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	if len(args) != 2 || (args[0] != "scan" && args[0] != "check") {
		return errors.New("usage: fugue-control-migration <scan|check> REPOSITORY")
	}
	root, err := filepath.Abs(args[1])
	if err != nil {
		return err
	}
	cmd := exec.Command("git", "-C", root, "ls-files", "--full-name", "-z", "--",
		".github/workflows", ".github/actions", "scripts", "deploy/environments/production", "deploy/releases")
	raw, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("list tracked legacy surfaces: %w", err)
	}
	files := strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")
	items, err := scan(files, func(name string) ([]byte, error) {
		return os.ReadFile(filepath.Join(root, name))
	})
	if err != nil {
		return err
	}
	if args[0] == "scan" {
		for _, s := range items {
			if _, err := fmt.Fprintln(out, s); err != nil {
				return err
			}
		}
		return nil
	}
	manifest, err := os.ReadFile(filepath.Join(root, inventoryPath))
	if err != nil {
		return err
	}
	migrate, retire, err := check(items, string(manifest))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "Legacy inventory matches: %d migrate, %d retire. This is not proof of cutover.\n", migrate, retire)
	return err
}

func scan(files []string, read func(string) ([]byte, error)) ([]surface, error) {
	var items []surface
	seen := make(map[surface]bool)
	add := func(kind, id string) error {
		item := surface{kind, id}
		if id == "" || strings.ContainsAny(id, "\t\r\n") || seen[item] {
			return fmt.Errorf("invalid or duplicate legacy surface: %q", item)
		}
		seen[item] = true
		items = append(items, item)
		return nil
	}
	for _, name := range files {
		ext := filepath.Ext(name)
		switch {
		case strings.HasPrefix(name, ".github/workflows/") && (ext == ".yml" || ext == ".yaml"):
			raw, err := read(name)
			if err != nil {
				return nil, err
			}
			var wf struct {
				Jobs map[string]yaml.Node `yaml:"jobs"`
				On   yaml.Node            `yaml:"on"`
			}
			if err := yaml.Unmarshal(raw, &wf); err != nil {
				return nil, fmt.Errorf("parse %s: %w", name, err)
			}
			if len(wf.Jobs) == 0 {
				return nil, fmt.Errorf("workflow has no jobs: %s", name)
			}
			for job := range wf.Jobs {
				if err := add("workflow-job", name+"#"+job); err != nil {
					return nil, err
				}
			}
			switch wf.On.Kind {
			case yaml.ScalarNode, yaml.SequenceNode:
				// GitHub requires mapping syntax for scheduled workflows.
			case yaml.MappingNode:
				var triggers struct {
					Schedule []struct {
						Cron string `yaml:"cron"`
					} `yaml:"schedule"`
				}
				if err := wf.On.Decode(&triggers); err != nil {
					return nil, fmt.Errorf("parse triggers in %s: %w", name, err)
				}
				for _, trigger := range triggers.Schedule {
					if len(strings.Fields(trigger.Cron)) != 5 {
						return nil, fmt.Errorf("missing or invalid cron in %s", name)
					}
					if err := add("workflow-schedule", name+"#"+trigger.Cron); err != nil {
						return nil, err
					}
				}
			default:
				return nil, fmt.Errorf("unknown trigger syntax in %s", name)
			}
		case strings.HasPrefix(name, ".github/actions/"):
			if err := add("local-action-file", name); err != nil {
				return nil, err
			}
		case strings.HasPrefix(name, "deploy/environments/production/"):
			if err := add("production-file", name); err != nil {
				return nil, err
			}
		case strings.HasPrefix(name, "deploy/releases/"):
			if err := add("release-file", name); err != nil {
				return nil, err
			}
			relative := strings.TrimPrefix(name, "deploy/releases/")
			if lane, _, found := strings.Cut(relative, "/"); found {
				item := surface{"release-lane", lane}
				if !seen[item] {
					if err := add(item.kind, item.id); err != nil {
						return nil, err
					}
				}
			}
		case strings.HasPrefix(name, "scripts/"):
			base := filepath.Base(name)
			parts := strings.Split(name, "/")
			if (ext == ".py" || ext == ".sh" || ext == ".ps1") &&
				!strings.HasPrefix(base, "test_") && !strings.HasSuffix(strings.TrimSuffix(base, ext), "_test") &&
				!slices.Contains(parts[:len(parts)-1], "tests") {
				if err := add("script", name); err != nil {
					return nil, err
				}
			}
		}
	}
	if len(items) == 0 {
		return nil, errors.New("no legacy surfaces found")
	}
	sortSurfaces(items)
	return items, nil
}

func check(items []surface, manifest string) (migrate, retire int, err error) {
	disposition := make(map[surface]string)
	for i, raw := range strings.Split(manifest, "\n") {
		line := strings.TrimSuffix(raw, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 4 || fields[0] == "" || fields[1] == "" || strings.TrimSpace(fields[3]) == "" ||
			(fields[2] != "migrate" && fields[2] != "retire") {
			return 0, 0, fmt.Errorf("inventory line %d: expected kind, identity, migrate/retire and destination or reason", i+1)
		}
		key := surface{fields[0], fields[1]}
		if _, exists := disposition[key]; exists {
			return 0, 0, fmt.Errorf("inventory line %d: duplicate %s", i+1, key)
		}
		disposition[key] = fields[2]
	}
	var missing, stale []surface
	for _, item := range items {
		switch disposition[item] {
		case "migrate":
			migrate++
		case "retire":
			retire++
		default:
			missing = append(missing, item)
		}
		delete(disposition, item)
	}
	for item := range disposition {
		stale = append(stale, item)
	}
	if len(missing)+len(stale) != 0 {
		sortSurfaces(missing)
		sortSurfaces(stale)
		return 0, 0, fmt.Errorf("legacy inventory drift: unclassified=%v stale=%v", missing, stale)
	}
	return migrate, retire, nil
}
