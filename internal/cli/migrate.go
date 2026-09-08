package cli

import (
	"bufio"
	"github.com/spf13/cobra"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

type commandMigrationFinding struct {
	File           string `json:"file"`
	Line           int    `json:"line"`
	Command        string `json:"command"`
	Replacement    string `json:"replacement,omitempty"`
	ReviewRequired bool   `json:"review_required"`
}
type commandMigrationScan struct {
	SchemaVersion int                       `json:"schema_version"`
	Findings      []commandMigrationFinding `json:"findings"`
	Skipped       []string                  `json:"skipped"`
}

var fugueDynamicExecutablePattern = regexp.MustCompile(`(?:^|[;&|])\s*(?:(?:sudo|exec)\s+)?["']?\$(?:FUGUE(?:_BIN|_CLI|_CMD)?\b|\{FUGUE(?:_BIN|_CLI|_CMD)?\})["']?(?:\s|$)`)
var fugueCommandPattern = regexp.MustCompile(`\bfugue\s+`)
var shellWordPattern = regexp.MustCompile(`"(?:\\.|[^"\\])*"|'[^']*'|[^\s]+`)

func scanCommandMigrations(paths []string) (commandMigrationScan, error) {
	result := commandMigrationScan{SchemaVersion: 1, Findings: []commandMigrationFinding{}, Skipped: []string{}}
	migrations := commandMigrations()
	knownRoot := newCLI(io.Discard, io.Discard).newRootCommand()
	visited := map[string]bool{}
	for _, input := range paths {
		absolute, err := filepath.Abs(input)
		if err != nil {
			return result, err
		}
		err = filepath.WalkDir(absolute, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			name := entry.Name()
			if entry.IsDir() {
				switch name {
				case ".git", "node_modules", "vendor", ".gocache", "bin", ".next", "cli-refactor-audit-2026-09-08":
					return filepath.SkipDir
				}
				return nil
			}
			if entry.Type()&os.ModeSymlink != 0 || name == ".env" || strings.HasPrefix(name, ".env.") || name == "auth.json" {
				return nil
			}
			if visited[path] {
				return nil
			}
			visited[path] = true
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || info.Size() > 2<<20 {
				result.Skipped = append(result.Skipped, path+": non-regular or larger than 2 MiB")
				return nil
			}
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			scanner.Buffer(make([]byte, 4096), 2<<20)
			lineNumber := 0
			for scanner.Scan() {
				lineNumber++
				line := scanner.Text()
				if !utf8.ValidString(line) {
					result.Skipped = append(result.Skipped, path+": binary content")
					break
				}
				if fugueDynamicExecutablePattern.MatchString(line) {
					result.Findings = append(result.Findings, commandMigrationFinding{File: path, Line: lineNumber, Command: "fugue <dynamic shell construction>", ReviewRequired: true})
					continue
				}
				for _, loc := range fugueCommandPattern.FindAllStringIndex(line, -1) {
					rawWords := shellWordPattern.FindAllString(line[loc[1]:], -1)
					words := []string{}
					dynamic := false
					for i := 0; i < len(rawWords); i++ {
						word := strings.Trim(rawWords[i], "`\"'")
						if strings.ContainsAny(word, "|;&") {
							break
						}
						if strings.HasPrefix(word, "--") || word == "-o" {
							key := strings.SplitN(word, "=", 2)[0]
							if key == "--force-publish" {
								result.Findings = append(result.Findings, commandMigrationFinding{path, lineNumber, "fugue admin artifact ... --force-publish", "--soft-override", false})
							}
							if !strings.Contains(word, "=") {
								switch key {
								case "--json", "--show-ids", "--save-token", "--confirm-raw-output", "--help":
								default:
									if i+1 < len(rawWords) {
										i++
									}
								}
							}
							continue
						}
						if strings.ContainsAny(word, "${}") {
							dynamic = true
							break
						}
						words = append(words, word)
					}
					joined := strings.Join(words, " ")
					matched := false
					for _, m := range migrations {
						if joined == m.Path || strings.HasPrefix(joined, m.Path+" ") {
							result.Findings = append(result.Findings, commandMigrationFinding{path, lineNumber, "fugue " + m.Path, m.Replacement, false})
							matched = true
							break
						}
					}
					if dynamic && !matched {
						resolved, _, resolveErr := knownRoot.Find(words)
						if len(words) > 0 && resolveErr == nil && resolved.Runnable() && !resolved.HasSubCommands() {
							continue
						}
						result.Findings = append(result.Findings, commandMigrationFinding{File: path, Line: lineNumber, Command: "fugue <dynamic shell construction>", ReviewRequired: true})
					}
				}
			}
			return scanner.Err()
		})
		if err != nil {
			return result, err
		}
	}
	sort.Slice(result.Findings, func(i, j int) bool {
		a, b := result.Findings[i], result.Findings[j]
		if a.File == b.File {
			return a.Line < b.Line
		}
		return a.File < b.File
	})
	return result, nil
}
func (c *CLI) newMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "migrate", Short: "Inspect local scripts for removed CLI commands"}
	scan := &cobra.Command{Use: "scan [path...]", Short: "Report command prefixes and locations without uploading or editing files", Long: "Scan local text files for CLI migration candidates. Output contains fixed command prefixes and locations, never argument values or complete source lines. Dynamic shell construction is marked for review. Symbolic links, credential files, dependency trees, binary files and files larger than 2 MiB are skipped.", RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			args = []string{"."}
		}
		result, err := scanCommandMigrations(args)
		if err != nil {
			return err
		}
		return c.renderResourceResult(result)
	}}
	cmd.AddCommand(scan)
	return cmd
}
