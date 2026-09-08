package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestDocumentedCommandExamplesParseWithoutExecution(t *testing.T) {
	root := newCLI(&bytes.Buffer{}, &bytes.Buffer{}).newRootCommand()
	seen := map[string]bool{}
	for _, entry := range commandCatalog(root, false) {
		for _, line := range strings.Split(entry.Example, "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "fugue ") || strings.ContainsAny(line, "|;&\n") || strings.HasSuffix(line, "\\") || strings.Contains(line, "$ ") {
				continue
			}
			if seen[line] {
				continue
			}
			seen[line] = true
			t.Run(entry.Path+"/"+line, func(t *testing.T) {
				words := shellWordPattern.FindAllString(strings.TrimPrefix(line, "fugue "), -1)
				for i := range words {
					words[i] = strings.Trim(words[i], "\"'")
				}
				commandRoot := newCLI(&bytes.Buffer{}, &bytes.Buffer{}).newRootCommand()
				cmd, remaining, err := commandRoot.Find(words)
				if err != nil {
					t.Fatal(err)
				}
				if err = cmd.ParseFlags(remaining); err != nil {
					t.Fatal(err)
				}
				if err = cmd.ValidateArgs(cmd.Flags().Args()); err != nil {
					t.Fatal(err)
				}
				if err = cmd.ValidateRequiredFlags(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	if len(seen) < 250 {
		t.Fatalf("only %d parsed examples", len(seen))
	}
}
