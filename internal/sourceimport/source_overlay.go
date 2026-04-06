package sourceimport

import (
	"fmt"
	"path/filepath"
	"strings"
)

type sourceOverlayFile struct {
	RelativePath  string
	Content       string
	OnlyIfMissing bool
}

func buildSourceOverlayInitContainer(workingDir string, files []sourceOverlayFile) (map[string]any, error) {
	script, err := buildSourceOverlayScript(workingDir, files)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(script) == "" {
		return nil, nil
	}
	return map[string]any{
		"name":    "source-overlay",
		"image":   defaultGitCloneImage,
		"command": []string{"sh", "-lc", script},
		"volumeMounts": []map[string]any{
			{"name": "workspace", "mountPath": "/workspace"},
		},
	}, nil
}

func buildSourceOverlayScript(workingDir string, files []sourceOverlayFile) (string, error) {
	workingDir = strings.TrimSpace(workingDir)
	if workingDir == "" {
		return "", fmt.Errorf("overlay working directory is required")
	}
	if len(files) == 0 {
		return "", nil
	}

	var script strings.Builder
	script.WriteString("set -eu\n")
	script.WriteString("cd ")
	script.WriteString(shellQuoteForOverlay(workingDir))
	script.WriteString("\n")

	for index, file := range files {
		relativePath, err := sanitizeOverlayRelativePath(file.RelativePath)
		if err != nil {
			return "", err
		}
		if relativePath == "" {
			continue
		}
		dir := filepath.ToSlash(filepath.Dir(relativePath))
		if dir != "." {
			script.WriteString("mkdir -p ")
			script.WriteString(shellQuoteForOverlay(dir))
			script.WriteString("\n")
		}
		if file.OnlyIfMissing {
			script.WriteString("if [ ! -f ")
			script.WriteString(shellQuoteForOverlay(relativePath))
			script.WriteString(" ]; then\n")
		}
		label := fmt.Sprintf("EOF_FUGUE_OVERLAY_%d", index+1)
		script.WriteString("cat > ")
		script.WriteString(shellQuoteForOverlay(relativePath))
		script.WriteString(" <<'")
		script.WriteString(label)
		script.WriteString("'\n")
		script.WriteString(file.Content)
		if !strings.HasSuffix(file.Content, "\n") {
			script.WriteString("\n")
		}
		script.WriteString(label)
		script.WriteString("\n")
		if file.OnlyIfMissing {
			script.WriteString("fi\n")
		}
	}

	return script.String(), nil
}

func sanitizeOverlayRelativePath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if filepath.IsAbs(raw) {
		return "", fmt.Errorf("overlay path %q must be relative", raw)
	}
	cleaned := filepath.ToSlash(filepath.Clean(raw))
	if cleaned == "." || cleaned == "" {
		return "", nil
	}
	if strings.HasPrefix(cleaned, "../") || cleaned == ".." {
		return "", fmt.Errorf("overlay path %q escapes the source root", raw)
	}
	return cleaned, nil
}

func shellQuoteForOverlay(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}
