package platformsafety

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTrackedPlatformSourcesContainNoBusinessCoupling(t *testing.T) {
	t.Parallel()
	root := filepath.Clean("../..")
	command := exec.Command("git", "ls-files", "-z")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	zeroApp := strings.Join([]string{"0", "-", "0"}, "")
	legacyResponsePath := "/v1/" + strings.Join([]string{"res", "ponses"}, "")
	legacySynthetic := strings.Join([]string{"responses", "synthetic"}, "_")
	terms := []string{
		zeroApp + ".pro",
		"api." + zeroApp + ".pro",
		strings.Join([]string{"uni", "api"}, "-"),
		legacyResponsePath,
		legacySynthetic,
		strings.Join([]string{"gpt", "5.6", "sol"}, "-"),
		strings.Join([]string{"gpt", "5.4", "mini"}, "-"),
		strings.Join([]string{"Reply", "with", "exactly", "ok."}, " "),
		strings.Join([]string{"0652", "cb2"}, ""),
	}
	for _, rawPath := range strings.Split(strings.TrimSuffix(string(output), "\x00"), "\x00") {
		if rawPath == "" {
			continue
		}
		body, err := os.ReadFile(filepath.Join(root, rawPath))
		if err != nil {
			t.Fatal(err)
		}
		text := strings.ToLower(string(body))
		for _, term := range terms {
			for _, variant := range businessCouplingVariants(term) {
				if strings.Contains(text, strings.ToLower(variant)) {
					t.Fatalf("tracked source %s contains forbidden business coupling variant for %q", rawPath, term)
				}
			}
		}
		if err := rejectBusinessAppToken(rawPath, text, zeroApp); err != nil {
			t.Fatal(err)
		}
	}
}

func businessCouplingVariants(value string) []string {
	encoded := base64.StdEncoding.EncodeToString([]byte(value))
	percent := ""
	unicodeEscaped := ""
	for _, b := range []byte(value) {
		percent += fmt.Sprintf("%%%02x", b)
		unicodeEscaped += fmt.Sprintf("\\u%04x", b)
	}
	return []string{value, strings.ToUpper(value), url.PathEscape(value), percent, unicodeEscaped, encoded}
}

func rejectBusinessAppToken(path, text, token string) error {
	for offset := 0; ; {
		index := strings.Index(text[offset:], token)
		if index < 0 {
			return nil
		}
		index += offset
		beforeDigit := index > 0 && text[index-1] >= '0' && text[index-1] <= '9'
		after := index + len(token)
		afterDigit := after < len(text) && text[after] >= '0' && text[after] <= '9'
		if !beforeDigit && !afterDigit && !allowedRegistryRangeToken(path, text, index, token) {
			return fmt.Errorf("tracked source %s contains business app token", path)
		}
		offset = after
	}
}

func allowedRegistryRangeToken(path, text string, index int, token string) bool {
	lineStart := strings.LastIndex(text[:index], "\n") + 1
	lineEnd := strings.Index(text[index:], "\n")
	if lineEnd < 0 {
		lineEnd = len(text)
	} else {
		lineEnd += index
	}
	line := strings.TrimSpace(text[lineStart:lineEnd])
	allowed := map[string]map[string]struct{}{
		"cmd/fugue-image-cache/main.go": {
			`return "` + token + `"`: {},
		},
		"scripts/verify_registry_image.py": {
			`{"range": "bytes=` + token + `"},`: {},
			`match = re.fullmatch(r"bytes\s+` + token + `/(\d+)", content_range, flags=re.ignorecase)`: {},
		},
		"scripts/test_verify_registry_image.py": {
			`if self.headers.get("range") == "bytes=` + token + `" and body:`: {},
			`content_range = f"bytes ` + token + `/{len(body)}"`:              {},
		},
	}
	lines, ok := allowed[path]
	if !ok {
		return false
	}
	_, ok = lines[line]
	return ok
}
