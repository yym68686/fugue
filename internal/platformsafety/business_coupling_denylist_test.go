package platformsafety

import (
	"encoding/base64"
	"encoding/json"
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
		text := strings.ToLower(string(businessCouplingSource(rawPath, body)))
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

// Exact constraint transitions must carry the actual hostname in configuration
// data. Exempt only that typed value, never an entire file or configuration tree:
// executable fields, comments, unknown schema and adjacent strings stay scanned.
func businessCouplingSource(path string, body []byte) []byte {
	const physicalDirectory = "deploy/environments/production/routing-physical-dns/"
	if strings.HasPrefix(path, physicalDirectory) && !strings.Contains(strings.TrimPrefix(path, physicalDirectory), "/") && strings.HasSuffix(path, ".json") {
		return orderedProjectionHostnameSource(body, "fugue.physical-dns-reconfiguration/v1")
	}
	const qualityDirectory = "deploy/environments/production/routing-dynamic-quality/"
	if strings.HasPrefix(path, qualityDirectory) && !strings.Contains(strings.TrimPrefix(path, qualityDirectory), "/") && strings.HasSuffix(path, ".json") {
		return orderedProjectionHostnameSource(body, "fugue.dynamic-quality-reconfiguration/v1")
	}
	const retirementDirectory = "deploy/environments/production/routing-dns-retirement/"
	if strings.HasPrefix(path, retirementDirectory) && !strings.Contains(strings.TrimPrefix(path, retirementDirectory), "/") && strings.HasSuffix(path, ".json") {
		return retirementHostnameSource(body)
	}
	const directory = "deploy/environments/production/cell-producer-reconfiguration/"
	if !strings.HasPrefix(path, directory) || strings.Contains(strings.TrimPrefix(path, directory), "/") || !strings.HasSuffix(path, ".json") {
		return body
	}
	var declaration map[string]json.RawMessage
	if json.Unmarshal(body, &declaration) != nil || string(declaration["schema"]) != `"fugue.cell-producer-reconfiguration/v1"` {
		return body
	}
	var policy, transition map[string]json.RawMessage
	var constraints []map[string]json.RawMessage
	if json.Unmarshal(declaration["policy"], &policy) != nil || json.Unmarshal(policy["route_placement_transition"], &transition) != nil || json.Unmarshal(transition["constraints"], &constraints) != nil {
		return body
	}
	for _, constraint := range constraints {
		var source map[string]json.RawMessage
		var hostname string
		if json.Unmarshal(constraint["source"], &source) != nil || json.Unmarshal(source["hostname"], &hostname) != nil {
			return body
		}
		source["hostname"] = json.RawMessage(`"configuration-hostname"`)
		constraint["source"], _ = json.Marshal(source)
	}
	transition["constraints"], _ = json.Marshal(constraints)
	policy["route_placement_transition"], _ = json.Marshal(transition)
	declaration["policy"], _ = json.Marshal(policy)
	out, err := json.Marshal(declaration)
	if err != nil {
		return body
	}
	return out
}

func retirementHostnameSource(body []byte) []byte {
	return orderedProjectionHostnameSource(body, "fugue.dns-selector-retirement/v1")
}

func orderedProjectionHostnameSource(body []byte, schema string) []byte {
	var declaration map[string]json.RawMessage
	var declaredSchema string
	if json.Unmarshal(body, &declaration) != nil || json.Unmarshal(declaration["schema"], &declaredSchema) != nil || declaredSchema != schema {
		return body
	}
	var policy, query, projection map[string]json.RawMessage
	var overrides []map[string]json.RawMessage
	if json.Unmarshal(declaration["projection_policy"], &policy) != nil || json.Unmarshal(policy["dns_query_policy"], &query) != nil || json.Unmarshal(query["ordered_projection"], &projection) != nil || json.Unmarshal(projection["overrides"], &overrides) != nil {
		return body
	}
	for _, override := range overrides {
		var hostname string
		if json.Unmarshal(override["hostname"], &hostname) != nil || hostname == "" {
			return body
		}
		override["hostname"] = json.RawMessage(`"configuration-hostname"`)
	}
	projection["overrides"], _ = json.Marshal(overrides)
	query["ordered_projection"], _ = json.Marshal(projection)
	policy["dns_query_policy"], _ = json.Marshal(query)
	declaration["projection_policy"], _ = json.Marshal(policy)
	encoded, err := json.Marshal(declaration)
	if err != nil {
		return body
	}
	return encoded
}

func TestDynamicQualityExceptionOnlyCoversPreservedHostnameConstraints(t *testing.T) {
	const path = "deploy/environments/production/routing-dynamic-quality/universal.json"
	const body = `{"schema":"fugue.dynamic-quality-reconfiguration/v1","projection_policy":{"dns_query_policy":{"ordered_projection":{"overrides":[{"hostname":"tenant.example.test","command":"branch on tenant.example.test"}]},"dynamic_quality":{"mode":"all_dynamic","command":"branch on tenant.example.test"}}}}`
	masked := string(businessCouplingSource(path, []byte(body)))
	if !strings.Contains(masked, `"hostname":"configuration-hostname"`) || strings.Count(masked, "branch on tenant.example.test") != 2 {
		t.Fatal("typed constraint exception hid executable content")
	}
	for _, other := range []string{"internal/api/routing.go", path + ".go", strings.Replace(path, "/universal", "/nested/universal", 1)} {
		if string(businessCouplingSource(other, []byte(body))) != body {
			t.Fatal("typed constraint exception escaped declaration scope")
		}
	}
	unknown := strings.Replace(body, "fugue.dynamic-quality-reconfiguration/v1", "unknown/v1", 1)
	if string(businessCouplingSource(path, []byte(unknown))) != unknown {
		t.Fatal("unknown schema exempted")
	}
}

func TestPhysicalQualityExceptionOnlyCoversPreservedHostnameConstraints(t *testing.T) {
	const path = "deploy/environments/production/routing-physical-dns/quality-canary.json"
	const body = `{"schema":"fugue.physical-dns-reconfiguration/v1","projection_policy":{"dns_query_policy":{"ordered_projection":{"overrides":[{"hostname":"tenant.example.test","command":"branch on tenant.example.test"}]},"command":"branch on tenant.example.test"}}}`
	masked := string(businessCouplingSource(path, []byte(body)))
	if !strings.Contains(masked, `"hostname":"configuration-hostname"`) || strings.Count(masked, "branch on tenant.example.test") != 2 {
		t.Fatal("physical configuration exception hid executable content")
	}
	for _, other := range []string{"internal/api/routing.go", path + ".go", strings.Replace(path, "/quality-canary", "/nested/quality-canary", 1)} {
		if string(businessCouplingSource(other, []byte(body))) != body {
			t.Fatal("physical configuration exception escaped declaration scope")
		}
	}
	unknown := strings.Replace(body, "fugue.physical-dns-reconfiguration/v1", "unknown/v1", 1)
	if string(businessCouplingSource(path, []byte(unknown))) != unknown {
		t.Fatal("unknown physical configuration schema exempted")
	}
}

func TestRetirementConfigExceptionOnlyCoversTypedHostnameValues(t *testing.T) {
	const path = "deploy/environments/production/routing-dns-retirement/physical-order.json"
	const body = `{"schema":"fugue.dns-selector-retirement/v1","projection_policy":{"dns_query_policy":{"ordered_projection":{"overrides":[{"hostname":"tenant.example.test","command":"branch on tenant.example.test"}]}}}}`
	masked := string(businessCouplingSource(path, []byte(body)))
	if !strings.Contains(masked, `"hostname":"configuration-hostname"`) || !strings.Contains(masked, `"command":"branch on tenant.example.test"`) {
		t.Fatal("retirement exception hid executable data")
	}
	for _, other := range []string{"internal/api/routing.go", path + ".go", strings.Replace(path, "/physical-order", "/nested/physical-order", 1)} {
		if string(businessCouplingSource(other, []byte(body))) != body {
			t.Fatal("exception escaped typed declaration")
		}
	}
	unknown := strings.Replace(body, "fugue.dns-selector-retirement/v1", "unknown/v1", 1)
	if string(businessCouplingSource(path, []byte(unknown))) != unknown {
		t.Fatal("unknown schema exempted")
	}
}

func TestBusinessCouplingConfigExceptionOnlyCoversHostnameData(t *testing.T) {
	const path = "deploy/environments/production/cell-producer-reconfiguration/cell-example.json"
	const hostname = "tenant.example.test"
	const body = `{"schema":"fugue.cell-producer-reconfiguration/v1","policy":{"route_placement_transition":{"constraints":[{"source":{"hostname":"` + hostname + `","command":"branch on tenant.example.test"}}]}}}`
	masked := string(businessCouplingSource(path, []byte(body)))
	if !strings.Contains(masked, `"hostname":"configuration-hostname"`) || !strings.Contains(masked, `"command":"branch on tenant.example.test"`) {
		t.Fatal("configuration exception hid executable field")
	}
	for _, other := range []string{"internal/api/routing.go", path + ".go", strings.Replace(path, "/cell-example", "/nested/cell-example", 1)} {
		if string(businessCouplingSource(other, []byte(body))) != body {
			t.Fatal("exception applied outside typed declaration")
		}
	}
	unknown := strings.Replace(body, "fugue.cell-producer-reconfiguration/v1", "arbitrary/v1", 1)
	if string(businessCouplingSource(path, []byte(unknown))) != unknown {
		t.Fatal("unknown schema exempted")
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
