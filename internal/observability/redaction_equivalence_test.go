package observability

import (
	"strings"
	"testing"
)

func redactTextReference(value string) (string, bool) {
	result := value
	changed := false
	for _, rule := range secretTextPatterns {
		next := rule.pattern.ReplaceAllString(result, "${1}[REDACTED]")
		if next != result {
			changed = true
			result = next
		}
	}
	return result, changed
}

func FuzzRedactTextPreservesAssignmentRules(f *testing.F) {
	for _, s := range []string{
		"", "plain log line", `{"route_id":"route-a","status_code":200,"duration_ms":17}`,
		"authorization=Bearer token=nested cookie=session password=value database_url=postgres://u:p@host/db",
		"SeT-CoOkIe: value, COOKIE=value, ACCESS_TOKEN = value refresh_token=value X-API-KEY=value",
		"paſſword=value toKen=value", "中文 token=value", string([]byte{0xff, ':', 't', 'o', 'k', 'e', 'n', '=', 'a'}),
		"authorization=token=x, token=cookie=y; database_url=password=z", "token=token=secret",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		want, changed := redactTextReference(s)
		got, gotChanged := RedactText(s)
		if got != want || gotChanged != changed {
			t.Fatalf("redaction behavior changed for %q: got %q/%v want %q/%v", s, got, gotChanged, want, changed)
		}
	})
}

func BenchmarkRedactTelemetryFields(b *testing.B) {
	cases := map[string]string{
		"identifier":      "edgegrouproute_" + strings.Repeat("a", 64),
		"request_summary": strings.Repeat(`{"hostname":"app.example.test","route_kind":"platform","status_code":200,"origin_connected":true,"trace_id":"trace-a"}`, 20),
		"secret":          "level=error authorization=secret token=secret database_url=postgres://u:p@host/db",
		"unicode":         "中文日志 paſſword=secret toKen=secret",
	}
	for name, input := range cases {
		for version, fn := range map[string]func(string) (string, bool){"original": redactTextReference, "optimized": RedactText} {
			b.Run(name+"/"+version, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(input)))
				for b.Loop() {
					fn(input)
				}
			})
		}
	}
}
