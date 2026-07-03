package observability

import (
	"strings"
	"testing"
)

func TestRedactDiagnosticTextCoversSecretsHeadersQueriesDSNAndPII(t *testing.T) {
	input := strings.Join([]string{
		"DATABASE_URL=postgres://demo:s3cr3t@example.internal/db",
		"dsn postgres://demo:open-sesame@example.internal/db",
		"Authorization: Bearer abc.def.ghi",
		"Cookie: session=super-secret",
		"callback=https://example.test/cb?token=tok_123&safe=1",
		"user alice@example.com paid with 4242424242424242",
	}, "\n")
	result := RedactDiagnosticText(input)
	if !result.Changed {
		t.Fatalf("expected redaction to change input")
	}
	for _, leaked := range []string{"s3cr3t", "open-sesame", "abc.def.ghi", "session=super-secret", "tok_123", "alice@example.com", "4242424242424242"} {
		if strings.Contains(result.Text, leaked) {
			t.Fatalf("expected %q to be redacted from %q", leaked, result.Text)
		}
	}
	for _, preserved := range []string{"DATABASE_URL=", "postgres://demo:", "Authorization: Bearer ", "Cookie: ", "?token=", "&safe=1"} {
		if !strings.Contains(result.Text, preserved) {
			t.Fatalf("expected diagnostic context %q to be preserved in %q", preserved, result.Text)
		}
	}
	if len(result.Findings) < 6 {
		t.Fatalf("expected multiple redaction findings, got %+v", result.Findings)
	}
	for _, finding := range result.Findings {
		if finding.Kind == "" || finding.OriginalLength <= 0 || finding.SHA256Prefix == "" {
			t.Fatalf("expected finding metadata, got %+v", finding)
		}
	}
}
