package model

import (
	"strings"
	"testing"
)

func TestSlugifyFallsBackToItem(t *testing.T) {
	t.Parallel()

	if got := Slugify(""); got != "item" {
		t.Fatalf("expected blank slug to fall back to item, got %q", got)
	}
}

func TestSlugifyOptionalReturnsEmptyForBlankOrNonAlphaNumericInput(t *testing.T) {
	t.Parallel()

	cases := []string{"", "   ", "---", "!!!"}
	for _, input := range cases {
		if got := SlugifyOptional(input); got != "" {
			t.Fatalf("expected optional slug %q to stay empty, got %q", input, got)
		}
	}
}

func TestDNS1035LabelPrefixesNumericNames(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"001-fugue-oiuhu89": "app-001-fugue-oiuhu89",
		"abc-123":           "abc-123",
		"---":               "app",
	}
	for input, want := range cases {
		if got := DNS1035Label(input, "app"); got != want {
			t.Fatalf("expected DNS-1035 label %q -> %q, got %q", input, want, got)
		}
	}
	if got := DNS1035Label("001-fugue-oiuhu89", "123"); got != "app-001-fugue-oiuhu89" {
		t.Fatalf("expected invalid fallback prefix to fall back to app, got %q", got)
	}
}

func TestDNS1035LabelTruncatesWithoutTrailingDash(t *testing.T) {
	t.Parallel()

	got := DNS1035Label("1-"+strings.Repeat("a", 80), "service")
	if len(got) > 63 {
		t.Fatalf("expected DNS-1035 label to be at most 63 chars, got %d", len(got))
	}
	if got[len(got)-1] == '-' {
		t.Fatalf("expected DNS-1035 label not to end with dash, got %q", got)
	}
	if got[:8] != "service-" {
		t.Fatalf("expected numeric label to use service prefix, got %q", got)
	}
}
