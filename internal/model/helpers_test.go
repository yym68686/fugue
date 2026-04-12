package model

import "testing"

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
