package frontrepairstate

import (
	"fugue/internal/edgetopology"
	"testing"
)

func TestRetainedAuthorityScopeSyntax(t *testing.T) {
	if authorityIDPattern != edgetopology.AuthorityIDPattern {
		t.Fatal("temporary repair tool and retained Front disagree on the durable authority ID format")
	}
}
