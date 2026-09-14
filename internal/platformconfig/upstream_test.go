package platformconfig

import (
	"reflect"
	"testing"
)

func TestValidateUpstreamIntentsRejectsAmbiguousUnsafeTargets(t *testing.T) {
	valid := UpstreamIntent{Role: "stable", Weight: 100, UpstreamURL: "https://origin.example:8443", ServicePort: 8443}
	if err := ValidateUpstreamIntents([]UpstreamIntent{valid}); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"", "origin.example", "file:///tmp/file", "http://user:password@origin", "https://origin:65536", "https://origin/#fragment", " https://origin"} {
		bad := valid
		bad.UpstreamURL = raw
		if err := ValidateUpstreamIntents([]UpstreamIntent{bad}); err == nil {
			t.Fatalf("invalid upstream URL accepted: %q", raw)
		}
	}
	for name, change := range map[string]func(*UpstreamIntent){
		"negative port":        func(v *UpstreamIntent) { v.ServicePort = -1 },
		"oversized port":       func(v *UpstreamIntent) { v.ServicePort = 65536 },
		"unknown kind":         func(v *UpstreamIntent) { v.UpstreamKind = "unknown" },
		"unknown scope":        func(v *UpstreamIntent) { v.UpstreamScope = "unknown" },
		"weight bounds":        func(v *UpstreamIntent) { v.Weight = 101 },
		"zero total":           func(v *UpstreamIntent) { v.Weight = 0 },
		"selection whitespace": func(v *UpstreamIntent) { v.ReleaseID = " release " },
	} {
		t.Run(name, func(t *testing.T) {
			bad := valid
			change(&bad)
			if err := ValidateUpstreamIntents([]UpstreamIntent{bad}); err == nil {
				t.Fatal("invalid target accepted")
			}
		})
	}
	duplicate := []UpstreamIntent{valid, valid}
	duplicate[0].Weight, duplicate[1].Weight = 50, 50
	duplicate[1].UpstreamURL = "https://another.example"
	if err := ValidateUpstreamIntents(duplicate); err == nil {
		t.Fatal("duplicate selector identity accepted")
	}
	if err := ValidateUpstreamIntents(make([]UpstreamIntent, 17)); err == nil {
		t.Fatal("unbounded target list accepted")
	}
}

func TestUpstreamNormalizationCopiesAndRetainsOrderedTargets(t *testing.T) {
	input := PlatformIntent{Generation: "ordered", Routes: []RouteIntent{{Hostname: "route.example", UpstreamURL: "https://fallback", Upstreams: []UpstreamIntent{
		{Role: "stable", Weight: 80, UpstreamURL: "https://stable"}, {Role: "canary", Weight: 20, UpstreamURL: "https://canary"},
	}}}}
	normalized := NormalizePlatformIntent(input)
	if !reflect.DeepEqual(input.Routes[0].Upstreams, normalized.Routes[0].Upstreams) {
		t.Fatal("traffic partition reordered")
	}
	normalized.Routes[0].Upstreams[0].Weight = 50
	if input.Routes[0].Upstreams[0].Weight != 80 {
		t.Fatal("normalization aliases mutable upstreams")
	}
	projected := ProjectUpstreamIntents(input.Routes[0].Upstreams)
	if projected[0].Role != "stable" || projected[1].Role != "canary" || projected[0].Status != "" || projected[1].StatusReason != "" {
		t.Fatal("upstream projection invented runtime facts")
	}
}
