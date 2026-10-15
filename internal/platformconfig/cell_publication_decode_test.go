package platformconfig

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestDNSPublicationDecodeReportsPolicyFailureWithoutCellReferences(t *testing.T) {
	artifact := model.PlatformArtifact{Content: map[string]any{
		"policy": map[string]any{
			"dns_query_policy": map[string]any{"ranking_mode": "active"},
		},
	}}
	_, err := DecodeCellRoutePublications(artifact)
	if err == nil || !strings.Contains(err.Error(), "DNS artifact policy cannot be decoded") ||
		!strings.Contains(err.Error(), "DNS query strategy requires complete explicit fields") {
		t.Fatalf("policy failure was hidden or blamed on absent Cell references: %v", err)
	}
}

func TestDNSPublicationDecodePreservesFailureStageAndCause(t *testing.T) {
	for _, test := range []struct {
		name, want string
		content    map[string]any
	}{
		{"policy", "DNS artifact policy cannot be decoded", map[string]any{"policy": false}},
		{"references", "DNS Cell references cannot be decoded", map[string]any{"policy": map[string]any{}, "cell_route_publications": false}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeCellRoutePublications(model.PlatformArtifact{Content: test.content})
			var cause *json.UnmarshalTypeError
			if err == nil || !strings.Contains(err.Error(), test.want) || !errors.As(err, &cause) {
				t.Fatalf("lost decode stage or wrapped type error: %v", err)
			}
		})
	}
}

func TestDNSPublicationDecodeStillEnforcesAuthorityBoundary(t *testing.T) {
	for _, test := range []struct {
		name, want string
		content    map[string]any
	}{
		{"ordinary DNS", "", map[string]any{"policy": map[string]any{}}},
		{"foreign references", "Cell references require DNS publication authority", map[string]any{
			"policy": map[string]any{}, "cell_route_publications": []any{map[string]any{}},
		}},
		{"missing references", "DNS Cell references must be nonempty and bounded", map[string]any{
			"policy": map[string]any{"publication_role": PublicationRoleCellDNS},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			publications, err := DecodeCellRoutePublications(model.PlatformArtifact{Content: test.content})
			if test.want == "" {
				if err != nil || len(publications) != 0 {
					t.Fatalf("ordinary DNS acquired Cell references: %v %v", publications, err)
				}
			} else if err == nil || err.Error() != test.want {
				t.Fatalf("authority check changed: %v", err)
			}
		})
	}
}
