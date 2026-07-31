package backupmaterializeridentity

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testCellKey = "backup/app-database/0123456789abcdef"
	testSAToken = "eyJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJmdWd1ZS1iYWNrdXAtbWF0ZXJpYWxpemVyIn0.signature"
)

type recordingReviewer struct {
	result    ReviewResult
	err       error
	called    int
	token     string
	audiences []string
}

func (reviewer *recordingReviewer) ReviewToken(_ context.Context, token string, audiences []string) (ReviewResult, error) {
	reviewer.called++
	reviewer.token = token
	reviewer.audiences = append([]string(nil), audiences...)
	return reviewer.result, reviewer.err
}

func TestMaterializerIdentityBindsExactCellAudienceServiceAccountAndPod(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	reviewer := &recordingReviewer{result: validReviewResult(testCellKey)}
	claims, err := Authenticate(context.Background(), reviewer, testSAToken, testCellKey, now)
	if err != nil {
		t.Fatalf("authenticate materializer: %v", err)
	}
	wantSA := "fugue-backup-materializer-app-database-0123456789abcdef"
	if reviewer.called != 1 || reviewer.token != testSAToken || !reflect.DeepEqual(reviewer.audiences, []string{Audience}) {
		t.Fatalf("review request widened: called=%d token=%q audiences=%v", reviewer.called, reviewer.token, reviewer.audiences)
	}
	if claims.CredentialID != "backup-materializer:app-database:0123456789abcdef" ||
		claims.CellKey != testCellKey || claims.Permission != PermissionReadBundle || claims.Audience != Audience ||
		claims.ServiceAccountNamespace != ServiceAccountNamespace || claims.ServiceAccountName != wantSA ||
		claims.ServiceAccountUID != "11111111-1111-4111-8111-111111111111" ||
		claims.CredentialDocumentID != "JTI=33333333-3333-4333-8333-333333333333" ||
		claims.PodName != wantSA+"-6d4f7c8b9f-abcde" ||
		claims.PodUID != "22222222-2222-4222-8222-222222222222" || claims.ReviewedAt != now {
		t.Fatalf("materializer claims drifted: %+v", claims)
	}
	if err := ValidateClaims(claims); err != nil {
		t.Fatalf("validate materializer claims: %v", err)
	}
	document, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal materializer claims: %v", err)
	}
	if strings.Contains(string(document), testSAToken) || strings.Contains(string(document), "eyJhbGci") {
		t.Fatalf("request-context claims retained the reviewed bearer token: %s", document)
	}
}

func TestMaterializerIdentityDerivesCanonicalReviewedCell(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 10, 5, 0, 0, time.UTC)
	reviewer := &recordingReviewer{result: validReviewResult(testCellKey)}
	claims, err := AuthenticateReviewedCell(context.Background(), reviewer, testSAToken, now)
	if err != nil {
		t.Fatalf("authenticate reviewed materializer cell: %v", err)
	}
	if claims.CellKey != testCellKey || claims.ServiceAccountName != ServiceAccountNameForCell(testCellKey) ||
		claims.CredentialID != CredentialIDForCell(testCellKey) || claims.ReviewedAt != now || reviewer.called != 1 {
		t.Fatalf("review-derived materializer claims drifted: claims=%+v reviewer=%+v", claims, reviewer)
	}
	for name, username := range map[string]string{
		"other namespace": "system:serviceaccount:default:" + ServiceAccountNameForCell(testCellKey),
		"unknown cell":    "system:serviceaccount:" + ServiceAccountNamespace + ":fugue-backup-materializer-all-0123456789abcdef",
		"extra suffix":    serviceAccountUsername(testCellKey) + "-other",
		"empty":           "",
	} {
		t.Run(name, func(t *testing.T) {
			result := validReviewResult(testCellKey)
			result.Username = username
			reviewer := &recordingReviewer{result: result}
			if _, err := AuthenticateReviewedCell(context.Background(), reviewer, testSAToken, now); !errors.Is(err, ErrInvalidIdentity) {
				t.Fatalf("derived-cell error = %v, want invalid identity", err)
			}
		})
	}
}

func TestMaterializerIdentityRejectsUnboundLegacyAndCrossCellReviews(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	tests := map[string]func(*ReviewResult){
		"not authenticated":    func(value *ReviewResult) { value.Authenticated = false },
		"review error":         func(value *ReviewResult) { value.Error = "token expired" },
		"missing audience":     func(value *ReviewResult) { value.Audiences = nil },
		"extra audience":       func(value *ReviewResult) { value.Audiences = append(value.Audiences, "other") },
		"wrong audience":       func(value *ReviewResult) { value.Audiences[0] = "other" },
		"cross-cell account":   func(value *ReviewResult) { value.Username = serviceAccountUsername("backup/registry/0123456789abcdef") },
		"legacy account token": func(value *ReviewResult) { delete(value.Extra, podNameExtraKey); delete(value.Extra, podUIDExtraKey) },
		"missing pod uid":      func(value *ReviewResult) { delete(value.Extra, podUIDExtraKey) },
		"duplicate pod uid": func(value *ReviewResult) {
			value.Extra[podUIDExtraKey] = append(value.Extra[podUIDExtraKey], value.Extra[podUIDExtraKey][0])
		},
		"foreign pod":           func(value *ReviewResult) { value.Extra[podNameExtraKey] = []string{"other-workload-abcde"} },
		"invalid pod uid":       func(value *ReviewResult) { value.Extra[podUIDExtraKey] = []string{"not-a-uid"} },
		"invalid account uid":   func(value *ReviewResult) { value.UID = "not-a-uid" },
		"missing credential id": func(value *ReviewResult) { delete(value.Extra, credentialDocumentIDExtraKey) },
		"invalid credential id": func(value *ReviewResult) { value.Extra[credentialDocumentIDExtraKey] = []string{"JTI=other"} },
		"duplicate credential id": func(value *ReviewResult) {
			value.Extra[credentialDocumentIDExtraKey] = append(value.Extra[credentialDocumentIDExtraKey], value.Extra[credentialDocumentIDExtraKey][0])
		},
		"missing group":   func(value *ReviewResult) { value.Groups = value.Groups[:2] },
		"extra group":     func(value *ReviewResult) { value.Groups = append(value.Groups, "system:masters") },
		"duplicate group": func(value *ReviewResult) { value.Groups[2] = value.Groups[1] },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			result := validReviewResult(testCellKey)
			mutate(&result)
			if _, err := BindReviewResult(result, testCellKey, now); !errors.Is(err, ErrInvalidIdentity) {
				t.Fatalf("review error = %v, want invalid identity", err)
			}
		})
	}
}

func TestMaterializerIdentityRejectsMalformedTokensBeforeReviewAndSurfacesReviewerOutage(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	for name, token := range map[string]string{
		"empty":      "",
		"whitespace": " " + testSAToken,
		"opaque":     "fugue_wk_v1",
		"two parts":  "header.payload",
		"four parts": "a.b.c.d",
		"bad base64": "header.pay+load.signature",
		"oversized":  strings.Repeat("a", maxTokenBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			reviewer := &recordingReviewer{result: validReviewResult(testCellKey)}
			if _, err := Authenticate(context.Background(), reviewer, token, testCellKey, now); !errors.Is(err, ErrInvalidIdentity) {
				t.Fatalf("token error = %v, want invalid identity", err)
			}
			if reviewer.called != 0 {
				t.Fatal("malformed token reached external reviewer")
			}
		})
	}
	for name, invoke := range map[string]func() error{
		"nil context": func() error {
			_, err := Authenticate(nil, &recordingReviewer{}, testSAToken, testCellKey, now)
			return err
		},
		"nil reviewer": func() error {
			_, err := Authenticate(context.Background(), nil, testSAToken, testCellKey, now)
			return err
		},
		"typed nil reviewer": func() error {
			var reviewer *recordingReviewer
			_, err := Authenticate(context.Background(), reviewer, testSAToken, testCellKey, now)
			return err
		},
		"zero time": func() error {
			_, err := Authenticate(context.Background(), &recordingReviewer{}, testSAToken, testCellKey, time.Time{})
			return err
		},
		"invalid cell": func() error {
			_, err := Authenticate(context.Background(), &recordingReviewer{}, testSAToken, "backup/all/0123456789abcdef", now)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := invoke(); !errors.Is(err, ErrInvalidIdentity) {
				t.Fatalf("precondition error = %v, want invalid identity", err)
			}
		})
	}
	reviewer := &recordingReviewer{err: errors.New("tokenreview unavailable")}
	if _, err := Authenticate(context.Background(), reviewer, testSAToken, testCellKey, now); !errors.Is(err, ErrReviewerUnavailable) ||
		errors.Is(err, ErrInvalidIdentity) || strings.Contains(err.Error(), "tokenreview unavailable") {
		t.Fatalf("reviewer outage error = %v, want reviewer unavailable only", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	reviewer = &recordingReviewer{result: validReviewResult(testCellKey)}
	if _, err := Authenticate(canceled, reviewer, testSAToken, testCellKey, now); !errors.Is(err, ErrReviewerUnavailable) || reviewer.called != 0 {
		t.Fatalf("canceled review error=%v called=%d, want local reviewer-unavailable rejection", err, reviewer.called)
	}
}

func TestMaterializerIdentityCanonicalCellNamesRoundTripWithinDNSLimit(t *testing.T) {
	t.Parallel()
	for _, kind := range canonicalCellKinds {
		cellKey := "backup/" + kind + "/0123456789abcdef"
		name := ServiceAccountNameForCell(cellKey)
		if name == "" || len(name) > 63 || CellKeyForServiceAccountName(name) != cellKey || CredentialIDForCell(cellKey) == "" {
			t.Fatalf("canonical cell identity did not round trip: cell=%q name=%q credential=%q", cellKey, name, CredentialIDForCell(cellKey))
		}
	}
	for _, invalid := range []string{"", " backup/app-database/0123456789abcdef", "backup/all/0123456789abcdef", "backup/app-database/ABCDEF0123456789"} {
		if ServiceAccountNameForCell(invalid) != "" || CredentialIDForCell(invalid) != "" {
			t.Fatalf("invalid cell produced identity: %q", invalid)
		}
	}
	for _, invalid := range []string{"", "Fugue-backup-materializer-app-database-0123456789abcdef", "fugue-backup-materializer-all-0123456789abcdef", "fugue-backup-materializer-app-database-0123456789abcdef-extra"} {
		if CellKeyForServiceAccountName(invalid) != "" {
			t.Fatalf("invalid ServiceAccount name produced a cell: %q", invalid)
		}
	}
}

func TestMaterializerIdentityClaimsRejectRecomposedDrift(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	claims, err := BindReviewResult(validReviewResult(testCellKey), testCellKey, now)
	if err != nil {
		t.Fatalf("bind materializer claims: %v", err)
	}
	for name, mutate := range map[string]func(*Claims){
		"credential":          func(value *Claims) { value.CredentialID = "backup-materializer:other" },
		"cell":                func(value *Claims) { value.CellKey = "backup/registry/0123456789abcdef" },
		"permission":          func(value *Claims) { value.Permission = "backup.write" },
		"audience":            func(value *Claims) { value.Audience = "other" },
		"namespace":           func(value *Claims) { value.ServiceAccountNamespace = "default" },
		"account":             func(value *Claims) { value.ServiceAccountName = "other" },
		"account uid":         func(value *Claims) { value.ServiceAccountUID = "other" },
		"credential document": func(value *Claims) { value.CredentialDocumentID = "JTI=other" },
		"pod":                 func(value *Claims) { value.PodName = "other" },
		"pod uid":             func(value *Claims) { value.PodUID = "other" },
		"time":                func(value *Claims) { value.ReviewedAt = value.ReviewedAt.Add(time.Nanosecond) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := claims
			mutate(&candidate)
			if err := ValidateClaims(candidate); !errors.Is(err, ErrInvalidIdentity) {
				t.Fatalf("claims error = %v, want invalid identity", err)
			}
		})
	}
}

func TestMaterializerIdentityDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list materializer identity dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	if !reflect.DeepEqual(local, []string{}) {
		t.Fatalf("backup materializer identity dependency boundary widened: %v", local)
	}
}

func validReviewResult(cellKey string) ReviewResult {
	serviceAccountName := ServiceAccountNameForCell(cellKey)
	return ReviewResult{
		Authenticated: true,
		Audiences:     []string{Audience},
		Username:      serviceAccountUsername(cellKey),
		UID:           "11111111-1111-4111-8111-111111111111",
		Groups: []string{
			"system:serviceaccounts:" + ServiceAccountNamespace,
			"system:authenticated",
			"system:serviceaccounts",
		},
		Extra: map[string][]string{
			podNameExtraKey:              {serviceAccountName + "-6d4f7c8b9f-abcde"},
			podUIDExtraKey:               {"22222222-2222-4222-8222-222222222222"},
			credentialDocumentIDExtraKey: {"JTI=33333333-3333-4333-8333-333333333333"},
		},
	}
}

func serviceAccountUsername(cellKey string) string {
	return "system:serviceaccount:" + ServiceAccountNamespace + ":" + ServiceAccountNameForCell(cellKey)
}
