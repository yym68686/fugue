// Package backupmaterializeridentity defines the fixed-purpose workload
// identity boundary for fetching one backup observer input bundle. It accepts
// only a short-lived Kubernetes TokenReview result for one exact, Pod-bound,
// cell-derived ServiceAccount; it contains no Kubernetes client, signer,
// datastore, or long-lived bootstrap credential.
package backupmaterializeridentity

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	Audience                = "fugue-backup-materializer.fugue.dev"
	PermissionReadBundle    = "backup.observer-input-bundle.read.v1"
	ServiceAccountNamespace = "fugue-system"
	serviceAccountPrefix    = "fugue-backup-materializer-"
	maxTokenBytes           = 16 << 10

	credentialDocumentIDExtraKey = "authentication.kubernetes.io/credential-id"
	podNameExtraKey              = "authentication.kubernetes.io/pod-name"
	podUIDExtraKey               = "authentication.kubernetes.io/pod-uid"
)

var (
	ErrInvalidIdentity     = errors.New("invalid backup materializer identity")
	ErrReviewerUnavailable = errors.New("backup materializer identity reviewer unavailable")

	canonicalCellKey              = regexp.MustCompile(`^backup/(control-plane-db|app-database|persistent-storage|data-workspace|registry|platform-component)/[0-9a-f]{16}$`)
	canonicalJWTPart              = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	canonicalUID                  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	canonicalCredentialDocumentID = regexp.MustCompile(`^JTI=[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	canonicalPodName              = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$`)
)

var canonicalCellKinds = []string{
	"control-plane-db",
	"app-database",
	"persistent-storage",
	"data-workspace",
	"registry",
	"platform-component",
}

// TokenReviewer is the only external capability needed by this package. A
// future API adapter will submit the bearer token to Kubernetes TokenReview
// with exactly the supplied audience and translate only the returned status.
type TokenReviewer interface {
	ReviewToken(context.Context, string, []string) (ReviewResult, error)
}

// ReviewResult is the minimal authentication.k8s.io/v1 TokenReview status
// surface used by the identity policy. It intentionally contains no token.
type ReviewResult struct {
	Authenticated bool
	Audiences     []string
	Username      string
	UID           string
	Groups        []string
	Extra         map[string][]string
	Error         string
}

// Claims are safe to place in request context and operational diagnostics.
// They contain identity bindings only, never the reviewed bearer token.
type Claims struct {
	CredentialID            string    `json:"credentialId"`
	CellKey                 string    `json:"cellKey"`
	Permission              string    `json:"permission"`
	Audience                string    `json:"audience"`
	ServiceAccountNamespace string    `json:"serviceAccountNamespace"`
	ServiceAccountName      string    `json:"serviceAccountName"`
	ServiceAccountUID       string    `json:"serviceAccountUid"`
	CredentialDocumentID    string    `json:"credentialDocumentId"`
	PodName                 string    `json:"podName"`
	PodUID                  string    `json:"podUid"`
	ReviewedAt              time.Time `json:"reviewedAt"`
}

// Authenticate locally rejects every non-Kubernetes-token shape before
// invoking the reviewer, then binds the verified result to the exact expected
// backup cell. Reviewer transport failures remain distinguishable from invalid
// credentials so middleware can fail closed with a bounded retry response.
func Authenticate(
	ctx context.Context,
	reviewer TokenReviewer,
	token string,
	expectedCellKey string,
	now time.Time,
) (Claims, error) {
	if !canonicalCellKey.MatchString(expectedCellKey) {
		return Claims{}, ErrInvalidIdentity
	}
	result, err := reviewIdentity(ctx, reviewer, token, now)
	if err != nil {
		return Claims{}, err
	}
	return BindReviewResult(result, expectedCellKey, now)
}

// AuthenticateReviewedCell derives the canonical backup cell from the exact
// reviewed ServiceAccount username, then applies the same Pod-bound policy as
// Authenticate. This lets a GET-only HTTP boundary authenticate before a
// later handler loads cell-owned state; that handler must still compare its
// server-derived resource cell with Claims.CellKey.
func AuthenticateReviewedCell(
	ctx context.Context,
	reviewer TokenReviewer,
	token string,
	now time.Time,
) (Claims, error) {
	result, err := reviewIdentity(ctx, reviewer, token, now)
	if err != nil {
		return Claims{}, err
	}
	usernamePrefix := "system:serviceaccount:" + ServiceAccountNamespace + ":"
	if !strings.HasPrefix(result.Username, usernamePrefix) {
		return Claims{}, ErrInvalidIdentity
	}
	cellKey := CellKeyForServiceAccountName(strings.TrimPrefix(result.Username, usernamePrefix))
	if cellKey == "" {
		return Claims{}, ErrInvalidIdentity
	}
	return BindReviewResult(result, cellKey, now)
}

func reviewIdentity(
	ctx context.Context,
	reviewer TokenReviewer,
	token string,
	now time.Time,
) (ReviewResult, error) {
	if ctx == nil || nilTokenReviewer(reviewer) || now.IsZero() || !canonicalTokenShape(token) {
		return ReviewResult{}, ErrInvalidIdentity
	}
	if err := ctx.Err(); err != nil {
		return ReviewResult{}, ErrReviewerUnavailable
	}
	result, err := reviewer.ReviewToken(ctx, token, []string{Audience})
	if err != nil {
		return ReviewResult{}, ErrReviewerUnavailable
	}
	return result, nil
}

func nilTokenReviewer(reviewer TokenReviewer) bool {
	if reviewer == nil {
		return true
	}
	value := reflect.ValueOf(reviewer)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// BindReviewResult applies the policy to an already authenticated TokenReview
// status. It requires bound-Pod extras, excluding legacy Secret-backed service
// account tokens even when Kubernetes would otherwise authenticate them.
func BindReviewResult(result ReviewResult, expectedCellKey string, now time.Time) (Claims, error) {
	serviceAccountName := ServiceAccountNameForCell(expectedCellKey)
	expectedUsername := "system:serviceaccount:" + ServiceAccountNamespace + ":" + serviceAccountName
	if now.IsZero() || serviceAccountName == "" || !result.Authenticated || result.Error != "" ||
		len(result.Audiences) != 1 || result.Audiences[0] != Audience ||
		result.Username != expectedUsername || !canonicalUID.MatchString(result.UID) ||
		!exactServiceAccountGroups(result.Groups, ServiceAccountNamespace) {
		return Claims{}, ErrInvalidIdentity
	}
	podName, ok := exactSingleExtra(result.Extra, podNameExtraKey)
	if !ok || !canonicalPodName.MatchString(podName) || !strings.HasPrefix(podName, serviceAccountName+"-") {
		return Claims{}, ErrInvalidIdentity
	}
	podUID, ok := exactSingleExtra(result.Extra, podUIDExtraKey)
	if !ok || !canonicalUID.MatchString(podUID) {
		return Claims{}, ErrInvalidIdentity
	}
	credentialDocumentID, ok := exactSingleExtra(result.Extra, credentialDocumentIDExtraKey)
	if !ok || !canonicalCredentialDocumentID.MatchString(credentialDocumentID) {
		return Claims{}, ErrInvalidIdentity
	}
	claims := Claims{
		CredentialID:            CredentialIDForCell(expectedCellKey),
		CellKey:                 expectedCellKey,
		Permission:              PermissionReadBundle,
		Audience:                Audience,
		ServiceAccountNamespace: ServiceAccountNamespace,
		ServiceAccountName:      serviceAccountName,
		ServiceAccountUID:       result.UID,
		CredentialDocumentID:    credentialDocumentID,
		PodName:                 podName,
		PodUID:                  podUID,
		ReviewedAt:              now.UTC().Truncate(time.Second),
	}
	if ValidateClaims(claims) != nil {
		return Claims{}, ErrInvalidIdentity
	}
	return claims, nil
}

func ValidateClaims(claims Claims) error {
	if !canonicalCellKey.MatchString(claims.CellKey) ||
		claims.CredentialID != CredentialIDForCell(claims.CellKey) ||
		claims.Permission != PermissionReadBundle || claims.Audience != Audience ||
		claims.ServiceAccountNamespace != ServiceAccountNamespace ||
		claims.ServiceAccountName != ServiceAccountNameForCell(claims.CellKey) ||
		!canonicalUID.MatchString(claims.ServiceAccountUID) ||
		!canonicalCredentialDocumentID.MatchString(claims.CredentialDocumentID) ||
		!canonicalPodName.MatchString(claims.PodName) ||
		!strings.HasPrefix(claims.PodName, claims.ServiceAccountName+"-") ||
		!canonicalUID.MatchString(claims.PodUID) || !canonicalTime(claims.ReviewedAt) {
		return ErrInvalidIdentity
	}
	return nil
}

func ServiceAccountNameForCell(cellKey string) string {
	if !canonicalCellKey.MatchString(cellKey) {
		return ""
	}
	return serviceAccountPrefix + strings.ReplaceAll(strings.TrimPrefix(cellKey, "backup/"), "/", "-")
}

func CellKeyForServiceAccountName(name string) string {
	if strings.TrimSpace(name) != name || !strings.HasPrefix(name, serviceAccountPrefix) {
		return ""
	}
	remainder := strings.TrimPrefix(name, serviceAccountPrefix)
	for _, kind := range canonicalCellKinds {
		prefix := kind + "-"
		if !strings.HasPrefix(remainder, prefix) {
			continue
		}
		cellKey := "backup/" + kind + "/" + strings.TrimPrefix(remainder, prefix)
		if ServiceAccountNameForCell(cellKey) == name {
			return cellKey
		}
	}
	return ""
}

func CredentialIDForCell(cellKey string) string {
	if !canonicalCellKey.MatchString(cellKey) {
		return ""
	}
	return "backup-materializer:" + strings.ReplaceAll(strings.TrimPrefix(cellKey, "backup/"), "/", ":")
}

func canonicalTokenShape(token string) bool {
	if token == "" || len(token) > maxTokenBytes || strings.TrimSpace(token) != token {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if !canonicalJWTPart.MatchString(part) {
			return false
		}
	}
	return true
}

func exactServiceAccountGroups(groups []string, namespace string) bool {
	want := []string{
		"system:authenticated",
		"system:serviceaccounts",
		"system:serviceaccounts:" + namespace,
	}
	if len(groups) != len(want) {
		return false
	}
	got := append([]string(nil), groups...)
	sort.Strings(got)
	sort.Strings(want)
	for index := range want {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func exactSingleExtra(extra map[string][]string, key string) (string, bool) {
	values, ok := extra[key]
	if !ok || len(values) != 1 || strings.TrimSpace(values[0]) != values[0] || values[0] == "" {
		return "", false
	}
	return values[0], true
}

func canonicalTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Nanosecond() == 0
}
