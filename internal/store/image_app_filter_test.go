package store

import (
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestImageAppSetSQLRetainsTenantPredicate(t *testing.T) {
	clauses, args := imageFilterClauses(normalizeImageFilter(model.ImageFilter{TenantID: "tenant", AppIDs: []string{" b ", "a", "b"}}))
	if !reflect.DeepEqual(clauses, []string{"tenant_id = $1", "app_id IN ($2, $3)"}) || !reflect.DeepEqual(args, []any{"tenant", "a", "b"}) {
		t.Fatalf("query predicates changed: %v %v", clauses, args)
	}
}
