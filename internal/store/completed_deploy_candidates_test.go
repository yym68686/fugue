package store

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestCompletedDeployCandidatesPageThroughEqualCompletionTimes(t *testing.T) {
	t.Parallel()
	s := newStoreTestDatabase(t, "")
	const tenant, foreign, app = "candidate-tenant", "other-tenant", "candidate-app"
	seedStoreTestIdentity(t, s, tenant, "candidate-project", app)
	seedStoreTestIdentity(t, s, foreign, "", "")
	now := time.Now().UTC().Truncate(time.Microsecond)
	insert := func(id, owner, kind, status string, at time.Time) {
		t.Helper()
		_, err := s.db.ExecContext(t.Context(), `INSERT INTO fugue_operations
 (id, tenant_id, app_id, type, status, execution_mode, requested_by_type, requested_by_id,
  created_at, updated_at, completed_at, desired_spec_json, desired_source_json)
 VALUES ($1,$2,$3,$4,$5,'managed','system','fixture',$6,$6,$7,
   '{"image":"registry.example.test/app:candidate","replicas":1}',
   '{"desired_source":{"type":"github-public","repo_url":"https://github.com/example/source"},"config_base_spec":{"env":{"SHOULD_NOT_RETURN":"large"}}}')`,
			id, owner, app, kind, status, now, at)
		if err != nil {
			t.Fatal(err)
		}
	}
	count := CompletedDeployCandidatePageSize*2 + 3
	for i := 0; i < count; i++ {
		insert(fmt.Sprintf("candidate-%03d", i), tenant, model.OperationTypeDeploy, model.OperationStatusCompleted, now)
	}
	insert("old", tenant, model.OperationTypeDeploy, model.OperationStatusCompleted, now.Add(-2*time.Minute))
	insert("failed", tenant, model.OperationTypeDeploy, model.OperationStatusFailed, now)
	insert("import", tenant, model.OperationTypeImport, model.OperationStatusCompleted, now)
	insert("foreign", foreign, model.OperationTypeDeploy, model.OperationStatusCompleted, now)

	seen := map[string]bool{}
	var cursor *CompletedDeployCursor
	for {
		page, err := s.ListCompletedDeployCandidates(t.Context(), tenant, app, now.Add(-time.Minute), cursor)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) > CompletedDeployCandidatePageSize {
			t.Fatal("unbounded page")
		}
		for _, op := range page {
			if !strings.HasPrefix(op.ID, "candidate-") || seen[op.ID] || op.ConfigBaseSpec != nil || op.DesiredSource == nil {
				t.Fatalf("unexpected candidate: id=%s duplicate=%t source=%t config baseline=%t", op.ID, seen[op.ID], op.DesiredSource != nil, op.ConfigBaseSpec != nil)
			}
			seen[op.ID] = true
		}
		if len(page) < CompletedDeployCandidatePageSize {
			break
		}
		last := page[len(page)-1]
		cursor = &CompletedDeployCursor{CompletedAt: *last.CompletedAt, ID: last.ID}
	}
	if len(seen) != count {
		t.Fatalf("read %d of %d candidates", len(seen), count)
	}
}
