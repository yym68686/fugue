package store

import (
	"context"
	"database/sql"
	"fugue/internal/model"
	"time"
)

// Internal reconciler calls have no request principal. Request paths always
// supply one; the object change and audit record share a commit.
func mutationEvent(actors []model.Principal, action, kind, id, owner string) *model.AuditEvent {
	if len(actors) == 0 {
		return nil
	}
	a := actors[0]
	return &model.AuditEvent{ID: model.NewID("audit"), CreatedAt: time.Now().UTC(), ActorType: a.ActorType, ActorID: a.ActorID, TenantID: owner, Action: action, TargetType: kind, TargetID: id, Metadata: map[string]string{"actor_tenant": a.TenantID, "resource_tenant": owner, "result": "success"}}
}
func recordMutation(state *model.State, event *model.AuditEvent) {
	if event != nil {
		state.AuditEvents = append(state.AuditEvents, *event)
	}
}
func (s *Store) commitMutation(ctx context.Context, tx *sql.Tx, event *model.AuditEvent) error {
	if event != nil {
		if err := s.pgAppendAuditEventTx(ctx, tx, *event); err != nil {
			return err
		}
	}
	return tx.Commit()
}
