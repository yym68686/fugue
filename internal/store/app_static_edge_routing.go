package store

import (
	"context"
	"fugue/internal/model"
	"time"
)

// SetAppStaticEdgeRouting atomically updates only DNS routing. Never replace the
// app spec or a concurrently renamed route with an earlier API read.
func (s *Store) SetAppStaticEdgeRouting(id, revision string, routing *model.AppStaticEdgeRouting) (model.App, error) {
	if revision == "" || model.ValidateAppStaticEdgeRouting(routing) != nil {
		return model.App{}, ErrInvalidInput
	}
	update := func(app *model.App) error {
		if isDeletedApp(*app) {
			return ErrNotFound
		}
		if app.Route == nil || model.AppRouteRevision(app.Route) != revision {
			return ErrConflict
		}
		route := *app.Route
		route.DNSRouting = model.CloneAppStaticEdgeRouting(routing)
		app.Route = &route
		app.UpdatedAt = time.Now().UTC()
		return nil
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return model.App{}, err
		}
		defer tx.Rollback()
		app, err := s.pgGetAppTx(ctx, tx, id, true)
		if err != nil {
			return app, mapDBErr(err)
		}
		if err = update(&app); err != nil {
			return model.App{}, err
		}
		if err = s.pgUpdateAppTx(ctx, tx, app); err != nil {
			return model.App{}, mapDBErr(err)
		}
		return app, tx.Commit()
	}
	var out model.App
	err := s.withLockedState(true, func(state *model.State) error {
		i := findApp(state, id)
		if i < 0 {
			return ErrNotFound
		}
		out = state.Apps[i]
		if err := update(&out); err != nil {
			return err
		}
		state.Apps[i] = out
		return nil
	})
	return out, err
}
