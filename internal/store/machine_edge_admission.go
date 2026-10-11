package store

import (
	"context"
	"encoding/json"
	"time"

	"fugue/internal/model"
	"fugue/internal/model/edgecohort"
)

func applyEdgeAdmission(machine *model.Machine, id string) error {
	var intent model.EdgeNodeEnrollment
	if !machine.Policy.AllowEdge || json.Unmarshal([]byte(machine.Labels[model.MachineEdgeEnrollmentLabel]), &intent) != nil || intent.ID != id || intent.Validate(machine.ClusterNodeName, intent.EdgeGroupID, time.Now().UTC()) != nil {
		return ErrConflict
	}
	machine.Labels = cloneMap(machine.Labels)
	machine.Labels[edgecohort.PhaseLabel] = edgecohort.Admitted
	machine.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Store) AdmitMachineEdgeEnrollment(name, id string) (model.Machine, error) {
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return model.Machine{}, err
		}
		defer tx.Rollback()
		m, found, err := s.pgFindMachineByClusterNodeNameTx(ctx, tx, name, true)
		if err != nil {
			return m, err
		}
		if !found {
			return m, ErrNotFound
		}
		if err = applyEdgeAdmission(&m, id); err != nil {
			return m, err
		}
		if err = s.pgUpsertMachineTx(ctx, tx, m); err != nil {
			return m, err
		}
		return m, tx.Commit()
	}
	var m model.Machine
	err := s.withLockedState(true, func(state *model.State) error {
		idx := findMachineByClusterNodeName(state, name)
		if idx < 0 {
			return ErrNotFound
		}
		m = state.Machines[idx]
		if err := applyEdgeAdmission(&m, id); err != nil {
			return err
		}
		state.Machines[idx] = m
		return nil
	})
	return m, err
}
