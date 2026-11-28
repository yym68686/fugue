package buildadmission

import (
	"context"

	"fugue/internal/model"
)

// Checkpoint stores observations in the existing transaction; a false return
// means another replica holds the scoped checkpoint lock.
type Checkpoint interface {
	Read(context.Context, string) ([]byte, error)
	Update(context.Context, string, func([]byte) ([]byte, error)) (bool, error)
}

// Evidence supplies only the historical app operations and builder facts that
// can seed a pre-existing OOM record during a compatible rollout.
type Evidence interface {
	Operations(model.App) ([]model.Operation, error)
	Attempts(model.App, string) ([]model.OperationEvidence, error)
}

type Service struct {
	checkpoint Checkpoint
	evidence   Evidence
}

func New(checkpoint Checkpoint, evidence Evidence) *Service {
	return &Service{checkpoint: checkpoint, evidence: evidence}
}
