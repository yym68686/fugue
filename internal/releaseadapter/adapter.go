// Package releaseadapter defines the dormant single-domain transaction seam.
//
// The package does not select a domain, register a production adapter, or run
// a command on import. Boundary B only supplies the interface and state
// machine that Boundary C may activate after its pre-write gate.
package releaseadapter

import (
	"context"
	"fmt"
	"reflect"

	"fugue/internal/releasedomain"
)

// Phase is one fixed step in a single-domain release transaction.
type Phase string

const (
	PhasePrepare  Phase = "prepare"
	PhaseApply    Phase = "apply"
	PhaseVerify   Phase = "verify"
	PhaseRollback Phase = "rollback"
)

var orderedPhases = [...]Phase{
	PhasePrepare,
	PhaseApply,
	PhaseVerify,
	PhaseRollback,
}

// Command is one adapter phase. Prepare and Verify implementations must be
// read-only. Apply and Rollback are the only phase commands allowed to write.
// The runner passes only a verified immutable authorization, never the raw
// envelope bytes or a path that a phase could reopen.
type Command interface {
	Run(context.Context, releasedomain.ExecutionAuthorization) error
}

// CommandFunc adapts a function to Command. A nil CommandFunc is rejected
// before Prepare is called.
type CommandFunc func(context.Context, releasedomain.ExecutionAuthorization) error

// Run implements Command.
func (command CommandFunc) Run(ctx context.Context, authorization releasedomain.ExecutionAuthorization) error {
	return command(ctx, authorization)
}

// DomainAdapter exposes one complete, already-selected domain implementation.
// Domain selection and dispatch deliberately remain outside this B2 package.
// CommandFor must return one non-nil command for every fixed phase.
type DomainAdapter interface {
	Domain() releasedomain.Domain
	CommandFor(Phase) Command
}

type adapterSnapshot struct {
	domain   releasedomain.Domain
	commands map[Phase]Command
}

func snapshotDomainAdapter(adapter DomainAdapter) (snapshot adapterSnapshot, resultErr error) {
	if isNilInterface(adapter) {
		return adapterSnapshot{}, fmt.Errorf("release domain adapter is nil")
	}
	defer func() {
		if recover() != nil {
			snapshot = adapterSnapshot{}
			resultErr = fmt.Errorf("release domain adapter metadata panicked")
		}
	}()

	domain := adapter.Domain()
	if _, err := releasedomain.ParseDomain(string(domain)); err != nil {
		return adapterSnapshot{}, fmt.Errorf("release domain adapter: %w", err)
	}
	commands := make(map[Phase]Command, len(orderedPhases))
	for _, phase := range orderedPhases {
		command := adapter.CommandFor(phase)
		if isNilInterface(command) {
			return adapterSnapshot{}, fmt.Errorf("release domain adapter %q is missing %s command", domain, phase)
		}
		commands[phase] = command
	}
	return adapterSnapshot{domain: domain, commands: commands}, nil
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
