package edgegroupfront

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

const fixedWorkerRotationSchema = "fugue.worker-transport-fixed/v2"
const fixedWorkerBindingsSchema = "fugue.worker-fixed-bindings/v1"
const fixedWorkerBindingsLimit = 32 << 10
const fixedWorkerAdvanceReason = "advance fixed Worker using journal "
const fixedWorkerRestoreReason = "restore fixed Worker using journal "

type fixedWorkerBindings struct {
	Schema   string              `json:"schema"`
	Journals []NodeWorkerJournal `json:"journals"`
}

// ObserveFixedWorkerRotation is a read-only check of the serving Front, not
// authority to prepare a publication or change the inactive slot.
func ObserveFixedWorkerRotation(cfg Config, id NativeRecipient) (ActivationState, error) {
	fail := errors.New("Front has no healthy fixed Worker rotation binding")
	if id.Node != cfg.EdgeID || id.Group != cfg.EdgeGroupID {
		return ActivationState{}, fail
	}
	if err := cfg.prepareWorkerTransport(); err != nil || !cfg.fixedWorkerRotation {
		return ActivationState{}, errors.Join(fail, err)
	}
	state, found, err := ReadActivationState(cfg.ActiveSlotFile)
	if err != nil || !found || state.GroupID != cfg.EdgeGroupID {
		return ActivationState{}, errors.Join(fail, err)
	}
	if _, _, ok := cfg.activationWorkerTargets(state); !ok {
		return ActivationState{}, fail
	}
	return state, nil
}

func (cfg Config) fixedWorkerBindingsPath() string { return cfg.ActiveSlotFile + ".worker-bindings" }

func (cfg Config) readFixedWorkerBindings() ([]NodeWorkerJournal, error) {
	fd, err := unix.Open(cfg.fixedWorkerBindingsPath(), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), cfg.fixedWorkerBindingsPath())
	defer f.Close()
	bad := errors.New("invalid durable fixed Worker bindings")
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() < 1 || st.Size() > fixedWorkerBindingsLimit {
		return nil, errors.Join(bad, err)
	}
	raw, err := io.ReadAll(io.LimitReader(f, fixedWorkerBindingsLimit+1))
	var bindings fixedWorkerBindings
	if err != nil || int64(len(raw)) != st.Size() || json.Unmarshal(raw, &bindings) != nil ||
		bindings.Schema != fixedWorkerBindingsSchema || len(bindings.Journals) < 1 || len(bindings.Journals) > 2 {
		return nil, errors.Join(bad, err)
	}
	wire, err := json.Marshal(bindings)
	if err != nil || !bytes.Equal(raw, wire) {
		return nil, errors.Join(bad, err)
	}
	for i, j := range bindings.Journals {
		sealed, err := j.Seal()
		if err != nil || j.Digest != sealed.Digest || !j.FixedRotation ||
			j.Recipient.Group != cfg.EdgeGroupID || j.Recipient.Node != cfg.EdgeID {
			return nil, errors.Join(bad, err)
		}
		if i > 0 {
			switch bindings.Journals[i-1].Progress(j.Expected) {
			case NodeWorkerActivated, NodeWorkerReverted:
			default:
				return nil, bad
			}
		}
	}
	return bindings.Journals, nil
}

// Until its first promotion the Front uses its immutable bootstrap pins.
// After that, only a complete activation proven by the durable journal can
// select a recycled listener. Staging a successor leaves the old match valid.
func (cfg Config) activationWorkerTargets(a ActivationState) (SlotTargets, NodeWorkerJournal, bool) {
	if cfg.fixedWorkerRotation {
		journals, err := cfg.readFixedWorkerBindings()
		if err != nil {
			return SlotTargets{}, NodeWorkerJournal{}, false
		}
		for i := len(journals) - 1; i >= 0; i-- {
			j := journals[i]
			switch j.Progress(a) {
			case NodeWorkerActivated, NodeWorkerReverted:
				t, ok := cfg.Slots[a.ActiveSlot]
				return t, j, ok
			}
		}
		if len(journals) != 0 && journals[0].Progress(a) != NodeWorkerPending {
			return SlotTargets{}, NodeWorkerJournal{}, false
		}
		if strings.HasPrefix(a.Reason, fixedWorkerAdvanceReason) || strings.HasPrefix(a.Reason, fixedWorkerRestoreReason) {
			return SlotTargets{}, NodeWorkerJournal{}, false
		}
	}
	t, ok := cfg.workerTargets[workerPin(a)]
	return t, NodeWorkerJournal{}, ok
}

// Publish both the currently active and next binding before the activation
// CAS. A crash at either rename is safe: the activation itself is the only
// selector. The file lives beside the activation on the same Front volume.
func (cfg Config) stageFixedWorkerBinding(active, next NodeWorkerJournal, guard func() error) error {
	var journals []NodeWorkerJournal
	if active.Digest != "" {
		journals = append(journals, active)
	}
	journals = append(journals, next)
	raw, err := json.Marshal(fixedWorkerBindings{Schema: fixedWorkerBindingsSchema, Journals: journals})
	if err != nil {
		return err
	}
	return replaceActivationStateGuarded(cfg.fixedWorkerBindingsPath(), raw, guard)
}
