package observability

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"time"

	"fugue/internal/kubelease"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

const (
	logCheckpointAnnotation         = "fugue.pro/log-checkpoint"
	logCheckpointLimit              = 240 << 10 // Below the Kubernetes annotation total limit.
	logCheckpointSources            = 2048
	logCheckpointFilterBytes        = 8192
	logCheckpointLeaseSeconds int32 = 60
)

var errLogCheckpointOwnership = errors.New("log checkpoint ownership lost")

// The Lease is both the writer fence and its checkpoint: they change in one
// resourceVersion-CAS update. A paused predecessor cannot overwrite a successor.
// Values contain only inclusive source positions and hashed boundary witnesses.
// Source identities are hashed; no log text, attributes or credentials are stored.
type logCheckpointState struct {
	Version int                              `json:"v"`
	Sources map[string]logCheckpointPosition `json:"s"`
	// Compacted sources share an inclusive lower bound, never a newer lookback.
	// This trades duplicate replay and boundary proof for bounded storage, not
	// unread coverage. Missing witnesses are reported as unproven on recovery.
	ReplayFloor  int64  `json:"f,omitempty"`
	ReplayFilter []byte `json:"m,omitempty"`
}

type logCheckpointPosition struct {
	Time     int64          `json:"t"`
	Drained  int64          `json:"d,omitempty"`
	Visited  int64          `json:"v"`
	Complete bool           `json:"c,omitempty"`
	Unproven bool           `json:"u,omitempty"`
	Boundary map[string]int `json:"b,omitempty"`
}

type logCheckpoint struct {
	client       coordinationclient.LeaseInterface
	name, holder string
	lease        *coordinationv1.Lease
	acquired     bool
}

func newLogCheckpoint(client coordinationclient.LeaseInterface, namespace, name string) (*logCheckpoint, error) {
	if len(validation.IsDNS1123Label(namespace)) != 0 || namespace == "" ||
		len(validation.IsDNS1123Subdomain(name)) != 0 || name == "" {
		return nil, errors.New("log checkpoint namespace/name is invalid")
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, err
	}
	return &logCheckpoint{client: client, name: name, holder: hex.EncodeToString(token[:])}, nil
}

func logCheckpointKey(identity string) string {
	hash := sha256.Sum256([]byte(identity))
	return base64.RawURLEncoding.EncodeToString(hash[:])
}

func decodeLogCheckpoint(raw string) (logCheckpointState, error) {
	var state logCheckpointState
	if len(raw) > logCheckpointLimit || json.Unmarshal([]byte(raw), &state) != nil || state.Version != 1 || state.Sources == nil || len(state.Sources) > logCheckpointSources || state.ReplayFloor < 0 || state.ReplayFloor > 0 && len(state.ReplayFilter) != logCheckpointFilterBytes || state.ReplayFloor == 0 && len(state.ReplayFilter) != 0 {
		return state, errors.New("log checkpoint schema or bound is invalid")
	}
	for key, value := range state.Sources {
		hash, err := base64.RawURLEncoding.DecodeString(key)
		if err != nil || len(hash) != sha256.Size || value.Time <= 0 || value.Drained < 0 || value.Visited <= 0 {
			return state, errors.New("log checkpoint source position is invalid")
		}
		for hash, count := range value.Boundary {
			raw, err := base64.RawURLEncoding.DecodeString(hash)
			if err != nil || len(raw) != sha256.Size || count <= 0 {
				return state, errors.New("log checkpoint boundary is invalid")
			}
		}
	}
	return state, nil
}

// A contender waits without reading logs. Invalid or missing state on an
// existing Lease fails closed; a storage failure must never reset to five minutes.
func (s *logCheckpoint) acquire(ctx context.Context, now time.Time) (logCheckpointState, bool, error) {
	state := logCheckpointState{Version: 1, Sources: map[string]logCheckpointPosition{}}
	current, err := s.client.Get(ctx, s.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if s.acquired {
			return state, false, errLogCheckpointOwnership
		}
		raw, _ := json.Marshal(state)
		current = &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: s.name, Annotations: map[string]string{logCheckpointAnnotation: string(raw)}}}
		s.renew(current, now)
		created, createErr := s.client.Create(ctx, current, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(createErr) {
			return state, false, nil
		}
		if createErr != nil {
			return state, false, createErr
		}
		s.lease, s.acquired = created, true
		return state, true, nil
	}
	if err != nil {
		return state, false, err
	}
	state, err = decodeLogCheckpoint(current.Annotations[logCheckpointAnnotation])
	if err != nil {
		return state, false, err
	}
	holder := ""
	if current.Spec.HolderIdentity != nil {
		holder = *current.Spec.HolderIdentity
	}
	if holder != s.holder {
		if s.acquired {
			return state, false, errLogCheckpointOwnership
		}
		if holder != "" && !kubelease.Expired(*current, now) {
			return state, false, nil
		}
	}
	s.renew(current, now)
	updated, err := s.client.Update(ctx, current, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return state, false, nil
	}
	if err != nil {
		return state, false, err
	}
	s.lease, s.acquired = updated, true
	return state, true, nil
}

func (s *logCheckpoint) renew(lease *coordinationv1.Lease, now time.Time) {
	holder, duration, at := s.holder, logCheckpointLeaseSeconds, metav1.NewMicroTime(now.UTC())
	lease.Spec.HolderIdentity, lease.Spec.LeaseDurationSeconds, lease.Spec.RenewTime = &holder, &duration, &at
}

func (s *logCheckpoint) save(ctx context.Context, state logCheckpointState, now time.Time, release bool) error {
	if s.lease == nil {
		return errLogCheckpointOwnership
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if _, err = decodeLogCheckpoint(string(raw)); err != nil {
		return err
	}
	current := s.lease.DeepCopy()
	current.Annotations[logCheckpointAnnotation] = string(raw)
	s.renew(current, now)
	if release {
		empty := ""
		current.Spec.HolderIdentity = &empty
	}
	updated, err := s.client.Update(ctx, current, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
		return errLogCheckpointOwnership
	}
	if err == nil {
		s.lease = updated
	}
	return err
}

// Persisting admission would skip records still in the queue during a restart.
// Pin the inclusive frontier behind every pending or failed export. Boundaries
// intentionally replay after recovery: delivery is at least once, never exactly once.
func (c *kubernetesLogCollector) checkpointState() (logCheckpointState, error) {
	floors := c.deliveries.floors()
	state := logCheckpointState{Version: 1, Sources: map[string]logCheckpointPosition{}, ReplayFloor: c.replayFloor, ReplayFilter: append([]byte(nil), c.replayFilter...)}
	c.cursorsMu.Lock()
	defer c.cursorsMu.Unlock()
	for identity, cur := range c.cursors {
		at, drained := cur.Time, cur.DrainedThrough
		if floor := floors[identity]; !floor.IsZero() {
			at = earlierLogTime(at, floor)
			drained = time.Time{}
		}
		var drainedNanos int64
		if !drained.IsZero() {
			drainedNanos = drained.UnixNano()
		}
		position := logCheckpointPosition{Time: at.UnixNano(), Drained: drainedNanos, Visited: cur.Visited.Unix(), Complete: cur.Complete && floors[identity].IsZero(), Unproven: cur.RecoveryUnproven}
		if at.Equal(cur.Time) {
			boundary := cur.Boundary
			if len(cur.RecoveryBoundary) > 0 {
				boundary = cur.RecoveryBoundary
			}
			position.Boundary = map[string]int{}
			for hash, count := range boundary {
				position.Boundary[base64.RawURLEncoding.EncodeToString(hash[:])] = count
			}
		}
		state.Sources[logCheckpointKey(identity)] = position
	}
	// A discovery/cache TTL is not a delivery acknowledgement. Keep undiscovered
	// positions, or conservatively fold them into a shared inclusive replay floor.
	for key, position := range c.restored {
		if _, exists := state.Sources[key]; !exists {
			state.Sources[key] = position
		}
	}
	return compactLogCheckpoint(state)
}

// Bound both source count and actual JSON bytes. Old exact positions can be
// replaced with their minimum timestamp and a fixed membership filter: matching
// sources replay from at or before their unread frontier. False positives only
// add replay. No compacted source is silently reset to now-5m.
// One linear serialization plus a sort avoids repeatedly encoding the full map.
func compactLogCheckpoint(state logCheckpointState) (logCheckpointState, error) {
	type entry struct {
		key      string
		position logCheckpointPosition
		size     int
	}
	entries := make([]entry, 0, len(state.Sources))
	raw, err := json.Marshal(state)
	if err != nil {
		return state, err
	}
	size := len(raw)
	if len(state.Sources) <= logCheckpointSources && size <= logCheckpointLimit {
		return state, nil
	}
	for key, position := range state.Sources {
		encoded, err := json.Marshal(position)
		if err != nil {
			return state, err
		}
		entries = append(entries, entry{key, position, len(key) + len(encoded) + 4})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].position.Complete != entries[j].position.Complete {
			return entries[i].position.Complete
		}
		if entries[i].position.Visited != entries[j].position.Visited {
			return entries[i].position.Visited < entries[j].position.Visited
		}
		return entries[i].key < entries[j].key
	})
	// Reserve for the optional fixed membership filter, floor and separator.
	// A false positive causes extra replay; a negative can never hide a compacted
	// identity. This avoids relying on clocks to distinguish new sources.
	reserve := 32
	if len(state.ReplayFilter) == 0 {
		reserve += base64.StdEncoding.EncodedLen(logCheckpointFilterBytes) + 8
	}
	for _, entry := range entries {
		if len(state.Sources) <= logCheckpointSources && size <= logCheckpointLimit-reserve {
			break
		}
		if !entry.position.Complete {
			state.ReplayFloor = earlierCheckpointNanos(state.ReplayFloor, entry.position.Time)
			if len(state.ReplayFilter) == 0 {
				state.ReplayFilter = make([]byte, logCheckpointFilterBytes)
			}
			logCheckpointMembership(state.ReplayFilter, entry.key, true)
		}
		delete(state.Sources, entry.key)
		size -= entry.size
	}
	raw, err = json.Marshal(state)
	if err != nil {
		return state, err
	}
	if _, err = decodeLogCheckpoint(string(raw)); err != nil {
		return state, err
	}
	return state, nil
}

func earlierCheckpointNanos(a, b int64) int64 {
	if a == 0 || b > 0 && b < a {
		return b
	}
	return a
}

func logCheckpointMembership(filter []byte, key string, add bool) bool {
	raw, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil || len(raw) != sha256.Size || len(filter) != logCheckpointFilterBytes {
		return true
	}
	present := true
	for i := 0; i < 8; i += 2 {
		bit := binary.LittleEndian.Uint16(raw[i : i+2])
		mask := byte(1 << (bit % 8))
		present = present && filter[int(bit)/8]&mask != 0
		if add {
			filter[int(bit)/8] |= mask
		}
	}
	return present
}

func (c *kubernetesLogCollector) restoreCursor(identity string, now time.Time) kubernetesLogCursor {
	cur := kubernetesLogCursor{Time: now.Add(-5 * time.Minute), Boundary: map[[32]byte]int{}, FirstObserved: now}
	if c.replayFloor > 0 && logCheckpointMembership(c.replayFilter, logCheckpointKey(identity), false) {
		cur.Time = earlierLogTime(cur.Time, time.Unix(0, c.replayFloor).UTC())
		cur.RecoveringRotation, cur.RecoveryUnproven = true, true
	}
	if position, ok := c.restored[logCheckpointKey(identity)]; ok {
		cur.Time = time.Unix(0, position.Time).UTC()
		cur.Complete = position.Complete
		if position.Drained > 0 {
			cur.DrainedThrough = time.Unix(0, position.Drained).UTC()
		}
		cur.RecoveryBoundary = map[[32]byte]int{}
		for hash, count := range position.Boundary {
			raw, _ := base64.RawURLEncoding.DecodeString(hash)
			cur.RecoveryBoundary[[32]byte(raw)] = count
		}
		cur.RecoveryUnproven = position.Unproven || len(cur.RecoveryBoundary) == 0
		// The current-file API cannot prove that the inclusive frontier survived
		// rotation while this collector was absent. Inspect retained files first.
		cur.RecoveringRotation = true
		delete(c.restored, logCheckpointKey(identity))
	}
	return cur
}

func (c *kubernetesLogCollector) retryFailedDelivery(identity string, cur *kubernetesLogCursor) {
	if at := c.deliveries.takeFailure(identity); !at.IsZero() {
		cur.Time = earlierLogTime(cur.Time, at)
		cur.Boundary = map[[32]byte]int{}
		cur.RecoveryBoundary = nil
		cur.RecoveryUnproven = true
		cur.Complete, cur.RecoveringRotation = false, true
		cur.DrainedThrough, cur.NextAttempt = time.Time{}, time.Time{}
		cur.PendingAt = cur.Time
	}
}

func (c *kubernetesLogCollector) saveCheckpoint(ctx context.Context, release bool) error {
	state, err := c.checkpointState()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := c.checkpoint.save(ctx, state, c.logNow(), release); err != nil {
		return fmt.Errorf("save Kubernetes log checkpoint: %w", err)
	}
	// Adopt the bounded durable representation only after the CAS succeeds.
	// Inactive sources with no pending/failed export may leave the identity
	// cache: their exact hashed position or the conservative floor survives.
	savedSources := len(state.Sources)
	raw, _ := json.Marshal(state)
	membershipBits := 0
	for _, value := range state.ReplayFilter {
		membershipBits += bits.OnesCount8(value)
	}
	c.cursorsMu.Lock()
	c.restored = state.Sources
	for key, cur := range c.cursors {
		if c.logNow().Sub(cur.Visited) > time.Hour && c.deliveries.sourceDelivered(key) {
			delete(c.cursors, key)
			continue
		}
		delete(c.restored, logCheckpointKey(key))
	}
	c.replayFloor = state.ReplayFloor
	c.replayFilter = state.ReplayFilter
	c.cursorsMu.Unlock()
	c.pipeline.sourceObservationMu.Lock()
	c.pipeline.sourceCheckpoint.Sources = savedSources
	c.pipeline.sourceCheckpoint.SerializedBytes = len(raw)
	c.pipeline.sourceCheckpoint.MembershipBits = membershipBits
	c.pipeline.sourceCheckpoint.ReplayFloor = time.Time{}
	if state.ReplayFloor > 0 {
		c.pipeline.sourceCheckpoint.ReplayFloor = time.Unix(0, state.ReplayFloor).UTC()
	}
	c.pipeline.sourceObservationMu.Unlock()
	return nil
}
