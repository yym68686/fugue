package compositecoordinator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	RuntimeLaneAPIVersion = "release-domain.fugue.dev/v2"
	RuntimeLaneKind       = "CompositeRuntimeLane"
	RuntimeLanePolicy     = "single-active-record-cas-v1"
	RuntimeLaneKey        = "composite-runtime"
)

var (
	ErrInvalidRuntimeLane  = errors.New("invalid composite runtime lane")
	ErrRuntimeLaneConflict = errors.New("composite runtime lane conflict")
	nonNegativeDecimal     = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
)

// RuntimeLane is the dormant global mutation fence for composite releases.
// Record-local CAS remains necessary, but cannot by itself exclude a second
// valid record. Only the future guarded runtime path may consume a reservation.
type RuntimeLane struct {
	APIVersion                string    `json:"apiVersion"`
	Kind                      string    `json:"kind"`
	Policy                    string    `json:"policy"`
	Key                       string    `json:"key"`
	Generation                string    `json:"generation"`
	FencingEpoch              string    `json:"fencingEpoch"`
	Version                   int64     `json:"version"`
	ActiveRecordID            string    `json:"activeRecordId"`
	ActiveInitialRecordDigest string    `json:"activeInitialRecordDigest"`
	ActivePlanDigest          string    `json:"activePlanDigest"`
	LastSettledRecordID       string    `json:"lastSettledRecordId"`
	LastSettledRecordDigest   string    `json:"lastSettledRecordDigest"`
	LastSettledPlanDigest     string    `json:"lastSettledPlanDigest"`
	Frozen                    bool      `json:"frozen"`
	FreezeReason              string    `json:"freezeReason"`
	CreatedAt                 time.Time `json:"createdAt"`
	UpdatedAt                 time.Time `json:"updatedAt"`
	Digest                    string    `json:"digest"`
}

func (lane *RuntimeLane) UnmarshalJSON(data []byte) error {
	if err := validateStrictRuntimeLaneJSON(data); err != nil {
		return err
	}
	type laneAlias RuntimeLane
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded laneAlias
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("composite runtime lane has trailing JSON")
	}
	candidate := RuntimeLane(decoded)
	if err := VerifyRuntimeLane(candidate); err != nil {
		return err
	}
	*lane = candidate
	return nil
}

func validateStrictRuntimeLaneJSON(data []byte) error {
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return err
	}
	required := map[string]struct{}{
		"apiVersion": {}, "kind": {}, "policy": {}, "key": {}, "generation": {},
		"fencingEpoch": {}, "version": {}, "activeRecordId": {}, "lastSettledRecordId": {},
		"activeInitialRecordDigest": {}, "activePlanDigest": {}, "lastSettledRecordDigest": {}, "lastSettledPlanDigest": {},
		"frozen": {}, "freezeReason": {}, "createdAt": {}, "updatedAt": {}, "digest": {},
	}
	seen := make(map[string]struct{}, len(required))
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return errors.New("composite runtime lane must be an object")
	}
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return err
		}
		name, ok := nameToken.(string)
		if !ok {
			return errors.New("composite runtime lane field name must be a string")
		}
		if _, allowed := required[name]; !allowed {
			return fmt.Errorf("composite runtime lane contains unknown field %q", name)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("composite runtime lane contains duplicate field %q", name)
		}
		seen[name] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return errors.New("composite runtime lane object is not closed")
	}
	if len(seen) != len(required) {
		for name := range required {
			if _, present := seen[name]; !present {
				return fmt.Errorf("composite runtime lane is missing required field %q", name)
			}
		}
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("composite runtime lane has trailing JSON")
	}
	return nil
}

// NewRuntimeLaneFromHistory creates the additive genesis lane without
// granting mutation authority to any legacy record. Existing nonterminal
// history is ambiguous and therefore fails closed. A frozen record produces a
// frozen lane. Monotonic counters start after the largest verified history.
func NewRuntimeLaneFromHistory(records []Record, now time.Time) (RuntimeLane, error) {
	if now.IsZero() {
		return RuntimeLane{}, ErrInvalidRuntimeLane
	}
	records = append([]Record(nil), records...)
	sort.Slice(records, func(left, right int) bool {
		if records[left].UpdatedAt.Equal(records[right].UpdatedAt) {
			return records[left].ID < records[right].ID
		}
		return records[left].UpdatedAt.Before(records[right].UpdatedAt)
	})
	generation := "0"
	fencingEpoch := "0"
	lastSettled := ""
	lastSettledRecordDigest := ""
	lastSettledPlanDigest := ""
	frozen := false
	freezeReason := ""
	for _, record := range records {
		if VerifyRecord(record) != nil {
			return RuntimeLane{}, ErrInvalidRuntimeLane
		}
		var err error
		generation, err = maxCanonicalDecimal(generation, record.Plan.Generation)
		if err != nil {
			return RuntimeLane{}, ErrInvalidRuntimeLane
		}
		fencingEpoch, err = maxCanonicalDecimal(fencingEpoch, record.Plan.FencingEpoch)
		if err != nil {
			return RuntimeLane{}, ErrInvalidRuntimeLane
		}
		switch record.State {
		case StateCommitted, StateReverted:
			lastSettled = record.ID
			lastSettledRecordDigest = record.Digest
			lastSettledPlanDigest = record.Plan.Digest
		case StateFrozen:
			frozen = true
			freezeReason = "existing-frozen-record:" + record.ID
		case StatePrepared, StateApplying, StateObserving, StateReverting:
			return RuntimeLane{}, ErrRuntimeLaneConflict
		default:
			return RuntimeLane{}, ErrInvalidRuntimeLane
		}
	}
	now = now.UTC()
	lane := RuntimeLane{
		APIVersion: RuntimeLaneAPIVersion, Kind: RuntimeLaneKind, Policy: RuntimeLanePolicy,
		Key: RuntimeLaneKey, Generation: generation, FencingEpoch: fencingEpoch,
		Version: 0, ActiveRecordID: "", ActiveInitialRecordDigest: "", ActivePlanDigest: "",
		LastSettledRecordID: lastSettled, LastSettledRecordDigest: lastSettledRecordDigest,
		LastSettledPlanDigest: lastSettledPlanDigest,
		Frozen:                frozen, FreezeReason: freezeReason, CreatedAt: now, UpdatedAt: now,
	}
	lane.Digest = DigestRuntimeLane(lane)
	if VerifyRuntimeLane(lane) != nil {
		return RuntimeLane{}, ErrInvalidRuntimeLane
	}
	return lane, nil
}

// ReserveRuntimeLane performs the pure state transition used by both durable
// store backends. The plan must carry exactly the next generation and fencing
// epoch, and the caller must present the exact current lane version.
func ReserveRuntimeLane(
	lane RuntimeLane,
	record Record,
	expectedVersion int64,
	now time.Time,
) (RuntimeLane, error) {
	if VerifyRuntimeLane(lane) != nil || VerifyRecord(record) != nil || record.State != StatePrepared ||
		record.Revision != 1 || now.IsZero() {
		return RuntimeLane{}, ErrInvalidRuntimeLane
	}
	if lane.Frozen || lane.ActiveRecordID != "" || lane.Version != expectedVersion {
		return RuntimeLane{}, ErrRuntimeLaneConflict
	}
	nextGeneration, err := incrementCanonicalDecimal(lane.Generation)
	if err != nil || record.Plan.Generation != nextGeneration {
		return RuntimeLane{}, ErrRuntimeLaneConflict
	}
	nextFencingEpoch, err := incrementCanonicalDecimal(lane.FencingEpoch)
	if err != nil || record.Plan.FencingEpoch != nextFencingEpoch {
		return RuntimeLane{}, ErrRuntimeLaneConflict
	}
	if now.Before(lane.UpdatedAt) || lane.Version == int64(^uint64(0)>>1) {
		return RuntimeLane{}, ErrInvalidRuntimeLane
	}
	next := lane
	next.Generation = record.Plan.Generation
	next.FencingEpoch = record.Plan.FencingEpoch
	next.Version++
	next.ActiveRecordID = record.ID
	next.ActiveInitialRecordDigest = record.Digest
	next.ActivePlanDigest = record.Plan.Digest
	next.UpdatedAt = now.UTC()
	next.Digest = DigestRuntimeLane(next)
	if VerifyRuntimeLane(next) != nil {
		return RuntimeLane{}, ErrInvalidRuntimeLane
	}
	return next, nil
}

func VerifyRuntimeLane(lane RuntimeLane) error {
	if lane.APIVersion != RuntimeLaneAPIVersion || lane.Kind != RuntimeLaneKind ||
		lane.Policy != RuntimeLanePolicy || lane.Key != RuntimeLaneKey ||
		!validLaneDecimal(lane.Generation) || !validLaneDecimal(lane.FencingEpoch) ||
		lane.Version < 0 || lane.CreatedAt.IsZero() || lane.UpdatedAt.Before(lane.CreatedAt) ||
		(lane.ActiveRecordID != "" && !recordIDPattern.MatchString(lane.ActiveRecordID)) ||
		(lane.LastSettledRecordID != "" && !recordIDPattern.MatchString(lane.LastSettledRecordID)) ||
		!validLaneDigestBinding(lane.ActiveRecordID, lane.ActiveInitialRecordDigest, lane.ActivePlanDigest) ||
		!validLaneDigestBinding(lane.LastSettledRecordID, lane.LastSettledRecordDigest, lane.LastSettledPlanDigest) ||
		strings.TrimSpace(lane.FreezeReason) != lane.FreezeReason || len(lane.FreezeReason) > 1024 ||
		(lane.Frozen != (lane.FreezeReason != "")) || lane.Digest != DigestRuntimeLane(lane) {
		return ErrInvalidRuntimeLane
	}
	return nil
}

// VerifyRuntimeLaneHistory binds the global lane to all durable records. An
// empty active slot requires every historical record to be terminal; a set
// active slot must identify the only nonterminal record.
func VerifyRuntimeLaneHistory(lane RuntimeLane, records []Record) error {
	if VerifyRuntimeLane(lane) != nil {
		return ErrInvalidRuntimeLane
	}
	nonterminal := ""
	activeFound := false
	lastSettledFound := lane.LastSettledRecordID == ""
	for _, record := range records {
		if VerifyRecord(record) != nil {
			return ErrInvalidRuntimeLane
		}
		generation, err := maxCanonicalDecimal(lane.Generation, record.Plan.Generation)
		if err != nil || generation != lane.Generation {
			return ErrInvalidRuntimeLane
		}
		fencingEpoch, err := maxCanonicalDecimal(lane.FencingEpoch, record.Plan.FencingEpoch)
		if err != nil || fencingEpoch != lane.FencingEpoch {
			return ErrInvalidRuntimeLane
		}
		switch record.State {
		case StatePrepared, StateApplying, StateObserving, StateReverting:
			if nonterminal != "" && nonterminal != record.ID {
				return ErrRuntimeLaneConflict
			}
			nonterminal = record.ID
		case StateCommitted, StateReverted, StateFrozen:
		default:
			return ErrInvalidRuntimeLane
		}
		if record.ID == lane.ActiveRecordID {
			initial, err := NewRecord(record.ID, record.Plan, record.CreatedAt)
			if err != nil || initial.Digest != lane.ActiveInitialRecordDigest || record.Plan.Digest != lane.ActivePlanDigest {
				return ErrRuntimeLaneConflict
			}
			activeFound = true
		}
		if record.ID == lane.LastSettledRecordID {
			if record.State != StateCommitted && record.State != StateReverted {
				return ErrInvalidRuntimeLane
			}
			if record.Digest != lane.LastSettledRecordDigest || record.Plan.Digest != lane.LastSettledPlanDigest {
				return ErrRuntimeLaneConflict
			}
			lastSettledFound = true
		}
	}
	if !lastSettledFound ||
		(nonterminal != "" && lane.ActiveRecordID != nonterminal) ||
		(lane.ActiveRecordID == "" && nonterminal != "") ||
		(lane.ActiveRecordID != "" && !activeFound) {
		return ErrRuntimeLaneConflict
	}
	return nil
}

func DigestRuntimeLane(lane RuntimeLane) string {
	lane.Digest = ""
	encoded, err := json.Marshal(lane)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func incrementCanonicalDecimal(value string) (string, error) {
	if !validLaneDecimal(value) {
		return "", ErrInvalidRuntimeLane
	}
	integer, err := strconv.ParseUint(value, 10, 64)
	if err != nil || integer == ^uint64(0) {
		return "", ErrInvalidRuntimeLane
	}
	return strconv.FormatUint(integer+1, 10), nil
}

func maxCanonicalDecimal(left, right string) (string, error) {
	if !validLaneDecimal(left) || !validLaneDecimal(right) {
		return "", ErrInvalidRuntimeLane
	}
	leftInteger, leftErr := strconv.ParseUint(left, 10, 64)
	rightInteger, rightErr := strconv.ParseUint(right, 10, 64)
	if leftErr != nil || rightErr != nil {
		return "", ErrInvalidRuntimeLane
	}
	if leftInteger >= rightInteger {
		return left, nil
	}
	return right, nil
}

func validLaneDecimal(value string) bool {
	if len(value) == 0 || len(value) > 20 || !nonNegativeDecimal.MatchString(value) {
		return false
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}

func validLaneDigestBinding(id, recordDigest, planDigest string) bool {
	if id == "" {
		return recordDigest == "" && planDigest == ""
	}
	return digestPattern.MatchString(recordDigest) && digestPattern.MatchString(planDigest)
}
