package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

const PendingRootResourcePoolSchema = "fugue.compiler-input.pending-root-resource-pool.v1"

// A finite pool is bound to one complete storage identity, not a df number or
// an arbitrarily renewable lease. These are trusted configuration, never HTTP
// request fields. The initial installation and the ledger's own allocation,
// WAL, outcome recording and uncertain-exit costs require an independent bound.
type PendingRootResourceStorageIdentity struct {
	ClusterUID, PGPodUID, PVCUID, PVUID, LonghornVolumeUID string
	Database, Schema                                       string
	DatabaseOID                                            uint32
}

type PendingRootResourcePool struct {
	Schema                  string
	Epoch                   uint64
	Storage                 PendingRootResourceStorageIdentity
	EvidenceSHA256          string
	ExpiresAt               time.Time
	Limits                  inputarchive.PendingRootWholeOperationBudgetLimits
	Aggregate               inputarchive.PendingRootWholeOperationBounds
	SourceVolumeBudgetBytes uint64
	MaxReservations         uint32
}

func resourceHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			if c < 'a' || c > 'f' {
				return false
			}
		}
	}
	return true
}

func resourceJSON(value any) (string, string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", "", err
	}
	hash := sha256.Sum256(raw)
	return string(raw), hex.EncodeToString(hash[:]), nil
}

func (p PendingRootResourcePool) Canonical() (string, string, error) {
	if p.Schema != PendingRootResourcePoolSchema || p.Epoch == 0 || p.Epoch > math.MaxInt64 ||
		!resourceHash(p.EvidenceSHA256) || p.ExpiresAt.IsZero() || p.MaxReservations == 0 || p.MaxReservations > 1024 ||
		p.SourceVolumeBudgetBytes == 0 || p.Storage.DatabaseOID == 0 {
		return "", "", ErrConflict
	}
	for _, field := range []string{p.Storage.ClusterUID, p.Storage.PGPodUID, p.Storage.PVCUID, p.Storage.PVUID, p.Storage.LonghornVolumeUID, p.Storage.Database, p.Storage.Schema} {
		if len(field) == 0 || len(field) > 256 || strings.TrimSpace(field) != field || strings.ContainsAny(field, "\x00\r\n\t") {
			return "", "", ErrConflict
		}
	}
	// Validate the limits without inventing any measured operation. An explicit
	// zero-temp cap is valid; the other five independent caps must be present.
	l := p.Limits
	if l.MaxNewPhysicalAllocationBoundBytes == 0 || l.MaxPeakWALBytes == 0 || l.MaxGrowthDuringBoundedWaitBytes == 0 ||
		l.MaxFailureExitAllocationBytes == 0 || l.MaxWaitAndExitTimeBoundSeconds == 0 || l.MaxReservationLifetime <= 0 {
		return "", "", ErrConflict
	}
	if !resourceBoundsFit(inputarchive.PendingRootWholeOperationBounds{
		NewPhysicalAllocationBoundBytes: l.MaxNewPhysicalAllocationBoundBytes, PeakWALBytes: l.MaxPeakWALBytes,
		PeakTempBytes: l.MaxPeakTempBytes, GrowthDuringBoundedWaitBytes: l.MaxGrowthDuringBoundedWaitBytes,
		FailureExitAllocationBytes: l.MaxFailureExitAllocationBytes, WaitAndExitTimeBoundSeconds: l.MaxWaitAndExitTimeBoundSeconds,
	}, p.Aggregate) {
		return "", "", ErrConflict
	}
	if _, ok := resourceVolumeCharge(p.Aggregate); !ok {
		return "", "", ErrConflict
	}
	raw, hash, err := resourceJSON(p)
	if err != nil || len(raw) > 8192 {
		return "", "", ErrConflict
	}
	return raw, hash, nil
}

type PendingRootResourceOperation struct {
	Kind, RequestSHA256 string
}

// Native binds the noncycling tickets and three append-only relations plus the immutable pool
// row. Reopening a process must read the existing charges and identity again.
type PendingRootResourcePoolSnapshot struct {
	Pool                 PendingRootResourcePool
	SHA256, Xmin, CTID   string
	Relations, Filenodes [4]int64
}

type PendingRootResourceAllocation struct {
	PoolSHA256, EvidenceSHA256 string
	AccountingIncluded         bool
	Budget                     inputarchive.PendingRootWholeOperationBudget
}

// The source supplies independently established whole-operation bounds and
// checks exact current physical storage and resource policy. No measurements,
// grants or bounds are inferred from the request or from this ledger. Its
// checks must be local and bounded: native callers recheck while holding locks.
type PendingRootResourceSource interface {
	PreparePendingRootResources(context.Context, PendingRootResourceOperation, PendingRootResourcePoolSnapshot) (PendingRootResourceWitness, error)
}

type PendingRootResourceWitness interface {
	PendingRootResourceAllocation() PendingRootResourceAllocation
	CheckPendingRootResources(context.Context) error
}

type PendingRootResourceAdmission struct {
	store  *Store
	source PendingRootResourceSource
	pool   PendingRootResourcePool
	raw    string
}

// Construction does not install/seed schema or enable a producer, renewer or
// resource collector. Unknown production bounds therefore remain disabled.
func NewPendingRootResourceAdmission(s *Store, source PendingRootResourceSource, pool PendingRootResourcePool) (*PendingRootResourceAdmission, error) {
	if s == nil || s.db == nil || source == nil {
		return nil, ErrConflict
	}
	raw, _, err := pool.Canonical()
	if err != nil {
		return nil, err
	}
	return &PendingRootResourceAdmission{s, source, pool, raw}, nil
}

func (a *PendingRootResourceAdmission) ReservePendingRoot(ctx context.Context, request inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
	_, hash, err := request.Canonical()
	if err != nil {
		return nil, err
	}
	reservation, err := a.reserve(ctx, PendingRootResourceOperation{"pending-root", hash})
	if err != nil {
		return nil, err
	}
	return reservation, nil
}

func (a *PendingRootResourceAdmission) ReservePendingRootPolicy(ctx context.Context, change PendingRootPolicyChange) (PendingRootPolicyReservation, error) {
	if _, err := inputarchive.DecodePendingRootAuthorityPolicy([]byte(change.PolicyJSON)); err != nil {
		return nil, err
	}
	_, hash, err := resourceJSON(change)
	if err != nil {
		return nil, err
	}
	reservation, err := a.reserve(ctx, PendingRootResourceOperation{"pending-policy", hash})
	if err != nil {
		return nil, err
	}
	return reservation, nil
}

func (a *PendingRootResourceAdmission) reserve(ctx context.Context, operation PendingRootResourceOperation) (*pendingRootResourceReservation, error) {
	if a == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tx, err := beginPendingRootPolicyTx(ctx, a.store.db, true)
	if err != nil {
		return nil, err
	}
	before, _, err := a.readPool(ctx, tx)
	_ = tx.Rollback()
	if err != nil {
		return nil, err
	}
	witness, err := a.source.PreparePendingRootResources(ctx, operation, before)
	if err != nil || witness == nil {
		return nil, ErrConflict
	}
	allocation := witness.PendingRootResourceAllocation()
	checkSource := func(now time.Time) error {
		b := allocation.Budget
		if allocation.PoolSHA256 != before.SHA256 || !allocation.AccountingIncluded || !resourceHash(allocation.EvidenceSHA256) ||
			b.ExpiresAt.After(a.pool.ExpiresAt) || !reflect.DeepEqual(witness.PendingRootResourceAllocation(), allocation) ||
			witness.CheckPendingRootResources(ctx) != nil || !reflect.DeepEqual(witness.PendingRootResourceAllocation(), allocation) {
			return ErrConflict
		}
		return inputarchive.ValidatePendingRootWholeOperationBudget(b, now, operation.RequestSHA256, a.pool.Epoch, a.pool.Limits)
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(nonce[:])
	allocationRaw, _, err := resourceJSON(allocation)
	if err != nil || len(allocationRaw) > 4096 {
		return nil, ErrConflict
	}
	tx, err = beginPendingRootPolicyTx(ctx, a.store.db, false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// READ COMMITTED after this shared lock serializes aggregate reservation
	// across all API processes. An earlier MVCC snapshot must not be used here.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue.pending-root-resource-reservations.v1',0))`); err != nil {
		return nil, err
	}
	current, now, err := a.readPool(ctx, tx)
	if err != nil || !reflect.DeepEqual(current, before) {
		return nil, ErrConflict
	}
	if err := checkSource(now); err != nil {
		return nil, err
	}
	count, charged, err := a.charged(ctx, tx)
	if err != nil || count >= a.pool.MaxReservations {
		return nil, ErrConflict
	}
	charged, ok := addResourceBounds(charged, resourceLimitBounds(a.pool.Limits))
	volume, volumeOK := resourceVolumeCharge(charged)
	if !ok || !volumeOK || !resourceBoundsFit(charged, a.pool.Aggregate) || volume > a.pool.SourceVolumeBudgetBytes {
		return nil, ErrConflict
	}
	// nextval is deliberately not rolled back. Its transaction must be durably
	// acknowledged BEFORE allocation-bearing journal/business SQL. An unknown
	// ticket ACK starts no operation. No setval/reset/epoch rollover is provided.
	var ticket int64
	if err := tx.QueryRowContext(ctx, `SELECT nextval('fugue_platform_input_resource_tickets')`).Scan(&ticket); err != nil || ticket != int64(count)+1 {
		return nil, ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	tx, err = beginPendingRootPolicyTx(ctx, a.store.db, false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	current, now, err = a.readPool(ctx, tx)
	if err != nil || !reflect.DeepEqual(current, before) {
		return nil, ErrConflict
	}
	if err := checkSource(now); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_resource_reservations(reservation_id,ticket,operation,request_sha256,created_at,allocation_json) VALUES($1,$2,$3,$4,$5,$6)`, id, ticket, operation.Kind, operation.RequestSHA256, now, allocationRaw); err != nil {
		return nil, err
	}
	// Recheck provider and DB time immediately before durable acknowledgement.
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	if err := checkSource(now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		// A lost reservation ACK leaves a durable charge. No retry or refund.
		return nil, err
	}
	return &pendingRootResourceReservation{admission: a, pool: before, operation: operation, id: id, raw: allocationRaw, allocation: allocation, witness: witness}, nil
}

func (a *PendingRootResourceAdmission) readPool(ctx context.Context, tx *pendingRootNativeTx) (PendingRootResourcePoolSnapshot, time.Time, error) {
	var snapshot PendingRootResourcePoolSnapshot
	var now time.Time
	if err := schemamigrate.VerifyCompilerInputResourceReservations(ctx, tx.Tx); err != nil {
		return snapshot, now, err
	}
	var raw, database, schema string
	var databaseOID uint32
	err := tx.QueryRowContext(ctx, `SELECT left(config_json,8193),xmin::text,ctid::text,current_database(),(SELECT oid FROM pg_database WHERE datname=current_database()),current_schema(),clock_timestamp()
 FROM fugue_platform_input_resource_pool WHERE singleton`).Scan(&raw, &snapshot.Xmin, &snapshot.CTID, &database, &databaseOID, &schema, &now)
	if err != nil || raw != a.raw || database != a.pool.Storage.Database || databaseOID != a.pool.Storage.DatabaseOID || schema != a.pool.Storage.Schema || !now.Before(a.pool.ExpiresAt) {
		return snapshot, now, ErrConflict
	}
	snapshot.Pool = a.pool
	_, snapshot.SHA256, _ = resourceJSON(a.pool)
	for i, table := range resourceRelations() {
		if err := tx.QueryRowContext(ctx, `SELECT c.oid::bigint,pg_relation_filenode(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($1) AND n.nspname=$2`, table, a.pool.Storage.Schema).Scan(&snapshot.Relations[i], &snapshot.Filenodes[i]); err != nil || snapshot.Relations[i] == 0 || snapshot.Filenodes[i] == 0 {
			return snapshot, now, ErrConflict
		}
	}
	return snapshot, now, nil
}

func (a *PendingRootResourceAdmission) charged(ctx context.Context, tx *pendingRootNativeTx) (uint32, inputarchive.PendingRootWholeOperationBounds, error) {
	var count uint32
	var total inputarchive.PendingRootWholeOperationBounds
	var last int64
	var called bool
	if err := tx.QueryRowContext(ctx, `SELECT last_value,is_called FROM fugue_platform_input_resource_tickets`).Scan(&last, &called); err != nil || last < 1 || last > 1024 {
		return 0, total, ErrConflict
	}
	if called {
		count = uint32(last)
	}
	if count > a.pool.MaxReservations {
		return 0, total, ErrConflict
	}
	// Every consumed ticket is charged at the configured per-attempt maximum,
	// including missing/rolled-back/uncertain journal rows. Filling a hole or
	// recording a smaller actual allocation never lowers this high-water charge.
	for i := uint32(0); i < count; i++ {
		var ok bool
		if total, ok = addResourceBounds(total, resourceLimitBounds(a.pool.Limits)); !ok {
			return 0, total, ErrConflict
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT ticket,operation,request_sha256,created_at,left(allocation_json,4097) FROM fugue_platform_input_resource_reservations LIMIT $1`, a.pool.MaxReservations+1)
	if err != nil {
		return 0, total, err
	}
	defer rows.Close()
	var rowsRead uint32
	for rows.Next() {
		rowsRead++
		var operation, hash, raw string
		var ticket int64
		var created time.Time
		if rowsRead > count || rows.Scan(&ticket, &operation, &hash, &created, &raw) != nil || ticket < 1 || ticket > int64(count) || len(raw) > 4096 || (operation != "pending-root" && operation != "pending-policy") {
			return 0, total, ErrConflict
		}
		var allocation PendingRootResourceAllocation
		decoder := json.NewDecoder(bytes.NewBufferString(raw))
		decoder.DisallowUnknownFields()
		_, poolHash, _ := resourceJSON(a.pool)
		if decoder.Decode(&allocation) != nil || decoder.Decode(new(any)) != io.EOF || allocation.Budget.ExpiresAt.After(a.pool.ExpiresAt) ||
			allocation.PoolSHA256 != poolHash || !allocation.AccountingIncluded || !resourceHash(allocation.EvidenceSHA256) ||
			inputarchive.ValidatePendingRootWholeOperationBudget(allocation.Budget, created, hash, a.pool.Epoch, a.pool.Limits) != nil {
			return 0, total, ErrConflict
		}
		canonical, _, _ := resourceJSON(allocation)
		if canonical != raw {
			return 0, total, ErrConflict
		}
	}
	return count, total, rows.Err()
}

type pendingRootResourceReservation struct {
	admission  *PendingRootResourceAdmission
	pool       PendingRootResourcePoolSnapshot
	operation  PendingRootResourceOperation
	id, raw    string
	allocation PendingRootResourceAllocation
	witness    PendingRootResourceWitness
	ended      atomic.Bool
	recordMu   sync.Mutex
	recorded   bool
	outcome    inputarchive.PendingRootOutcome
	outcomeErr error
}

func (r *pendingRootResourceReservation) CheckPendingRootReservation(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil || r.ended.Load() || !reflect.DeepEqual(r.witness.PendingRootResourceAllocation(), r.allocation) || r.witness.CheckPendingRootResources(ctx) != nil || !reflect.DeepEqual(r.witness.PendingRootResourceAllocation(), r.allocation) {
		return ErrConflict
	}
	tx, err := beginPendingRootPolicyTx(ctx, r.admission.store.db, true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, now, err := r.admission.readPool(ctx, tx)
	if err != nil || !reflect.DeepEqual(current, r.pool) {
		return ErrConflict
	}
	if err := inputarchive.ValidatePendingRootWholeOperationBudget(r.allocation.Budget, now, r.operation.RequestSHA256, r.pool.Pool.Epoch, r.pool.Pool.Limits); err != nil {
		return err
	}
	var raw, kind, hash string
	if err := tx.QueryRowContext(ctx, `SELECT allocation_json,operation,request_sha256 FROM fugue_platform_input_resource_reservations WHERE reservation_id=$1`, r.id).Scan(&raw, &kind, &hash); err != nil || raw != r.raw || kind != r.operation.Kind || hash != r.operation.RequestSHA256 {
		return ErrConflict
	}
	// A terminal outcome is never permission to execute this handle again.
	var ended bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_input_resource_outcomes WHERE reservation_id=$1)`, r.id).Scan(&ended); err != nil || ended {
		return ErrConflict
	}
	_, total, err := r.admission.charged(ctx, tx)
	volume, ok := resourceVolumeCharge(total)
	if err != nil || !ok || !resourceBoundsFit(total, r.pool.Pool.Aggregate) || volume > r.pool.Pool.SourceVolumeBudgetBytes || r.ended.Load() {
		return ErrConflict
	}
	return ctx.Err()
}

func (r *pendingRootResourceReservation) PendingRootWholeOperationBudget(ctx context.Context) (inputarchive.PendingRootWholeOperationBudget, error) {
	if err := r.CheckPendingRootReservation(ctx); err != nil {
		return inputarchive.PendingRootWholeOperationBudget{}, err
	}
	return r.allocation.Budget, nil
}

func (r *pendingRootResourceReservation) CheckPendingRootPolicyReservation(ctx context.Context) error {
	return r.CheckPendingRootReservation(ctx)
}

func (r *pendingRootResourceReservation) RecordPendingRootPolicyOutcome(outcome inputarchive.PendingRootOutcome) {
	r.RecordPendingRootOutcome(outcome)
}

func (r *pendingRootResourceReservation) RecordPendingRootOutcome(outcome inputarchive.PendingRootOutcome) {
	// The legacy interface cannot return an audit-write error. Failure therefore
	// retains the full charge and uncertain state. Callers needing an audit ACK
	// use RecordOutcome directly. No outcome, including not-attempted, refunds.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = r.RecordOutcome(ctx, outcome)
}

func (r *pendingRootResourceReservation) RecordOutcome(ctx context.Context, outcome inputarchive.PendingRootOutcome) error {
	if outcome != inputarchive.PendingRootNotAttempted && outcome != inputarchive.PendingRootCompletionNotConfirmed && outcome != inputarchive.PendingRootCommitted {
		return ErrConflict
	}
	// Even a failed audit write ends this in-memory handle. Reopening a new
	// provider cannot reconstruct handles; the durable charge remains present.
	r.ended.Store(true)
	r.recordMu.Lock()
	defer r.recordMu.Unlock()
	if r.recorded {
		if r.outcome != outcome {
			return ErrConflict
		}
		return r.outcomeErr
	}
	r.recorded, r.outcome = true, outcome
	r.outcomeErr = r.recordOutcome(ctx, outcome)
	return r.outcomeErr
}

func (r *pendingRootResourceReservation) recordOutcome(ctx context.Context, outcome inputarchive.PendingRootOutcome) error {
	if ctx == nil {
		return ErrConflict
	}
	// Exactly one bounded allocation-bearing outcome attempt, even if it rolls
	// back or loses its ACK. Repeated calls return that attempt's cached result.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := beginPendingRootPolicyTx(ctx, r.admission.store.db, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Outcome recording is permitted after expiry but only against the original
	// guarded relations, original immutable pool row and exact reservation.
	if err := schemamigrate.VerifyCompilerInputResourceReservations(ctx, tx.Tx); err != nil {
		return err
	}
	var raw, xmin, ctid string
	if err := tx.QueryRowContext(ctx, `SELECT config_json,xmin::text,ctid::text FROM fugue_platform_input_resource_pool WHERE singleton`).Scan(&raw, &xmin, &ctid); err != nil || raw != r.admission.raw || xmin != r.pool.Xmin || ctid != r.pool.CTID {
		return ErrConflict
	}
	for i, table := range resourceRelations() {
		var relation, filenode int64
		if err := tx.QueryRowContext(ctx, `SELECT to_regclass($1)::oid::bigint,pg_relation_filenode(to_regclass($1))`, table).Scan(&relation, &filenode); err != nil || relation != r.pool.Relations[i] || filenode != r.pool.Filenodes[i] {
			return ErrConflict
		}
	}
	var kind, hash string
	if err := tx.QueryRowContext(ctx, `SELECT allocation_json,operation,request_sha256 FROM fugue_platform_input_resource_reservations WHERE reservation_id=$1`, r.id).Scan(&raw, &kind, &hash); err != nil || raw != r.raw || kind != r.operation.Kind || hash != r.operation.RequestSHA256 {
		return ErrConflict
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_resource_outcomes(reservation_id,outcome) VALUES($1,$2) ON CONFLICT DO NOTHING`, r.id, outcome); err != nil {
		return err
	}
	var stored inputarchive.PendingRootOutcome
	if err := tx.QueryRowContext(ctx, `SELECT outcome FROM fugue_platform_input_resource_outcomes WHERE reservation_id=$1`, r.id).Scan(&stored); err != nil || stored != outcome {
		return ErrConflict
	}
	return tx.Commit()
}

func resourceRelations() [4]string {
	return [4]string{"fugue_platform_input_resource_pool", "fugue_platform_input_resource_reservations", "fugue_platform_input_resource_outcomes", "fugue_platform_input_resource_tickets"}
}

func resourceLimitBounds(l inputarchive.PendingRootWholeOperationBudgetLimits) inputarchive.PendingRootWholeOperationBounds {
	return inputarchive.PendingRootWholeOperationBounds{
		NewPhysicalAllocationBoundBytes: l.MaxNewPhysicalAllocationBoundBytes, PeakWALBytes: l.MaxPeakWALBytes, PeakTempBytes: l.MaxPeakTempBytes,
		GrowthDuringBoundedWaitBytes: l.MaxGrowthDuringBoundedWaitBytes, FailureExitAllocationBytes: l.MaxFailureExitAllocationBytes, WaitAndExitTimeBoundSeconds: l.MaxWaitAndExitTimeBoundSeconds,
	}
}

func resourceBoundsFit(a, b inputarchive.PendingRootWholeOperationBounds) bool {
	return a.NewPhysicalAllocationBoundBytes <= b.NewPhysicalAllocationBoundBytes && a.PeakWALBytes <= b.PeakWALBytes &&
		a.PeakTempBytes <= b.PeakTempBytes && a.GrowthDuringBoundedWaitBytes <= b.GrowthDuringBoundedWaitBytes &&
		a.FailureExitAllocationBytes <= b.FailureExitAllocationBytes && a.WaitAndExitTimeBoundSeconds <= b.WaitAndExitTimeBoundSeconds
}

func addResourceBounds(a, b inputarchive.PendingRootWholeOperationBounds) (inputarchive.PendingRootWholeOperationBounds, bool) {
	add := func(x, y uint64) (uint64, bool) { return x + y, y <= math.MaxUint64-x }
	var ok bool
	pairs := [][2]*uint64{
		{&a.NewPhysicalAllocationBoundBytes, &b.NewPhysicalAllocationBoundBytes},
		{&a.PeakWALBytes, &b.PeakWALBytes}, {&a.PeakTempBytes, &b.PeakTempBytes},
		{&a.GrowthDuringBoundedWaitBytes, &b.GrowthDuringBoundedWaitBytes},
		{&a.FailureExitAllocationBytes, &b.FailureExitAllocationBytes},
	}
	for _, pair := range pairs {
		if *pair[0], ok = add(*pair[0], *pair[1]); !ok {
			return inputarchive.PendingRootWholeOperationBounds{}, false
		}
	}
	// Time is a per-operation wall-clock bound, not additive capacity. Pool
	// expiry and live-source checks also constrain every reservation separately.
	if b.WaitAndExitTimeBoundSeconds > a.WaitAndExitTimeBoundSeconds {
		a.WaitAndExitTimeBoundSeconds = b.WaitAndExitTimeBoundSeconds
	}
	return a, true
}

func resourceVolumeCharge(b inputarchive.PendingRootWholeOperationBounds) (uint64, bool) {
	var total uint64
	for _, charge := range []uint64{b.NewPhysicalAllocationBoundBytes, b.PeakWALBytes, b.PeakTempBytes, b.GrowthDuringBoundedWaitBytes, b.FailureExitAllocationBytes} {
		if charge > math.MaxUint64-total {
			return 0, false
		}
		total += charge
	}
	return total, true
}

var _ inputarchive.PendingRootAdmission = (*PendingRootResourceAdmission)(nil)
var _ PendingRootPolicyAdmission = (*PendingRootResourceAdmission)(nil)
var _ inputarchive.PendingRootWholeOperationBudgetReservation = (*pendingRootResourceReservation)(nil)
