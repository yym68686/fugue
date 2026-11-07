package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
)

// Preparation is explicit maintenance, not an unbounded side effect of claim.
// The trusted runner must enforce physical/PG/WAL/worker gates and immutable
// operation receipts. This contract never authorizes claim, route or retirement.
type NodeTaskInputPreparationContract struct {
	Version            int    `json:"version"`
	TaskID             string `json:"task_id"`
	UpdaterID          string `json:"updater_id"`
	OperationKey       string `json:"operation_key"`
	ExpectedPlanSHA256 string `json:"expected_plan_sha256"`
	MaxInputs          int    `json:"max_inputs"`
	MaxRawBytes        int64  `json:"max_raw_bytes"`
	MaxStoredBytes     int64  `json:"max_stored_bytes"`
	MaxGetBytes        int64  `json:"max_get_bytes"`
	MaxGetRequests     int64  `json:"max_get_requests"`
	MaxSeconds         int    `json:"max_seconds"`
}

func (c NodeTaskInputPreparationContract) Validate(apply bool) error {
	if c.Version != 1 || c.TaskID == "" || len(c.TaskID) > 128 || c.UpdaterID == "" || len(c.UpdaterID) > 128 ||
		!compilerInputPinKeyPattern.MatchString(c.OperationKey) || len(c.OperationKey) > 48 ||
		c.MaxInputs < 1 || c.MaxInputs > 32 || c.MaxRawBytes < 1 || c.MaxRawBytes > 128<<20 ||
		c.MaxStoredBytes < 1 || c.MaxStoredBytes > 128<<20 || c.MaxGetBytes < 1 || c.MaxGetBytes > 448<<20 ||
		c.MaxGetRequests < 1 || c.MaxGetRequests > 32768 || c.MaxSeconds < 1 || c.MaxSeconds > 540 ||
		apply && !inputDigestPattern.MatchString("sha256:"+c.ExpectedPlanSHA256) ||
		!apply && c.ExpectedPlanSHA256 != "" {
		return ErrInvalidInput
	}
	return nil
}

type NodeTaskInputPreparationReport struct {
	Action               string                        `json:"action"`
	TaskID               string                        `json:"task_id"`
	PlanSHA256           string                        `json:"plan_sha256"`
	InputCount           int                           `json:"input_count"`
	RawBytes             int64                         `json:"raw_bytes"`
	StoredBytes          int64                         `json:"stored_bytes"`
	Pins                 []CompilerInputLocalPinReport `json:"pins"`
	Usage                inputarchive.CopyUsage        `json:"usage"`
	Prepared             bool                          `json:"prepared"`
	TaskActivated        bool                          `json:"task_activated"`
	RetirementAuthorized bool                          `json:"retirement_authorized"`
}

func RunNodeTaskInputPreparation(ctx context.Context, databaseURL string, keyring bundleauth.Keyring, c NodeTaskInputPreparationContract, apply bool) (NodeTaskInputPreparationReport, error) {
	if c.Validate(apply) != nil || keyring.PrimaryKey == "" || keyring.PrimaryKeyID == "" {
		return NodeTaskInputPreparationReport{}, ErrInvalidInput
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return NodeTaskInputPreparationReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// No Init: maintenance must never seed or repair application state.
	s := &Store{db: db, databaseURL: databaseURL}
	s.ConfigurePlatformArtifactSigning(keyring)
	return s.prepareNodeTaskInputs(ctx, c, apply, nil, "")
}

func (s *Store) prepareNodeTaskInputs(ctx context.Context, c NodeTaskInputPreparationContract, apply bool, objects inputarchive.Objects, backend string) (report NodeTaskInputPreparationReport, err error) {
	report = NodeTaskInputPreparationReport{Action: "task-input-prepare", TaskID: c.TaskID, Pins: []CompilerInputLocalPinReport{}}
	if c.Validate(apply) != nil {
		return report, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.MaxSeconds)*time.Second)
	defer cancel()
	if apply {
		enabled, e := inputarchive.MigrationEnabledFromEnv()
		if e != nil || !enabled {
			return report, ErrConflict
		}
	}
	var state string
	if err = s.db.QueryRowContext(ctx, `SELECT status FROM fugue_node_update_tasks WHERE id=$1 AND node_updater_id=$2`, c.TaskID, c.UpdaterID).Scan(&state); err != nil {
		return report, err
	}
	if state != "pending" && state != "running" {
		return report, ErrConflict
	}
	initial, err := s.prepareCompilerInputActivation(ctx, nodeTaskActivationRoots(c.TaskID))
	if err != nil {
		return report, err
	}
	report.PlanSHA256 = nodeTaskPreparationPlan(initial)
	report.InputCount = len(initial.inputs)
	for _, n := range initial.closure.Nodes {
		if n.Identity.Kind == "input" {
			report.RawBytes += n.LogicalBytes
		}
	}
	if report.InputCount > c.MaxInputs || report.RawBytes > c.MaxRawBytes || apply && report.PlanSHA256 != c.ExpectedPlanSHA256 {
		return report, ErrConflict
	}
	if !apply {
		return report, nil
	}
	var budget *inputarchive.BudgetObjects
	defer func() {
		if budget != nil {
			report.Usage = budget.Usage()
		}
	}()
	for _, n := range initial.closure.Nodes {
		if n.Identity.Kind != "input" {
			continue
		}
		if n.NeedsLocalCopy {
			if budget == nil {
				if objects == nil {
					cfg, configured, e := resolveCompilerInputArchiveConfig(ctx, s.db, false)
					if e != nil || !configured {
						return report, ErrConflict
					}
					// Managed DB configuration already resolves the reader.
					_, fromEnv, e := inputarchive.ConfigFromEnv()
					if e != nil {
						return report, ErrConflict
					}
					if fromEnv {
						cfg, err = taskPreparationReaderConfig(cfg)
						if err != nil {
							return report, err
						}
					}
					objects, err = inputarchive.NewSingleAttemptS3(cfg)
					if err != nil {
						return report, err
					}
					backend, err = cfg.Fingerprint()
					if err != nil {
						return report, err
					}
				}
				budget, err = inputarchive.NewBudgetObjects(objects, taskPreparationNoPut{}, inputarchive.CopyBudget{MaxPutBytes: 1, MaxPutRequests: 1, MaxGetBytes: c.MaxGetBytes, MaxGetRequests: c.MaxGetRequests, BytesPerSecond: 2 << 20})
				if err != nil {
					return report, err
				}
			}
			if report.StoredBytes >= c.MaxStoredBytes {
				return report, ErrConflict
			}
			restored, e := restoreCompilerInputArchiveHot(ctx, s.db, budget, backend, CompilerInputArchiveHotOptions{Digest: n.Identity.ID, Apply: true, MaxStoredBytes: c.MaxStoredBytes - report.StoredBytes})
			report.StoredBytes += restored.StoredBytes
			if e != nil {
				return report, e
			}
		}
		sum := sha256.Sum256([]byte(n.Identity.ID))
		pin, e := runCompilerInputLocalPin(ctx, s.db, CompilerInputLocalPinOptions{Key: c.OperationKey + "/" + hex.EncodeToString(sum[:]), Digest: n.Identity.ID, MaxRawBytes: n.LogicalBytes, Apply: true})
		report.Pins = append(report.Pins, pin)
		if e != nil || !pin.Verified {
			return report, errors.Join(e, ErrConflict)
		}
	}
	// Restores legitimately change input tuples. Everything else, including
	// task ownership/status/payload and signed artifact tuples, must be exact.
	// A fresh single-snapshot graph is verified, never pieced together.
	fresh, pins, err := s.prepareNodeTaskInputAdmission(ctx, c.TaskID)
	if err != nil || !sameTaskPreparationClosure(initial, fresh) {
		return report, errors.Join(err, ErrConflict)
	}
	err = s.withPreparedCompilerInputActivation(ctx, fresh, pins, func(ctx context.Context, tx *sql.Tx) error {
		var valid bool
		if err := tx.QueryRowContext(ctx, `SELECT node_updater_id=$2 AND status IN ('pending','running') FROM fugue_node_update_tasks WHERE id=$1`, c.TaskID, c.UpdaterID).Scan(&valid); err != nil {
			return err
		}
		if !valid {
			return ErrConflict
		}
		return nil
	})
	if err != nil {
		return report, err
	}
	report.Prepared = true
	return report, nil
}

// Legacy env configuration names the writer by default. A task restore must
// explicitly select the separate reader role; no writer/session fallback.
func taskPreparationReaderConfig(cfg inputarchive.S3Config) (inputarchive.S3Config, error) {
	writerID := cfg.AccessKeyID
	cfg.AccessKeyID = os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_ACCESS_KEY_ID")
	cfg.SecretAccessKey = os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_SECRET_ACCESS_KEY")
	if cfg.Validate() != nil || cfg.AccessKeyID == writerID || cfg.SessionToken != "" {
		return inputarchive.S3Config{}, ErrConflict
	}
	return cfg, nil
}

// A metadata graph hash alone does not bind changed task payload/owner rows
// or manifest geometry. Include every physical tuple witness, not a horizon
// (which necessarily changes between preview and apply).
func nodeTaskPreparationPlan(p *compilerInputActivationProof) string {
	tuples := make([][6]any, 0, len(p.tuples))
	for _, t := range p.tuples {
		tuples = append(tuples, [6]any{t.table, t.key, t.xmin, t.ctid, t.relation, t.filenode})
	}
	raw, _ := json.Marshal(struct {
		Closure string
		Tuples  [][6]any
	}{p.closure.PlanSHA256, tuples})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func nodeTaskActivationRoots(taskID string) []inputarchive.HotRoot {
	return []inputarchive.HotRoot{{Source: "task-claim", ID: taskID, Reference: inputarchive.HotReference{Kind: "task", ID: taskID}}}
}

func sameTaskPreparationClosure(a, b *compilerInputActivationProof) bool {
	if a == nil || b == nil || a.keyringRevision != b.keyringRevision || !reflect.DeepEqual(a.inputs, b.inputs) || len(a.closure.Nodes) != len(b.closure.Nodes) || len(a.inputManifests) != len(a.inputs) || len(b.inputManifests) != len(b.inputs) {
		return false
	}
	for _, digest := range a.inputs {
		x, okX := a.inputManifests[digest]
		y, okY := b.inputManifests[digest]
		if !okX || !okY || x.Location != y.Location && !(x.Location == "legacy" && y.Location == "hot") {
			return false
		}
		// Only the restored physical location may change. A fresh valid pin
		// for changed geometry/created_at cannot silently replace this plan.
		x.Location, y.Location = "", ""
		if x != y {
			return false
		}
	}
	for i, n := range a.closure.Nodes {
		m := b.closure.Nodes[i]
		if n.Identity.Kind == "input" {
			n.Location, m.Location = "", ""
			n.NeedsLocalCopy, m.NeedsLocalCopy = false, false
		}
		if !reflect.DeepEqual(n, m) {
			return false
		}
	}
	filter := func(p *compilerInputActivationProof) []compilerInputActivationTuple {
		x := []compilerInputActivationTuple{}
		for _, t := range p.tuples {
			if t.table != "fugue_platform_input_manifests" {
				x = append(x, t)
			}
		}
		return x
	}
	return reflect.DeepEqual(filter(a), filter(b))
}

type taskPreparationNoPut struct{}

func (taskPreparationNoPut) Put(context.Context, string, []byte) error { return ErrConflict }
func (taskPreparationNoPut) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, ErrConflict
}
