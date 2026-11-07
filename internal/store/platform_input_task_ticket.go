package store

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
)

const taskPreparationTicketMaxBytes = 512 << 10

var taskPreparationXmin = regexp.MustCompile(`^[0-9]{1,10}$`)
var taskPreparationCTID = regexp.MustCompile(`^\([0-9]{1,10},[0-9]{1,5}\)$`)

// This ticket selects bounded restoration work, NOT activation authority. Only
// the trusted API role can derive it after full signature/closure verification.
// The isolated worker receives its public verification key and dedicated GET
// role, never a platform signing/decryption key. Fresh trusted confirmation and
// claim independently verify the complete current closure before activation.
type NodeTaskInputPreparationTicket struct {
	Version       int                              `json:"version"`
	Contract      NodeTaskInputPreparationContract `json:"contract"`
	BackendHash   string                           `json:"backend_hash"`
	ClosureSHA256 string                           `json:"closure_sha256"`
	Epoch         int64                            `json:"epoch"`
	Horizon       int64                            `json:"horizon"`
	IssuedAt      time.Time                        `json:"issued_at"`
	ExpiresAt     time.Time                        `json:"expires_at"`
	Inputs        []taskPreparationManifest        `json:"inputs"`
	Tuples        []taskPreparationTuple           `json:"tuples"`
	Signature     string                           `json:"signature"`
}

type taskPreparationManifest struct {
	Digest    string    `json:"digest"`
	Format    string    `json:"format"`
	Location  string    `json:"location"`
	RawBytes  int64     `json:"raw_bytes"`
	ChunkSize int       `json:"chunk_size"`
	Chunks    int       `json:"chunks"`
	CreatedAt time.Time `json:"created_at"`
}

func (m taskPreparationManifest) manifest() inputManifest {
	return inputManifest{Digest: m.Digest, Format: m.Format, Location: m.Location, Size: m.RawBytes, ChunkSize: m.ChunkSize, Count: m.Chunks, Created: m.CreatedAt}
}

type taskPreparationTuple struct {
	Table    string `json:"table"`
	Key      string `json:"key"`
	Xmin     string `json:"xmin"`
	CTID     string `json:"ctid"`
	Relation int64  `json:"relation"`
	Filenode int64  `json:"filenode"`
}

type NodeTaskInputPreparationPlan struct {
	Ticket          NodeTaskInputPreparationTicket `json:"ticket"`
	VerificationKey string                         `json:"verification_key"`
}

// A domain-separated Ed25519 seed exposes only a public verification key to
// workers. It is not the bundle HMAC key and cannot sign platform artifacts.
func taskPreparationSigningKey(keys bundleauth.Keyring) (ed25519.PrivateKey, error) {
	return inputPreparationSigningKey(keys, "fugue.task-input-preparation.ed25519.v1")
}

func inputPreparationSigningKey(keys bundleauth.Keyring, domain string) (ed25519.PrivateKey, error) {
	if keys.PrimaryKey == "" || keys.PrimaryKeyID == "" {
		return nil, ErrConflict
	}
	if _, revoked := keys.RevokedKeyIDs[strings.ToLower(keys.PrimaryKeyID)]; revoked {
		return nil, ErrConflict
	}
	h := hmac.New(sha256.New, []byte(keys.PrimaryKey))
	_, _ = h.Write([]byte(domain + "\x00" + keys.PrimaryKeyID))
	return ed25519.NewKeyFromSeed(h.Sum(nil)), nil
}

func taskPreparationTicketPayload(t NodeTaskInputPreparationTicket) ([]byte, error) {
	t.Signature = ""
	return json.Marshal(t)
}

// Location is the one intentional mutation during restore. Bind complete
// roots/nodes/edges and every non-manifest physical tuple; input geometry is
// separately exact in Inputs. Dynamic RR horizon is checked, never hashed.
func taskPreparationClosureHash(p *compilerInputActivationProof) string {
	nodes := append([]inputarchive.HotClosureNode(nil), p.closure.Nodes...)
	for i := range nodes {
		if nodes[i].Identity.Kind == "input" {
			nodes[i].Location, nodes[i].NeedsLocalCopy = "", false
		}
	}
	raw, _ := json.Marshal(struct {
		Roots []inputarchive.HotRoot
		Nodes []inputarchive.HotClosureNode
	}{p.closure.Roots, nodes})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s *Store) planNodeTaskInputPreparation(ctx context.Context, c NodeTaskInputPreparationContract, backend string) (NodeTaskInputPreparationPlan, error) {
	var plan NodeTaskInputPreparationPlan
	if c.Validate(false) != nil || !inputDigestPattern.MatchString(backend) {
		return plan, ErrInvalidInput
	}
	var valid bool
	if err := s.db.QueryRowContext(ctx, `SELECT status IN ('pending','running') AND node_updater_id=$2 FROM fugue_node_update_tasks WHERE id=$1`, c.TaskID, c.UpdaterID).Scan(&valid); err != nil || !valid {
		return plan, errors.Join(err, ErrConflict)
	}
	p, err := s.prepareCompilerInputActivation(ctx, nodeTaskActivationRoots(c.TaskID))
	if err != nil {
		return plan, err
	}
	c.ExpectedPlanSHA256 = nodeTaskPreparationPlan(p)
	t := NodeTaskInputPreparationTicket{Version: 1, Contract: c, BackendHash: backend, ClosureSHA256: taskPreparationClosureHash(p), Epoch: p.sourceEpoch, Horizon: p.horizon,
		IssuedAt: time.Now().UTC(), Inputs: []taskPreparationManifest{}, Tuples: []taskPreparationTuple{}}
	t.ExpiresAt = t.IssuedAt.Add(10 * time.Minute)
	for _, digest := range p.inputs {
		m := p.inputManifests[digest]
		t.Inputs = append(t.Inputs, taskPreparationManifest{Digest: m.Digest, Format: m.Format, Location: m.Location, RawBytes: m.Size, ChunkSize: m.ChunkSize, Chunks: m.Count, CreatedAt: m.Created})
	}
	for _, v := range p.tuples {
		if v.table != "fugue_platform_input_manifests" {
			t.Tuples = append(t.Tuples, taskPreparationTuple{v.table, v.key, v.xmin, v.ctid, v.relation, v.filenode})
		}
	}
	s.platformArtifactKeyringMu.RLock()
	defer s.platformArtifactKeyringMu.RUnlock()
	if s.platformArtifactKeyringRevision != p.keyringRevision || !time.Now().Before(p.expires) {
		return plan, ErrConflict
	}
	private, err := taskPreparationSigningKey(s.platformArtifactKeyring)
	if err != nil {
		return plan, err
	}
	key := base64.StdEncoding.EncodeToString(private.Public().(ed25519.PublicKey))
	raw, err := taskPreparationTicketPayload(t)
	if err != nil || len(raw) > taskPreparationTicketMaxBytes-128 {
		return plan, ErrConflict
	}
	t.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(private, raw))
	if err := t.Validate(key, time.Now()); err != nil {
		return plan, err
	}
	// Recheck every physical witness including the task owner/status row.
	// No pin or work is created by issuing a plan.
	if err := checkTaskPreparationTicket(ctx, s.db, t, false, nil); err != nil {
		return plan, err
	}
	return NodeTaskInputPreparationPlan{Ticket: t, VerificationKey: key}, nil
}

func (t NodeTaskInputPreparationTicket) Validate(verificationKey string, now time.Time) error {
	if t.Version != 1 || t.Contract.Validate(true) != nil || !inputDigestPattern.MatchString(t.BackendHash) ||
		!inputDigestPattern.MatchString("sha256:"+t.ClosureSHA256) || t.Epoch < 0 || t.Horizon < 1 ||
		t.IssuedAt.IsZero() || t.IssuedAt.After(now) || !t.ExpiresAt.After(now) || t.ExpiresAt.Sub(t.IssuedAt) != 10*time.Minute ||
		len(t.Inputs) > t.Contract.MaxInputs || len(t.Tuples) == 0 || len(t.Tuples) > 32768 {
		return ErrInvalidInput
	}
	var rawBytes int64
	for i, m := range t.Inputs {
		if validateCompilerInputManifest(m.manifest()) != nil || i > 0 && t.Inputs[i-1].Digest >= m.Digest {
			return ErrInvalidInput
		}
		rawBytes += m.RawBytes
		if rawBytes > t.Contract.MaxRawBytes {
			return ErrConflict
		}
	}
	taskWitness := false
	for i, v := range t.Tuples {
		switch v.Table {
		case "fugue_platform_artifacts", "fugue_platform_artifact_releases", "fugue_platform_expected_consumer_sets", "fugue_node_update_tasks":
		case "fugue_platform_lkg_snapshots":
			if v.Key != compilerInputTaskConfigurationSelectorKey() {
				return ErrInvalidInput
			}
		default:
			return ErrInvalidInput
		}
		if v.Table == "fugue_node_update_tasks" && v.Key == t.Contract.TaskID {
			taskWitness = true
		}
		if v.Key == "" || len(v.Key) > 512 || !taskPreparationXmin.MatchString(v.Xmin) || !taskPreparationCTID.MatchString(v.CTID) || v.Relation < 1 || v.Filenode < 1 ||
			i > 0 && t.Tuples[i-1].Table+"/"+t.Tuples[i-1].Key >= v.Table+"/"+v.Key {
			return ErrInvalidInput
		}
	}
	if !taskWitness {
		return ErrInvalidInput
	}
	key, e1 := base64.StdEncoding.DecodeString(verificationKey)
	signature, e2 := base64.StdEncoding.DecodeString(t.Signature)
	raw, err := taskPreparationTicketPayload(t)
	if err != nil || e1 != nil || e2 != nil || len(key) != ed25519.PublicKeySize || len(signature) != ed25519.SignatureSize || len(raw) > taskPreparationTicketMaxBytes-128 || !ed25519.Verify(ed25519.PublicKey(key), raw, signature) {
		return ErrConflict
	}
	return nil
}

// Exact current state checks in one short transaction, never around GET/body
// I/O. A stale task/artifact/epoch refuses work; no sampled prefix is accepted.
func checkTaskPreparationTicket(ctx context.Context, db *sql.DB, t NodeTaskInputPreparationTicket, restored bool, pins []CompilerInputLocalPinReport) error {
	return t.work().check(ctx, db, restored, pins, t.checkTarget)
}

func (t NodeTaskInputPreparationTicket) checkTarget(ctx context.Context, tx *sql.Tx) error {
	var valid bool
	if err := tx.QueryRowContext(ctx, `SELECT status IN ('pending','running') AND node_updater_id=$2 FROM fugue_node_update_tasks WHERE id=$1`, t.Contract.TaskID, t.Contract.UpdaterID).Scan(&valid); err != nil || !valid {
		return errors.Join(err, ErrConflict)
	}
	return nil
}

func (t NodeTaskInputPreparationTicket) work() compilerInputPreparationWork {
	c := t.Contract
	return compilerInputPreparationWork{operation: c.OperationKey, backend: t.BackendHash, epoch: t.Epoch, horizon: t.Horizon, expires: t.ExpiresAt, inputs: t.Inputs, tuples: t.Tuples,
		limits: CompilerInputPreparationBudget{c.MaxInputs, c.MaxRawBytes, c.MaxStoredBytes, c.MaxGetBytes, c.MaxGetRequests, c.MaxSeconds}}
}

func taskPreparationPinKey(operation, digest string) string {
	sum := sha256.Sum256([]byte(digest))
	return operation + "/" + hex.EncodeToString(sum[:])
}

// These entry points use the existing trusted API process environment only.
// The formal runner must not copy that environment into an isolated worker.
func RunNodeTaskInputPreparationPlan(ctx context.Context, databaseURL string, keys bundleauth.Keyring, c NodeTaskInputPreparationContract, backend string) (NodeTaskInputPreparationPlan, error) {
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return NodeTaskInputPreparationPlan{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, databaseURL: databaseURL}
	s.ConfigurePlatformArtifactSigning(keys)
	return s.planNodeTaskInputPreparation(ctx, c, backend)
}

func RunNodeTaskInputPreparationConfirmation(ctx context.Context, databaseURL string, keys bundleauth.Keyring, t NodeTaskInputPreparationTicket) (NodeTaskInputPreparationReport, error) {
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return NodeTaskInputPreparationReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, databaseURL: databaseURL}
	s.ConfigurePlatformArtifactSigning(keys)
	return s.confirmNodeTaskInputPreparation(ctx, t)
}

func (s *Store) confirmNodeTaskInputPreparation(ctx context.Context, t NodeTaskInputPreparationTicket) (NodeTaskInputPreparationReport, error) {
	report := NodeTaskInputPreparationReport{Action: "task-input-confirm", TaskID: t.Contract.TaskID, PlanSHA256: t.Contract.ExpectedPlanSHA256, Pins: []CompilerInputLocalPinReport{}}
	s.platformArtifactKeyringMu.RLock()
	private, err := taskPreparationSigningKey(s.platformArtifactKeyring)
	keyringRevision := s.platformArtifactKeyringRevision
	s.platformArtifactKeyringMu.RUnlock()
	if err != nil || t.Validate(base64.StdEncoding.EncodeToString(private.Public().(ed25519.PublicKey)), time.Now()) != nil {
		return report, ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ctx, cancelExpiry := context.WithDeadline(ctx, t.ExpiresAt)
	defer cancelExpiry()
	p, pins, err := s.prepareNodeTaskInputAdmission(ctx, t.Contract.TaskID)
	if err != nil || p.keyringRevision != keyringRevision || p.sourceEpoch != t.Epoch || taskPreparationClosureHash(p) != t.ClosureSHA256 || len(p.inputs) != len(t.Inputs) {
		return report, errors.Join(err, ErrConflict)
	}
	for i, m := range t.Inputs {
		if p.inputs[i] != m.Digest {
			return report, ErrConflict
		}
		// Select the exact operation's pin, not an unrelated verified one.
		var pin CompilerInputLocalPinReport
		pin.Key, pin.Digest = taskPreparationPinKey(t.Contract.OperationKey, m.Digest), m.Digest
		if err := s.db.QueryRowContext(ctx, `SELECT token,state,raw_bytes FROM fugue_platform_input_local_pins WHERE pin_key=$1 AND content_hash=$2`, pin.Key, pin.Digest).Scan(&pin.Token, &pin.State, &pin.RawBytes); err != nil {
			return report, err
		}
		pin.Verified = pin.State == "verified"
		report.Pins = append(report.Pins, pin)
		pins[i] = compilerInputPinBinding{Key: pin.Key, Token: pin.Token, Digest: pin.Digest}
		report.RawBytes += m.RawBytes
	}
	if err := checkTaskPreparationTicket(ctx, s.db, t, true, report.Pins); err != nil {
		return report, err
	}
	if err := s.withPreparedCompilerInputActivation(ctx, p, pins, func(context.Context, *sql.Tx) error { return nil }); err != nil {
		return report, err
	}
	report.InputCount, report.Prepared = len(t.Inputs), true
	return report, nil
}

// No keyring argument and no managed configuration decryption in this worker.
// Its explicit reader configuration is supplied by the trusted CI coordinator.
func RunNodeTaskInputRestore(ctx context.Context, databaseURL string, t NodeTaskInputPreparationTicket, verificationKey string, reader inputarchive.S3Config) (NodeTaskInputPreparationReport, error) {
	if t.Validate(verificationKey, time.Now()) != nil || reader.Validate() != nil || reader.SessionToken != "" {
		return NodeTaskInputPreparationReport{}, ErrInvalidInput
	}
	backend, err := reader.Fingerprint()
	if err != nil || backend != t.BackendHash {
		return NodeTaskInputPreparationReport{}, ErrConflict
	}
	// Treat unexpected platform/writer secrets as an isolation failure even
	// though the implementation never reads them to obtain credentials.
	if err := taskPreparationWorkerIsolation(); err != nil {
		return NodeTaskInputPreparationReport{}, err
	}
	objects, err := inputarchive.NewSingleAttemptS3(reader)
	if err != nil {
		return NodeTaskInputPreparationReport{}, err
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return NodeTaskInputPreparationReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return restoreNodeTaskInputs(ctx, db, t, verificationKey, objects)
}

func taskPreparationWorkerIsolation() error {
	for _, name := range []string{
		"FUGUE_BUNDLE_SIGNING_KEY", "FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY",
		"FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "FUGUE_CREDENTIAL_ENCRYPTION_KEY",
		"FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY", "FUGUE_AGENT_EDGE_SIGNING_KEY_FILE",
		"FUGUE_COMPILER_INPUT_ARCHIVE_ACCESS_KEY_ID", "FUGUE_COMPILER_INPUT_ARCHIVE_SECRET_ACCESS_KEY", "FUGUE_COMPILER_INPUT_ARCHIVE_SESSION_TOKEN",
		"FUGUE_COMPILER_INPUT_ARCHIVE_WRITER_ACCESS_KEY_ID", "FUGUE_COMPILER_INPUT_ARCHIVE_WRITER_SECRET_ACCESS_KEY",
	} {
		if os.Getenv(name) != "" {
			return ErrConflict
		}
	}
	return nil
}

// Isolated workers never call the managed-config decryption path or fall back
// to legacy writer credentials. The coordinator supplies an explicit reader.
func NodeTaskPreparationReaderFromEnv() (inputarchive.S3Config, error) {
	if err := taskPreparationWorkerIsolation(); err != nil {
		return inputarchive.S3Config{}, err
	}
	c := inputarchive.S3Config{
		Endpoint: os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_ENDPOINT"), AccountID: os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_ACCOUNT_ID"),
		Region: os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_REGION"), Bucket: os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_BUCKET"), Prefix: os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_PREFIX"),
		AccessKeyID: os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_SECRET_ACCESS_KEY"),
	}
	if os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_SESSION_TOKEN") != "" || c.Validate() != nil {
		return inputarchive.S3Config{}, ErrConflict
	}
	return c, nil
}

func restoreNodeTaskInputs(ctx context.Context, db *sql.DB, t NodeTaskInputPreparationTicket, key string, objects inputarchive.Objects) (report NodeTaskInputPreparationReport, err error) {
	c := t.Contract
	report = NodeTaskInputPreparationReport{Action: "task-input-restore", TaskID: c.TaskID, PlanSHA256: c.ExpectedPlanSHA256, InputCount: len(t.Inputs), Pins: []CompilerInputLocalPinReport{}}
	if err := t.Validate(key, time.Now()); err != nil {
		return report, err
	}
	receipt := compilerInputPreparationReceipt{Pins: []CompilerInputLocalPinReport{}}
	err = t.work().restore(ctx, db, objects, &receipt, t.checkTarget)
	report.RawBytes, report.StoredBytes, report.Pins, report.Usage = receipt.RawBytes, receipt.StoredBytes, receipt.Pins, receipt.Usage
	// Prepared deliberately remains false: only fresh trusted closure/signature
	// confirmation can set it. These are data restoration/pin receipts only.
	return report, err
}
