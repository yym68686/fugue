package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

// Preparation never registers a consumer, accepts a heartbeat or signs an
// audit. Original business identity/cursor/lane checks remain mandatory. The
// full immutable request is bound, including every reported retained generation.
type ConsumerInputPreparationContract struct {
	Version            int                                                `json:"version"`
	Mode               string                                             `json:"mode"`
	ExpectedSetID      string                                             `json:"expected_set_id"`
	Registration       *model.PlatformExpectedConsumerSet                 `json:"registration,omitempty"`
	Heartbeat          *platformcontrol.PlatformConsumerHeartbeatEnvelope `json:"heartbeat,omitempty"`
	OperationKey       string                                             `json:"operation_key"`
	ExpectedPlanSHA256 string                                             `json:"expected_plan_sha256"`
	CompilerInputPreparationBudget
}

func (c ConsumerInputPreparationContract) Validate(apply bool) error {
	if c.Version != 1 || c.CompilerInputPreparationBudget.validate() != nil ||
		!compilerInputPinKeyPattern.MatchString(c.OperationKey) || len(c.OperationKey) > 48 ||
		c.ExpectedSetID == "" || strings.TrimSpace(c.ExpectedSetID) != c.ExpectedSetID || len(c.ExpectedSetID) > 512 ||
		apply && !inputDigestPattern.MatchString("sha256:"+c.ExpectedPlanSHA256) || !apply && c.ExpectedPlanSHA256 != "" {
		return ErrInvalidInput
	}
	raw, err := json.Marshal(c)
	if err != nil || len(raw) > 128<<10 {
		return ErrInvalidInput
	}
	switch c.Mode {
	case "register":
		if c.Registration == nil || c.Heartbeat != nil || c.Registration.ID != c.ExpectedSetID || c.Registration.CreatedAt.IsZero() || c.Registration.UpdatedAt.IsZero() {
			return ErrInvalidInput
		}
		normal, err := normalizePlatformExpectedConsumerSetForStore(*c.Registration)
		before, _ := json.Marshal(c.Registration)
		after, _ := json.Marshal(normal)
		if err != nil || !bytes.Equal(before, after) {
			return ErrInvalidInput
		}
	case "trusted", "shadow":
		if c.Registration != nil || c.Heartbeat == nil || c.Heartbeat.ExpectedConsumerSetID != "" && c.Heartbeat.ExpectedConsumerSetID != c.ExpectedSetID || c.Mode == "shadow" && c.Heartbeat.ExpectedConsumerSetID == "" {
			return ErrInvalidInput
		}
		if _, _, err := cloneCompilerInputConsumerHeartbeat(*c.Heartbeat); err != nil {
			return err
		}
	default:
		return ErrInvalidInput
	}
	return nil
}

type ConsumerInputPreparationTicket struct {
	Version       int                              `json:"version"`
	Contract      ConsumerInputPreparationContract `json:"contract"`
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

type ConsumerInputPreparationPlan struct {
	Ticket          ConsumerInputPreparationTicket `json:"ticket"`
	VerificationKey string                         `json:"verification_key"`
}

type ConsumerInputPreparationReport struct {
	Action               string                        `json:"action"`
	Mode                 string                        `json:"mode"`
	ExpectedSetID        string                        `json:"expected_set_id"`
	PlanSHA256           string                        `json:"plan_sha256"`
	InputCount           int                           `json:"input_count"`
	RawBytes             int64                         `json:"raw_bytes"`
	StoredBytes          int64                         `json:"stored_bytes"`
	Pins                 []CompilerInputLocalPinReport `json:"pins"`
	Usage                inputarchive.CopyUsage        `json:"usage"`
	Prepared             bool                          `json:"prepared"`
	ConsumerActivated    bool                          `json:"consumer_activated"`
	RetirementAuthorized bool                          `json:"retirement_authorized"`
}

func consumerPreparationKey(keys bundleauth.Keyring) (ed25519.PrivateKey, error) {
	return inputPreparationSigningKey(keys, "fugue.consumer-input-preparation.ed25519.v1")
}

func consumerPreparationPayload(t ConsumerInputPreparationTicket) ([]byte, error) {
	t.Signature = ""
	return json.Marshal(t)
}

func (t ConsumerInputPreparationTicket) work() compilerInputPreparationWork {
	return compilerInputPreparationWork{operation: t.Contract.OperationKey, backend: t.BackendHash, epoch: t.Epoch, horizon: t.Horizon, expires: t.ExpiresAt, inputs: t.Inputs, tuples: t.Tuples, limits: t.Contract.CompilerInputPreparationBudget}
}

func (t ConsumerInputPreparationTicket) Validate(key string, now time.Time) error {
	if t.Version != 1 || t.Contract.Validate(true) != nil || !inputDigestPattern.MatchString(t.BackendHash) || !inputDigestPattern.MatchString("sha256:"+t.ClosureSHA256) ||
		t.Epoch < 0 || t.Horizon < 1 || t.IssuedAt.IsZero() || t.IssuedAt.After(now) || !now.Before(t.ExpiresAt) || t.ExpiresAt.Sub(t.IssuedAt) != 10*time.Minute ||
		len(t.Inputs) > t.Contract.MaxInputs || len(t.Tuples) == 0 || len(t.Tuples) > 32768 {
		return ErrInvalidInput
	}
	var total int64
	for i, m := range t.Inputs {
		if validateCompilerInputManifest(m.manifest()) != nil || i > 0 && t.Inputs[i-1].Digest >= m.Digest {
			return ErrInvalidInput
		}
		total += m.RawBytes
		if total > t.Contract.MaxRawBytes {
			return ErrConflict
		}
	}
	witness := false
	for i, v := range t.Tuples {
		switch v.Table {
		case "fugue_platform_artifacts", "fugue_platform_artifact_releases", "fugue_platform_expected_consumer_sets", "fugue_node_update_tasks":
			if len(v.Key) == 0 || len(v.Key) > 512 || v.Xmin == "0" || v.CTID == "(0,0)" {
				return ErrInvalidInput
			}
		case "fugue_platform_lkg_snapshots":
			if _, _, err := compilerInputLKGSelector(v.Key); err != nil || (v.Xmin == "0") != (v.CTID == "(0,0)") {
				return ErrInvalidInput
			}
		default:
			return ErrInvalidInput
		}
		witness = witness || t.Contract.Mode != "register" && v.Table == "fugue_platform_expected_consumer_sets" && v.Key == t.Contract.ExpectedSetID ||
			t.Contract.Mode == "register" && v.Table == "fugue_platform_artifacts" && v.Key == t.Contract.Registration.ReleaseSetID
		if !taskPreparationXmin.MatchString(v.Xmin) || !taskPreparationCTID.MatchString(v.CTID) || v.Relation < 1 || v.Filenode < 1 || i > 0 && t.Tuples[i-1].Table+"/"+t.Tuples[i-1].Key >= v.Table+"/"+v.Key {
			return ErrInvalidInput
		}
	}
	public, e1 := base64.StdEncoding.DecodeString(key)
	sig, e2 := base64.StdEncoding.DecodeString(t.Signature)
	raw, err := consumerPreparationPayload(t)
	if !witness || err != nil || e1 != nil || e2 != nil || len(public) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize || len(raw) > taskPreparationTicketMaxBytes-128 || !ed25519.Verify(public, raw, sig) {
		return ErrConflict
	}
	return nil
}

func (s *Store) consumerPreparationProof(ctx context.Context, c ConsumerInputPreparationContract) (*compilerInputActivationProof, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return s.prepareCompilerInputActivationIntent(ctx, nil, nil, nil, &compilerInputConsumerIntent{prospective: c.Registration, expectedID: c.ExpectedSetID, heartbeat: c.Heartbeat, bytes: len(raw)})
}

func consumerPreparationPlanHash(c ConsumerInputPreparationContract, p *compilerInputActivationProof) string {
	c.ExpectedPlanSHA256 = ""
	raw, _ := json.Marshal(struct {
		Contract ConsumerInputPreparationContract
		Proof    string
	}{c, nodeTaskPreparationPlan(p)})
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func (s *Store) planConsumerInputPreparation(ctx context.Context, c ConsumerInputPreparationContract, backend string) (ConsumerInputPreparationPlan, error) {
	var plan ConsumerInputPreparationPlan
	if c.Validate(false) != nil || !inputDigestPattern.MatchString(backend) {
		return plan, ErrInvalidInput
	}
	// Detach caller-owned slices and maps before deriving or signing any intent.
	raw, _ := json.Marshal(c)
	if err := json.Unmarshal(raw, &c); err != nil {
		return plan, err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	p, err := s.consumerPreparationProof(ctx, c)
	if err != nil {
		return plan, err
	}
	c.ExpectedPlanSHA256 = consumerPreparationPlanHash(c, p)
	t := ConsumerInputPreparationTicket{Version: 1, Contract: c, BackendHash: backend, ClosureSHA256: taskPreparationClosureHash(p), Epoch: p.sourceEpoch, Horizon: p.horizon, IssuedAt: time.Now().UTC(), Inputs: []taskPreparationManifest{}, Tuples: []taskPreparationTuple{}}
	t.ExpiresAt = t.IssuedAt.Add(10 * time.Minute)
	for _, d := range p.inputs {
		m := p.inputManifests[d]
		t.Inputs = append(t.Inputs, taskPreparationManifest{m.Digest, m.Format, m.Location, m.Size, m.ChunkSize, m.Count, m.Created})
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
	private, err := consumerPreparationKey(s.platformArtifactKeyring)
	if err != nil {
		return plan, err
	}
	key := base64.StdEncoding.EncodeToString(private.Public().(ed25519.PublicKey))
	raw, err = consumerPreparationPayload(t)
	if err != nil || len(raw) > taskPreparationTicketMaxBytes-128 {
		return plan, ErrConflict
	}
	t.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(private, raw))
	if err := t.Validate(key, time.Now()); err != nil {
		return plan, err
	}
	if err := t.work().check(ctx, s.db, false, nil, nil); err != nil {
		return plan, err
	}
	return ConsumerInputPreparationPlan{t, key}, nil
}

func consumerPreparationReport(t ConsumerInputPreparationTicket, action string) ConsumerInputPreparationReport {
	return ConsumerInputPreparationReport{Action: action, Mode: t.Contract.Mode, ExpectedSetID: t.Contract.ExpectedSetID, PlanSHA256: t.Contract.ExpectedPlanSHA256, InputCount: len(t.Inputs), Pins: []CompilerInputLocalPinReport{}}
}

func restoreConsumerInputs(ctx context.Context, db *sql.DB, t ConsumerInputPreparationTicket, key string, objects inputarchive.Objects) (ConsumerInputPreparationReport, error) {
	r := consumerPreparationReport(t, "consumer-input-restore")
	if err := t.Validate(key, time.Now()); err != nil {
		return r, err
	}
	w := compilerInputPreparationReceipt{Pins: []CompilerInputLocalPinReport{}}
	err := t.work().restore(ctx, db, objects, &w, nil)
	r.RawBytes, r.StoredBytes, r.Pins, r.Usage = w.RawBytes, w.StoredBytes, w.Pins, w.Usage
	return r, err
}

func (s *Store) confirmConsumerInputPreparation(ctx context.Context, t ConsumerInputPreparationTicket) (ConsumerInputPreparationReport, error) {
	r := consumerPreparationReport(t, "consumer-input-confirm")
	s.platformArtifactKeyringMu.RLock()
	private, err := consumerPreparationKey(s.platformArtifactKeyring)
	revision := s.platformArtifactKeyringRevision
	s.platformArtifactKeyringMu.RUnlock()
	if err != nil || t.Validate(base64.StdEncoding.EncodeToString(private.Public().(ed25519.PublicKey)), time.Now()) != nil {
		return r, ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ctx, expiry := context.WithDeadline(ctx, t.ExpiresAt)
	defer expiry()
	p, err := s.consumerPreparationProof(ctx, t.Contract)
	if err != nil || p.keyringRevision != revision || p.sourceEpoch != t.Epoch || taskPreparationClosureHash(p) != t.ClosureSHA256 || len(p.inputs) != len(t.Inputs) {
		return r, errors.Join(err, ErrConflict)
	}
	pins := make([]compilerInputPinBinding, 0, len(t.Inputs))
	for i, m := range t.Inputs {
		if p.inputs[i] != m.Digest {
			return r, ErrConflict
		}
		pin := CompilerInputLocalPinReport{Key: taskPreparationPinKey(t.Contract.OperationKey, m.Digest), Digest: m.Digest}
		if err := s.db.QueryRowContext(ctx, `SELECT token,state,raw_bytes FROM fugue_platform_input_local_pins WHERE pin_key=$1 AND content_hash=$2`, pin.Key, pin.Digest).Scan(&pin.Token, &pin.State, &pin.RawBytes); err != nil {
			return r, err
		}
		pin.Verified = pin.State == "verified"
		r.Pins = append(r.Pins, pin)
		r.RawBytes += m.RawBytes
		pins = append(pins, compilerInputPinBinding{Key: pin.Key, Token: pin.Token, Digest: pin.Digest})
	}
	if err := t.work().check(ctx, s.db, true, r.Pins, nil); err != nil {
		return r, err
	}
	if err := s.withPreparedCompilerInputActivation(ctx, p, pins, func(context.Context, *sql.Tx) error { return nil }); err != nil {
		return r, err
	}
	r.Prepared = true
	return r, nil
}

func RunConsumerInputPreparationPlan(ctx context.Context, url string, keys bundleauth.Keyring, c ConsumerInputPreparationContract, backend string) (ConsumerInputPreparationPlan, error) {
	db, err := openPostgresDatabase(url)
	if err != nil {
		return ConsumerInputPreparationPlan{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, databaseURL: url}
	s.ConfigurePlatformArtifactSigning(keys)
	return s.planConsumerInputPreparation(ctx, c, backend)
}

func RunConsumerInputPreparationConfirmation(ctx context.Context, url string, keys bundleauth.Keyring, t ConsumerInputPreparationTicket) (ConsumerInputPreparationReport, error) {
	db, err := openPostgresDatabase(url)
	if err != nil {
		return ConsumerInputPreparationReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, databaseURL: url}
	s.ConfigurePlatformArtifactSigning(keys)
	return s.confirmConsumerInputPreparation(ctx, t)
}

func RunConsumerInputRestore(ctx context.Context, url string, t ConsumerInputPreparationTicket, key string, reader inputarchive.S3Config) (ConsumerInputPreparationReport, error) {
	if t.Validate(key, time.Now()) != nil || reader.Validate() != nil || reader.SessionToken != "" || taskPreparationWorkerIsolation() != nil {
		return ConsumerInputPreparationReport{}, ErrInvalidInput
	}
	backend, err := reader.Fingerprint()
	if err != nil || backend != t.BackendHash {
		return ConsumerInputPreparationReport{}, ErrConflict
	}
	objects, err := inputarchive.NewSingleAttemptS3(reader)
	if err != nil {
		return ConsumerInputPreparationReport{}, err
	}
	db, err := openPostgresDatabase(url)
	if err != nil {
		return ConsumerInputPreparationReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return restoreConsumerInputs(ctx, db, t, key, objects)
}
