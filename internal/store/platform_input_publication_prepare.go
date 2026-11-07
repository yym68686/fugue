package store

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

// Explicit cold preparation only. Original publication/rollback/verification
// authority, producer guards and lane/convergence checks remain in the business
// transaction. This contract cannot authorize those mutations or retirement.
// Optional producer intent uses the original producer method arguments and
// their shared server-owned request/guard builders, never a raw guard.
type PublicationInputPreparationContract struct {
	Version            int                                  `json:"version"`
	Mode               string                               `json:"mode"`
	ArtifactID         string                               `json:"artifact_id"`
	RollbackGeneration string                               `json:"rollback_generation"`
	ReleaseID          string                               `json:"release_id"`
	Request            model.PlatformArtifactReleaseRequest `json:"request"`
	OperationKey       string                               `json:"operation_key"`
	ExpectedPlanSHA256 string                               `json:"expected_plan_sha256"`
	Producer           *ProducerInputPreparationIntent      `json:"producer,omitempty"`
	CompilerInputPreparationBudget
}

func (c PublicationInputPreparationContract) Validate(apply bool) error {
	if c.Version != 1 || c.CompilerInputPreparationBudget.validate() != nil ||
		!compilerInputPinKeyPattern.MatchString(c.OperationKey) || len(c.OperationKey) > 48 ||
		len(c.ArtifactID) > 512 || len(c.RollbackGeneration) > 512 || len(c.ReleaseID) > 512 ||
		apply && !inputDigestPattern.MatchString("sha256:"+c.ExpectedPlanSHA256) || !apply && c.ExpectedPlanSHA256 != "" {
		return ErrInvalidInput
	}
	raw, err := json.Marshal(c.Request)
	empty, _ := json.Marshal(model.PlatformArtifactReleaseRequest{})
	if err != nil || len(raw) > 65536 {
		return ErrInvalidInput
	}
	switch c.Mode {
	case "release":
		if c.ArtifactID == "" || c.RollbackGeneration != "" || c.ReleaseID != "" {
			return ErrInvalidInput
		}
	case "rollback":
		if c.ArtifactID == "" || c.RollbackGeneration == "" || c.ReleaseID != "" || string(raw) != string(empty) {
			return ErrInvalidInput
		}
	case "lkg":
		if c.ReleaseID == "" || c.ArtifactID != "" || c.RollbackGeneration != "" || string(raw) != string(empty) {
			return ErrInvalidInput
		}
	default:
		return ErrInvalidInput
	}
	if c.Producer != nil {
		if _, _, err := c.producerIntent(); err != nil {
			return err
		}
	}
	return nil
}

type PublicationInputPreparationTicket struct {
	Version       int                                 `json:"version"`
	Contract      PublicationInputPreparationContract `json:"contract"`
	BackendHash   string                              `json:"backend_hash"`
	ClosureSHA256 string                              `json:"closure_sha256"`
	Epoch         int64                               `json:"epoch"`
	Horizon       int64                               `json:"horizon"`
	IssuedAt      time.Time                           `json:"issued_at"`
	ExpiresAt     time.Time                           `json:"expires_at"`
	Inputs        []taskPreparationManifest           `json:"inputs"`
	Tuples        []taskPreparationTuple              `json:"tuples"`
	Signature     string                              `json:"signature"`
}

type PublicationInputPreparationPlan struct {
	Ticket          PublicationInputPreparationTicket `json:"ticket"`
	VerificationKey string                            `json:"verification_key"`
}

type PublicationInputPreparationReport struct {
	Action               string                        `json:"action"`
	Mode                 string                        `json:"mode"`
	ArtifactID           string                        `json:"artifact_id"`
	ReleaseID            string                        `json:"release_id"`
	PlanSHA256           string                        `json:"plan_sha256"`
	InputCount           int                           `json:"input_count"`
	RawBytes             int64                         `json:"raw_bytes"`
	StoredBytes          int64                         `json:"stored_bytes"`
	Pins                 []CompilerInputLocalPinReport `json:"pins"`
	Usage                inputarchive.CopyUsage        `json:"usage"`
	Prepared             bool                          `json:"prepared"`
	PublicationActivated bool                          `json:"publication_activated"`
	RetirementAuthorized bool                          `json:"retirement_authorized"`
}

func publicationPreparationPayload(t PublicationInputPreparationTicket) ([]byte, error) {
	t.Signature = ""
	return json.Marshal(t)
}

func publicationPreparationKey(keys bundleauth.Keyring) (ed25519.PrivateKey, error) {
	return inputPreparationSigningKey(keys, "fugue.publication-input-preparation.ed25519.v1")
}

func (t PublicationInputPreparationTicket) work() compilerInputPreparationWork {
	return compilerInputPreparationWork{operation: t.Contract.OperationKey, backend: t.BackendHash, epoch: t.Epoch, horizon: t.Horizon, expires: t.ExpiresAt, inputs: t.Inputs, tuples: t.Tuples, limits: t.Contract.CompilerInputPreparationBudget}
}

func (t PublicationInputPreparationTicket) Validate(key string, now time.Time) error {
	if t.Version != 1 || t.Contract.Validate(true) != nil || !inputDigestPattern.MatchString(t.BackendHash) ||
		!inputDigestPattern.MatchString("sha256:"+t.ClosureSHA256) || t.Epoch < 0 || t.Horizon < 1 ||
		t.IssuedAt.IsZero() || t.IssuedAt.After(now) || !now.Before(t.ExpiresAt) || t.ExpiresAt.Sub(t.IssuedAt) != 10*time.Minute ||
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
		case "fugue_platform_release_lanes":
			if t.Contract.Producer == nil || len(v.Key) == 0 || len(v.Key) > 2048 || (v.Xmin == "0") != (v.CTID == "(0,0)") {
				return ErrInvalidInput
			}
		default:
			return ErrInvalidInput
		}
		witness = witness || t.Contract.Mode == "lkg" && v.Table == "fugue_platform_artifact_releases" && v.Key == t.Contract.ReleaseID ||
			t.Contract.Mode != "lkg" && v.Table == "fugue_platform_artifacts" && v.Key == t.Contract.ArtifactID
		if !taskPreparationXmin.MatchString(v.Xmin) || !taskPreparationCTID.MatchString(v.CTID) || v.Relation < 1 || v.Filenode < 1 ||
			i > 0 && t.Tuples[i-1].Table+"/"+t.Tuples[i-1].Key >= v.Table+"/"+v.Key {
			return ErrInvalidInput
		}
	}
	public, e1 := base64.StdEncoding.DecodeString(key)
	sig, e2 := base64.StdEncoding.DecodeString(t.Signature)
	raw, err := publicationPreparationPayload(t)
	if !witness || err != nil || e1 != nil || e2 != nil || len(public) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize || len(raw) > taskPreparationTicketMaxBytes-128 || !ed25519.Verify(public, raw, sig) {
		return ErrConflict
	}
	return nil
}

func (s *Store) publicationPreparationProof(ctx context.Context, c PublicationInputPreparationContract) (*compilerInputActivationProof, error) {
	var guard *platformProducerReleaseGuard
	if c.Producer != nil {
		var err error
		_, guard, err = c.producerIntent()
		if err != nil {
			return nil, err
		}
	}
	if c.Mode == "lkg" && guard == nil {
		return s.prepareCompilerInputActivation(ctx, compilerInputLKGPromotionRoots(c.ReleaseID))
	}
	bindings, err := compilerInputPublicationRequestBindings(c.Request)
	if err != nil {
		return nil, err
	}
	intent := &compilerInputPublicationIntent{id: c.ArtifactID, rollbackGeneration: c.RollbackGeneration, requestBindings: bindings}
	if guard != nil {
		intent.hasGuard, intent.guard = true, *guard
		if c.Mode == "lkg" {
			intent.lkgReleaseID = c.ReleaseID
		}
	}
	p, err := s.prepareCompilerInputActivationIntent(ctx, nil, nil, intent)
	if err == nil && (p.publication == nil || c.Mode != "lkg" && p.publication.artifactID != c.ArtifactID) {
		return nil, ErrConflict // generation aliases are not a canonical intent
	}
	return p, err
}

func publicationPreparationPlanHash(c PublicationInputPreparationContract, p *compilerInputActivationProof) string {
	c.ExpectedPlanSHA256 = ""
	raw, _ := json.Marshal(struct {
		Contract PublicationInputPreparationContract
		Proof    string
	}{c, nodeTaskPreparationPlan(p)})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s *Store) planPublicationInputPreparation(ctx context.Context, c PublicationInputPreparationContract, backend string) (PublicationInputPreparationPlan, error) {
	var plan PublicationInputPreparationPlan
	if c.Validate(false) != nil || !inputDigestPattern.MatchString(backend) {
		return plan, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	p, err := s.publicationPreparationProof(ctx, c)
	if err != nil {
		return plan, err
	}
	c.ExpectedPlanSHA256 = publicationPreparationPlanHash(c, p)
	t := PublicationInputPreparationTicket{Version: 1, Contract: c, BackendHash: backend, ClosureSHA256: taskPreparationClosureHash(p), Epoch: p.sourceEpoch, Horizon: p.horizon,
		IssuedAt: time.Now().UTC(), Inputs: []taskPreparationManifest{}, Tuples: []taskPreparationTuple{}}
	t.ExpiresAt = t.IssuedAt.Add(10 * time.Minute)
	for _, digest := range p.inputs {
		m := p.inputManifests[digest]
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
	private, err := publicationPreparationKey(s.platformArtifactKeyring)
	if err != nil {
		return plan, err
	}
	key := base64.StdEncoding.EncodeToString(private.Public().(ed25519.PublicKey))
	raw, err := publicationPreparationPayload(t)
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
	return PublicationInputPreparationPlan{t, key}, nil
}

func publicationPreparationReport(t PublicationInputPreparationTicket, action string) PublicationInputPreparationReport {
	return PublicationInputPreparationReport{Action: action, Mode: t.Contract.Mode, ArtifactID: t.Contract.ArtifactID, ReleaseID: t.Contract.ReleaseID, PlanSHA256: t.Contract.ExpectedPlanSHA256, InputCount: len(t.Inputs), Pins: []CompilerInputLocalPinReport{}}
}

func restorePublicationInputs(ctx context.Context, db *sql.DB, t PublicationInputPreparationTicket, key string, objects inputarchive.Objects) (PublicationInputPreparationReport, error) {
	r := publicationPreparationReport(t, "publication-input-restore")
	if err := t.Validate(key, time.Now()); err != nil {
		return r, err
	}
	workReceipt := compilerInputPreparationReceipt{Pins: []CompilerInputLocalPinReport{}}
	err := t.work().restore(ctx, db, objects, &workReceipt, nil)
	r.RawBytes, r.StoredBytes, r.Pins, r.Usage = workReceipt.RawBytes, workReceipt.StoredBytes, workReceipt.Pins, workReceipt.Usage
	return r, err
}

func (s *Store) confirmPublicationInputPreparation(ctx context.Context, t PublicationInputPreparationTicket) (PublicationInputPreparationReport, error) {
	r := publicationPreparationReport(t, "publication-input-confirm")
	s.platformArtifactKeyringMu.RLock()
	private, err := publicationPreparationKey(s.platformArtifactKeyring)
	keyringRevision := s.platformArtifactKeyringRevision
	s.platformArtifactKeyringMu.RUnlock()
	if err != nil || t.Validate(base64.StdEncoding.EncodeToString(private.Public().(ed25519.PublicKey)), time.Now()) != nil {
		return r, ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ctx, cancelExpiry := context.WithDeadline(ctx, t.ExpiresAt)
	defer cancelExpiry()
	p, err := s.publicationPreparationProof(ctx, t.Contract)
	if err != nil || p.keyringRevision != keyringRevision || p.sourceEpoch != t.Epoch || taskPreparationClosureHash(p) != t.ClosureSHA256 || len(p.inputs) != len(t.Inputs) {
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

func RunPublicationInputPreparationPlan(ctx context.Context, databaseURL string, keys bundleauth.Keyring, c PublicationInputPreparationContract, backend string) (PublicationInputPreparationPlan, error) {
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return PublicationInputPreparationPlan{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, databaseURL: databaseURL}
	s.ConfigurePlatformArtifactSigning(keys)
	return s.planPublicationInputPreparation(ctx, c, backend)
}

func RunPublicationInputPreparationConfirmation(ctx context.Context, databaseURL string, keys bundleauth.Keyring, t PublicationInputPreparationTicket) (PublicationInputPreparationReport, error) {
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return PublicationInputPreparationReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, databaseURL: databaseURL}
	s.ConfigurePlatformArtifactSigning(keys)
	return s.confirmPublicationInputPreparation(ctx, t)
}

func RunPublicationInputRestore(ctx context.Context, databaseURL string, t PublicationInputPreparationTicket, key string, reader inputarchive.S3Config) (PublicationInputPreparationReport, error) {
	if t.Validate(key, time.Now()) != nil || reader.Validate() != nil || reader.SessionToken != "" || taskPreparationWorkerIsolation() != nil {
		return PublicationInputPreparationReport{}, ErrInvalidInput
	}
	backend, err := reader.Fingerprint()
	if err != nil || backend != t.BackendHash {
		return PublicationInputPreparationReport{}, ErrConflict
	}
	objects, err := inputarchive.NewSingleAttemptS3(reader)
	if err != nil {
		return PublicationInputPreparationReport{}, err
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return PublicationInputPreparationReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return restorePublicationInputs(ctx, db, t, key, objects)
}
