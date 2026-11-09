package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

const compilerInputExternalRegistryLimit = 1024
const compilerInputExternalOwnerLimit = 32
const compilerInputExternalReferenceLimit = 32768

type compilerInputPendingRootIntent struct {
	registration      inputarchive.PendingRootRegistration
	raw               string
	hash              string
	admissionRevision uint64
	checkAdmission    func(context.Context) error
}

// preparePendingCompilerInputExternalRoot authenticates the COMPLETE native
// dependency closure under current platform trust in one bounded RR snapshot.
// It does not authenticate the external source/custody or grant activation.
// Cold original bytes may remain unavailable: registration adds conservative
// retention before restoration, just as prospective pending task creation does.
func (s *Store) preparePendingCompilerInputExternalRoot(ctx context.Context, registration inputarchive.PendingRootRegistration) (*compilerInputActivationProof, error) {
	raw, hash, err := registration.Canonical()
	if err != nil {
		return nil, ErrInvalidInput
	}
	var copy inputarchive.PendingRootRegistration
	if err := json.Unmarshal(raw, &copy); err != nil {
		return nil, err
	}
	roots := make([]inputarchive.HotRoot, len(copy.References))
	for i, ref := range copy.References {
		roots[i] = inputarchive.HotRoot{Source: "pending-external-root", ID: hash + "/" + strconv.Itoa(i), Reference: ref}
	}
	proof, err := s.prepareCompilerInputActivation(ctx, roots)
	if err != nil {
		return nil, err
	}
	proof.pendingRoot = &compilerInputPendingRootIntent{registration: copy, raw: string(raw), hash: hash}
	return proof, nil
}

// ConfigurePendingRootAdmission installs trusted process providers. It does
// not install the registry or enable any producer/route/retirement policy.
// Reconfiguration invalidates every prepared witness, including idempotent
// requests: an old receipt never bypasses current authorization or admission.
func (s *Store) ConfigurePendingRootAdmission(source inputarchive.PendingRootSourceVerifier, admission inputarchive.PendingRootAdmission) {
	if s == nil {
		return
	}
	s.pendingRootAdmissionMu.Lock()
	defer s.pendingRootAdmissionMu.Unlock()
	s.pendingRootSourceVerifier, s.pendingRootAdmission = source, admission
	s.pendingRootAdmissionRevision++
}

// RetainPendingCompilerInputExternalRoot is an internal, retention-only store
// operation, not an HTTP surface. Missing source or allocation providers fail
// before DB access. The complete request is copied before any provider call.
// No production caller or ordinary migration enables it implicitly. A crash or
// lost reply after commit leaves the exact root protected; retrying the SAME
// registration is idempotent. Changing its source/ref set is never replacement.
func (s *Store) RetainPendingCompilerInputExternalRoot(ctx context.Context, request inputarchive.PendingRootRequest) (inputarchive.PendingRootReceipt, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if s == nil {
		return inputarchive.PendingRootReceipt{}, ErrConflict
	}
	raw, _, err := request.Canonical()
	if err != nil {
		return inputarchive.PendingRootReceipt{}, ErrInvalidInput
	}
	var isolated inputarchive.PendingRootRequest
	if err := json.Unmarshal(raw, &isolated); err != nil {
		return inputarchive.PendingRootReceipt{}, ErrInvalidInput
	}
	request = isolated // never reuse the caller's slices or RawMessage backing array
	s.pendingRootAdmissionMu.RLock()
	source, admission, revision := s.pendingRootSourceVerifier, s.pendingRootAdmission, s.pendingRootAdmissionRevision
	s.pendingRootAdmissionMu.RUnlock()
	if source == nil || admission == nil {
		return inputarchive.PendingRootReceipt{}, ErrConflict
	}
	witness, err := source.VerifyPendingRootSource(ctx, request)
	if err != nil || witness == nil {
		return inputarchive.PendingRootReceipt{}, ErrConflict
	}
	if err := witness.CheckPendingRootSource(ctx, request); err != nil {
		return inputarchive.PendingRootReceipt{}, err
	}
	reservation, err := admission.ReservePendingRoot(ctx, request)
	if err != nil || reservation == nil {
		return inputarchive.PendingRootReceipt{}, ErrConflict
	}
	outcome := inputarchive.PendingRootNotAttempted
	defer func() { reservation.RecordPendingRootOutcome(outcome) }()
	check := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := witness.CheckPendingRootSource(ctx, request); err != nil {
			return err
		}
		return reservation.CheckPendingRootReservation(ctx)
	}
	if err := check(ctx); err != nil {
		return inputarchive.PendingRootReceipt{}, err
	}
	proof, err := s.preparePendingCompilerInputExternalRoot(ctx, request.Registration)
	if err != nil {
		return inputarchive.PendingRootReceipt{}, err
	}
	proof.pendingRoot.admissionRevision, proof.pendingRoot.checkAdmission = revision, check
	// Any transaction error is conservatively ambiguous to the reservation
	// provider. A caller timeout is never permission to release retained costs.
	outcome = inputarchive.PendingRootCompletionNotConfirmed
	receipt, err := s.commitPendingCompilerInputExternalRoot(ctx, proof)
	if err == nil {
		outcome = inputarchive.PendingRootCommitted
	}
	return receipt, err
}

func (s *Store) commitPendingCompilerInputExternalRoot(ctx context.Context, proof *compilerInputActivationProof) (inputarchive.PendingRootReceipt, error) {
	var receipt inputarchive.PendingRootReceipt
	if proof == nil || proof.pendingRoot == nil || proof.newTask != nil || proof.publication != nil || proof.consumerKind != "" || proof.ordinary || proof.fullInventory {
		return receipt, ErrInvalidInput
	}
	intent := proof.pendingRoot
	if intent.checkAdmission == nil {
		return receipt, ErrConflict // private preparation is not authorization
	}
	raw, hash, err := intent.registration.Canonical()
	if err != nil || string(raw) != intent.raw || hash != intent.hash {
		return receipt, ErrInvalidInput
	}
	// Persist a native FK for EVERY concrete artifact/release/input descendant,
	// not only a claimed list of direct roots. Generation aliases resolve to
	// their exact artifact descendants; no synthetic verified leaf is inserted.
	refs := []inputarchive.HotReference{}
	for _, node := range proof.closure.Nodes {
		switch node.Identity.Kind {
		case "artifact", "release", "input":
			refs = append(refs, node.Identity)
		case "generation":
			continue
		default:
			return receipt, ErrConflict
		}
	}
	if len(refs) == 0 || len(refs) > compilerInputExternalReferenceLimit {
		return receipt, ErrConflict
	}
	err = s.consumePreparedCompilerInput(ctx, proof, nil, true, nil, func(ctx context.Context, tx *sql.Tx) error {
		if err := schemamigrate.VerifyCompilerInputExternalRoots(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue.compiler-input-external-roots.v1',0))`); err != nil {
			return err
		}
		var existingHash, existingRaw string
		err := tx.QueryRowContext(ctx, `SELECT registration_sha256,registration_json,registry_revision,source_epoch FROM fugue_platform_input_external_roots WHERE owner_key=$1 AND generation=$2 FOR SHARE`, intent.registration.Owner, intent.registration.Generation).
			Scan(&existingHash, &existingRaw, &receipt.RegistryRevision, &receipt.SourceEpoch)
		if err == nil {
			if existingHash != hash || existingRaw != intent.raw {
				return ErrConflict
			}
			if err := checkPendingCompilerInputExternalReferences(ctx, tx, hash, refs); err != nil {
				return err
			}
			receipt.RegistrationSHA256 = hash
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var total, owner, bindings int
		if err := tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE owner_key=$1) FROM fugue_platform_input_external_roots`, intent.registration.Owner).Scan(&total, &owner); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM fugue_platform_input_external_root_refs`).Scan(&bindings); err != nil {
			return err
		}
		if total >= compilerInputExternalRegistryLimit || owner >= compilerInputExternalOwnerLimit || bindings > compilerInputExternalReferenceLimit-len(refs) {
			return ErrConflict // refuse a new root; never evict an old protection
		}
		if err := tx.QueryRowContext(ctx, `INSERT INTO fugue_platform_input_external_roots(registration_sha256,owner_key,generation,source_digest,source_epoch,registration_json)
 VALUES($1,$2,$3,$4,$5,$6) RETURNING registry_revision`, hash, intent.registration.Owner, intent.registration.Generation, intent.registration.SourceDigest, proof.sourceEpoch, intent.raw).Scan(&receipt.RegistryRevision); err != nil {
			return err
		}
		for i, ref := range refs {
			var artifact, release, input any
			switch ref.Kind {
			case "artifact":
				artifact = ref.ID
			case "release":
				release = ref.ID
			case "input":
				input = ref.ID
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_external_root_refs(registration_sha256,ordinal,artifact_id,release_id,content_hash) VALUES($1,$2,$3,$4,$5)`, hash, i, artifact, release, input); err != nil {
				return err
			}
		}
		receipt.RegistrationSHA256, receipt.SourceEpoch = hash, proof.sourceEpoch
		return nil
	})
	if err != nil {
		return inputarchive.PendingRootReceipt{}, err // no uncommitted acknowledgement
	}
	return receipt, nil
}

func checkPendingCompilerInputExternalReferences(ctx context.Context, tx *sql.Tx, hash string, refs []inputarchive.HotReference) error {
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,artifact_id,release_id,content_hash FROM fugue_platform_input_external_root_refs WHERE registration_sha256=$1 ORDER BY ordinal LIMIT $2`, hash, compilerInputExternalReferenceLimit+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var ordinal int
		var artifact, release, input sql.NullString
		if err := rows.Scan(&ordinal, &artifact, &release, &input); err != nil {
			return err
		}
		if ordinal != i || i >= len(refs) || (artifact.Valid != (refs[i].Kind == "artifact")) ||
			(release.Valid != (refs[i].Kind == "release")) || (input.Valid != (refs[i].Kind == "input")) ||
			artifact.Valid && artifact.String != refs[i].ID || release.Valid && release.String != refs[i].ID || input.Valid && input.String != refs[i].ID {
			return ErrConflict
		}
		i++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if i != len(refs) {
		return ErrConflict
	}
	return nil
}

func compilerInputExternalRoots(ctx context.Context, tx *sql.Tx, limit int, observe func(inputarchive.HotRoot, compilerInputRootState) error) ([]inputarchive.HotRoot, int64, error) {
	var installed bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_external_roots') IS NOT NULL OR to_regclass('fugue_platform_input_external_root_refs') IS NOT NULL`).Scan(&installed); err != nil || !installed {
		return nil, 0, err
	}
	if err := schemamigrate.VerifyCompilerInputExternalRoots(ctx, tx); err != nil {
		return nil, 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT registration_sha256,registry_revision,owner_key,generation,source_digest,source_epoch,left(registration_json,16385) FROM fugue_platform_input_external_roots ORDER BY registry_revision LIMIT $1`, compilerInputExternalRegistryLimit+1)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	roots := []inputarchive.HotRoot{}
	owners := map[string]int{}
	var revision int64
	count := 0
	for rows.Next() {
		var hash, owner, generation, digest, raw string
		var next, epoch int64
		if err := rows.Scan(&hash, &next, &owner, &generation, &digest, &epoch, &raw); err != nil {
			return nil, 0, err
		}
		var registration inputarchive.PendingRootRegistration
		if json.Unmarshal([]byte(raw), &registration) != nil {
			return nil, 0, ErrConflict
		}
		canonical, actual, err := registration.Canonical()
		owners[owner]++
		count++
		if err != nil || actual != hash || string(canonical) != raw || registration.Owner != owner || registration.Generation != generation || registration.SourceDigest != digest ||
			next <= revision || epoch < 0 || count > compilerInputExternalRegistryLimit || owners[owner] > compilerInputExternalOwnerLimit || len(registration.References) > limit-len(roots) {
			return nil, 0, ErrConflict
		}
		revision = next
		for i, ref := range registration.References {
			root := inputarchive.HotRoot{Source: "pending-external-root", ID: hash + "/" + strconv.Itoa(i), Reference: ref}
			roots = append(roots, root)
			if observe != nil {
				if err := observe(root, compilerInputRootState{State: "pending", Generation: generation}); err != nil {
					return nil, 0, err
				}
			}
		}
	}
	return roots, revision, rows.Err()
}

// Final enforcement takes SHARE on both tables BEFORE re-reading the census.
// This also catches a root appended after preparation that happens to point to
// already-retained native inputs. A record's identity must not disappear just
// because its input set overlaps another root.
func lockCompilerInputExternalRootRegistry(ctx context.Context, tx *sql.Tx) error {
	var installed bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_external_roots') IS NOT NULL OR to_regclass('fugue_platform_input_external_root_refs') IS NOT NULL`).Scan(&installed); err != nil || !installed {
		return err
	}
	if _, err := tx.ExecContext(ctx, `LOCK TABLE fugue_platform_input_external_roots,fugue_platform_input_external_root_refs IN SHARE MODE`); err != nil {
		return err
	}
	return schemamigrate.VerifyCompilerInputExternalRoots(ctx, tx)
}

func pendingCompilerInputProofMatches(proof *compilerInputActivationProof) bool {
	if proof == nil || proof.pendingRoot == nil {
		return false
	}
	roots := make([]inputarchive.HotRoot, len(proof.pendingRoot.registration.References))
	for i, ref := range proof.pendingRoot.registration.References {
		roots[i] = inputarchive.HotRoot{Source: "pending-external-root", ID: proof.pendingRoot.hash + "/" + strconv.Itoa(i), Reference: ref}
	}
	return reflect.DeepEqual(canonicalCompilerInputHistoryRoots(roots), canonicalCompilerInputHistoryRoots(proof.closure.Roots))
}
