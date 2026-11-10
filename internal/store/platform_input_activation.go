package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/schemamigrate"
)

// This is an internal server-created proof, never an HTTP/operator assertion.
// It covers the COMPLETE closure of the supplied server-derived roots, not the
// complete retained inventory. Authority/TTL/convergence remain entry-point
// checks. Task claim, LKG promotion, release and rollback consume this proof;
// remaining activation entry points and retirement still need their own checks.
type compilerInputActivationProof struct {
	store           *Store
	database        *sql.DB
	keyringRevision uint64
	expires         time.Time
	horizon         int64
	sourceEpoch     int64
	closure         inputarchive.HotClosure
	tuples          []compilerInputActivationTuple
	inputs          []string
	inputManifests  map[string]inputManifest
	// A prospective task is immutable server-owned intent, not a row inserted
	// before admission. Its descendants and selector are checked in the same
	// RR snapshot; consume inserts this exact pending root atomically.
	newTask      *model.NodeUpdateTask
	pendingRoot  *compilerInputPendingRootIntent
	publication  *compilerInputPublicationIntent
	consumerKind string
	// Only consumer receipts and selected-input reads may share an LKG guard:
	// their apply callbacks never write an LKG. Publication, promotion, rollback
	// and task activation keep the writer guard, including absent selectors.
	lkgReadOnly bool
	// Capability-only ordinary requests retain original business checks. This
	// is never a complete dependency proof and cannot be used for preparation
	// confirmation, enforcement, fencing or retirement.
	capabilityOnly bool
	ordinary       bool
	fullInventory  bool
}

type compilerInputActivationTuple struct {
	table, key, xmin, ctid string
	relation, filenode     int64
}

// prepareCompilerInputActivation reads and verifies every signed artifact in
// one bounded RR snapshot, using the existing typed graph decoder/limits. No
// activation lock, network, R2 request, mutation or original input read occurs.
// Restore/full original SHA and pending->verified pins belong between prepare
// and consume; if they outlive this proof, prepare again rather than merging
// graphs from different snapshots. Missing/bad nodes refuse the whole proof.
func (s *Store) prepareCompilerInputActivation(ctx context.Context, roots []inputarchive.HotRoot) (*compilerInputActivationProof, error) {
	return s.prepareCompilerInputActivationWithTask(ctx, roots, nil)
}

func (s *Store) prepareCompilerInputActivationWithTask(ctx context.Context, roots []inputarchive.HotRoot, task *model.NodeUpdateTask) (*compilerInputActivationProof, error) {
	return s.prepareCompilerInputActivationIntent(ctx, roots, task, nil)
}

func (s *Store) prepareCompilerInputActivationIntent(ctx context.Context, roots []inputarchive.HotRoot, task *model.NodeUpdateTask, publication *compilerInputPublicationIntent, consumers ...*compilerInputConsumerIntent) (*compilerInputActivationProof, error) {
	if len(consumers) > 1 || len(consumers) == 1 && consumers[0] == nil {
		return nil, ErrInvalidInput
	}
	var consumer *compilerInputConsumerIntent
	if len(consumers) == 1 {
		consumer = consumers[0]
	}
	return s.prepareCompilerInputDependencyProof(ctx, roots, task, publication, consumer, nil)
}

// Reads use the same bounded signed graph and physical witnesses as activation,
// but consume them without installing or advancing any business state. Original
// request-specific ownership and serving checks remain the reader's authority.
func (s *Store) prepareCompilerInputDependencyProof(ctx context.Context, roots []inputarchive.HotRoot, task *model.NodeUpdateTask, publication *compilerInputPublicationIntent, consumer *compilerInputConsumerIntent, reads []PlatformInputReadSelection) (*compilerInputActivationProof, error) {
	return s.prepareCompilerInputProof(ctx, roots, task, publication, consumer, reads, false, false)
}

func (s *Store) prepareOrdinaryCompilerInputProof(ctx context.Context, roots []inputarchive.HotRoot, task *model.NodeUpdateTask, publication *compilerInputPublicationIntent, consumer *compilerInputConsumerIntent, reads []PlatformInputReadSelection) (*compilerInputActivationProof, error) {
	return s.prepareCompilerInputProof(ctx, roots, task, publication, consumer, reads, true, false)
}

func (s *Store) prepareCompilerInputProof(ctx context.Context, roots []inputarchive.HotRoot, task *model.NodeUpdateTask, publication *compilerInputPublicationIntent, consumer *compilerInputConsumerIntent, reads []PlatformInputReadSelection, ordinary, inventory bool, pending ...*compilerInputPendingRootIntent) (*compilerInputActivationProof, error) {
	if len(pending) > 1 || len(pending) == 1 && (pending[0] == nil || task != nil || publication != nil || consumer != nil || len(reads) != 0 || ordinary || inventory) {
		return nil, ErrInvalidInput
	}
	if s == nil || s.db == nil || !inventory && len(roots) == 0 && publication == nil && consumer == nil && len(reads) == 0 || len(roots) > compilerInputHotRootLimit || task != nil && publication != nil || consumer != nil && (task != nil || publication != nil || len(roots) != 0) || len(reads) != 0 && (task != nil || publication != nil || consumer != nil || len(roots) != 0) || inventory && (ordinary || task != nil || publication != nil || consumer != nil || len(reads) != 0 || len(roots) != 0) {
		return nil, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	proof := &compilerInputActivationProof{store: s, database: s.db, expires: time.Now().Add(time.Minute), inputManifests: map[string]inputManifest{}, ordinary: ordinary, fullInventory: inventory, lkgReadOnly: consumer != nil || len(reads) != 0}
	// Validate the current global LKG while its artifact is in the bounded
	// batch, then discard the body. Never cache the entire closure's bodies or
	// fetch them again outside the batch transport budget. If a mixed root set
	// resolved the selected artifact before discovering the task, reread just
	// that artifact inside the task's shared batch budget.
	var configurationSnapshot *model.PlatformLKGSnapshot
	configurationRead, configurationVerified := false, false
	verifiedArtifacts := map[string]bool{}
	var newPayload []byte
	if task != nil {
		if task.ID == "" || task.NodeUpdaterID == "" || task.Status != model.NodeUpdateTaskStatusPending {
			return nil, ErrInvalidInput
		}
		copy := *task
		copy.Payload = cloneMap(task.Payload)
		proof.newTask = &copy
		var err error
		newPayload, err = json.Marshal(copy.Payload)
		if err != nil || len(newPayload) > 65536 {
			return nil, ErrConflict
		}
	}
	s.platformArtifactKeyringMu.RLock()
	keyring := s.platformArtifactKeyring
	proof.keyringRevision = s.platformArtifactKeyringRevision
	s.platformArtifactKeyringMu.RUnlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return nil, err
		}
	}
	if err := schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint`).Scan(&proof.horizon); err != nil {
		return nil, err
	}
	proof.sourceEpoch, err = readInputArchiveEpoch(ctx, tx, -1)
	if err != nil {
		return nil, err
	}
	if ordinary {
		proof.capabilityOnly, err = compilerInputCapabilityPhase(ctx, tx, false)
		if err != nil {
			return nil, err
		}
	}
	if inventory {
		roots, err = compilerInputHotRoots(ctx, tx)
		if err != nil {
			return nil, err
		}
	}
	var publicationSnapshots []model.PlatformLKGSnapshot
	var publicationVerified []bool
	publicationBytes := 0
	if len(pending) == 1 {
		proof.pendingRoot = pending[0]
		if publicationBytes, err = preparePendingRootNativeWitness(ctx, tx, proof.pendingRoot); err != nil {
			return nil, err
		}
		if expiry := time.Unix(proof.pendingRoot.policy.expiresUnix, 0); expiry.Before(proof.expires) {
			proof.expires = expiry
		}
	}
	if consumer != nil {
		roots, err = prepareCompilerInputConsumerRoots(ctx, tx, consumer)
		if err != nil {
			return nil, err
		}
		// Consumer intent and namespace projection share the first existing
		// 32MiB resolver allowance; no extra metadata/transport budget.
		publicationBytes = consumer.bytes + 1024
		proof.consumerKind = consumer.artifactKind
	}
	if len(reads) != 0 {
		roots, publicationSnapshots, publicationBytes, err = prepareCompilerInputReadRoots(ctx, tx, proof, reads)
		if err != nil {
			return nil, err
		}
		publicationVerified = make([]bool, len(publicationSnapshots))
	}
	if publication != nil {
		roots, publicationSnapshots, err = prepareCompilerInputPublicationRoots(ctx, tx, proof, *publication)
		if err != nil {
			return nil, err
		}
		// Charge the bounded selection/tuple witnesses and selected LKG to
		// the first resolver batch, sharing its existing 32MiB allowance.
		// Publication metadata does not receive a separate transport budget.
		publicationBytes = len(publication.requestBindings) + 1024*(1+len(proof.tuples))
		if publication.hasGuard {
			g := publication.guard
			publicationBytes += len(g.PolicyReleaseID) + len(g.PreviousReleaseID) + len(g.PreviousFullReleaseID) + len(g.BaselineArtifactID) + len(g.FailedReleaseID) + len(g.Phase) + len(publication.lkgReleaseID)
		}
		// Selector keys may contain escaped scope text larger than 1KiB.
		// Charge their actual strings in addition to the structural allowance.
		for _, tuple := range proof.tuples {
			publicationBytes += len(tuple.table) + len(tuple.key) + len(tuple.xmin) + len(tuple.ctid)
		}
		publicationVerified = make([]bool, len(publicationSnapshots))
		for _, snapshot := range publicationSnapshots {
			encoded, err := json.Marshal(snapshot)
			if err != nil {
				return nil, err
			}
			publicationBytes += len(encoded)
		}
	}
	if proof.capabilityOnly {
		proof.closure.Roots = append([]inputarchive.HotRoot(nil), roots...)
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return proof, nil
	}
	loadConfiguration := func(taskType string, payload []byte) (int, error) {
		var value any
		if len(payload) > 65536 || json.Unmarshal(payload, &value) != nil {
			return 0, ErrConflict
		}
		if configurationRead || !compilerInputTaskUsesConfiguration(taskType, value) {
			return 0, nil
		}
		configurationRead = true
		snapshot, err := readCompilerInputTaskConfigurationSnapshot(ctx, tx)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return 0, err
		}
		if err == nil {
			configurationSnapshot = &snapshot
			encoded, err := json.Marshal(snapshot)
			if err != nil {
				return 0, err
			}
			size := len(encoded)
			if verifiedArtifacts[snapshot.ArtifactID] {
				artifact, artifactBytes, err := readCompilerInputActivationArtifact(ctx, tx, snapshot.ArtifactID)
				if err != nil {
					return 0, err
				}
				size += artifactBytes
				configurationVerified = platformsafety.EvaluatePlatformLKGSnapshot(snapshot, artifact, keyring, time.Now()).Pass
			}
			return size, nil
		}
		return 0, nil
	}
	proof.closure, err = inputarchive.PlanHotClosureBatched(ctx, roots,
		inputarchive.HotClosureBudget{Roots: compilerInputHotRootLimit, Nodes: 32768, Edges: 262144, Bindings: 524288},
		func(ctx context.Context, refs []inputarchive.HotReference) (map[inputarchive.HotReference]inputarchive.HotNode, error) {
			other := []inputarchive.HotReference{}
			nodes := map[inputarchive.HotReference]inputarchive.HotNode{}
			bytes := publicationBytes
			publicationBytes = 0
			// Task nodes introduce the implicit selector dependency. Load its
			// bounded metadata before artifacts in this same resolver batch;
			// unrelated artifact-only activations do not read the selector.
			for _, ref := range refs {
				if ref.Kind != "task" {
					continue
				}
				var taskType string
				var payload []byte
				if proof.newTask != nil && ref.ID == proof.newTask.ID {
					taskType, payload = proof.newTask.Type, newPayload
				} else if err := tx.QueryRowContext(ctx, `SELECT task_type,left(COALESCE(payload_json,'{}'::jsonb)::text,65537) FROM fugue_node_update_tasks WHERE id=$1`, ref.ID).Scan(&taskType, &payload); err != nil {
					return nil, err
				}
				selectorBytes, err := loadConfiguration(taskType, payload)
				bytes += len(payload) + selectorBytes
				if err != nil || bytes > 32<<20 {
					return nil, errors.Join(err, ErrConflict)
				}
			}
			for _, ref := range refs {
				if proof.newTask != nil && ref.Kind == "task" && ref.ID == proof.newTask.ID {
					bytes += len(newPayload)
					if bytes > 32<<20 {
						return nil, ErrConflict
					}
					node, err := compilerInputTaskHotNode(ctx, tx, proof.newTask.Type, newPayload)
					if err != nil {
						return nil, err
					}
					node.Identity = ref
					nodes[ref] = node
					continue
				}
				if ref.Kind != "artifact" {
					other = append(other, ref)
					continue
				}
				// Unlike metadata projection, this reads the actual complete
				// content and provenance and verifies the existing signature.
				artifact, size, err := readCompilerInputActivationArtifact(ctx, tx, ref.ID)
				if err != nil {
					return nil, err
				}
				bytes += size
				if bytes > 32<<20 || !platformsafety.EvaluateArtifactIntegrity(artifact, keyring).Pass {
					return nil, fmt.Errorf("activation artifact integrity/budget unavailable: %w", ErrConflict)
				}
				verifiedArtifacts[artifact.ID] = true
				for i, snapshot := range publicationSnapshots {
					if snapshot.ArtifactID == artifact.ID {
						publicationVerified[i] = platformLKGSnapshotMatchesArtifact(snapshot, artifact, time.Now(), keyring)
					}
				}
				if configurationSnapshot != nil && configurationSnapshot.ArtifactID == artifact.ID {
					configurationVerified = platformsafety.EvaluatePlatformLKGSnapshot(*configurationSnapshot, artifact, keyring, time.Now()).Pass
				}
				var node inputarchive.HotNode
				if inventory || compilerInputReadRetainsOriginal(reads) {
					node, err = compilerInputArtifactHotNode(artifact)
				} else {
					node, err = compilerInputArtifactRuntimeNode(artifact)
				}
				if err != nil {
					return nil, err
				}
				nodes[ref] = node
			}
			if len(other) > 0 {
				// Share one transport budget with the full signed artifacts.
				// A mixed batch must not receive an independent 32MiB allowance
				// for task/ledger/manifest metadata.
				metadataBytes, err := compilerInputActivationMetadataBytes(ctx, tx, other)
				if err != nil || int64(bytes)+metadataBytes > 32<<20 {
					return nil, fmt.Errorf("activation mixed batch budget unavailable: %w", errors.Join(err, ErrConflict))
				}
				metadata := []inputarchive.HotReference{}
				for _, ref := range other {
					if ref.Kind != "input" {
						metadata = append(metadata, ref)
						continue
					}
					// Discover cold dependencies without touching the retired
					// relation. This is NOT byte evidence: execution admission
					// requires full-readback pins; pending creation only retains.
					node, err := resolveCompilerInputActivationInput(ctx, tx, ref)
					if err != nil {
						return nil, err
					}
					nodes[ref] = node
				}
				if len(metadata) > 0 {
					resolved, err := resolveCompilerInputHotBatch(ctx, tx, metadata)
					if err != nil {
						return nil, err
					}
					for ref, node := range resolved {
						nodes[ref] = node
					}
				}
			}
			return nodes, nil
		})
	if err != nil {
		return nil, err
	}
	if !proof.closure.MetadataComplete || len(proof.closure.Blockers) != 0 {
		return nil, fmt.Errorf("activation dependency closure incomplete: %w", ErrConflict)
	}
	for _, verified := range publicationVerified {
		if !verified {
			return nil, fmt.Errorf("dependency LKG integrity unavailable: %w", ErrConflict)
		}
	}
	seen := map[string]bool{}
	physical := make(map[string]compilerInputActivationTuple, len(proof.tuples))
	unique := proof.tuples[:0]
	for _, witness := range proof.tuples {
		key := witness.table + "/" + witness.key
		if previous, ok := physical[key]; ok {
			if previous != witness {
				return nil, ErrConflict
			}
			continue
		}
		physical[key], seen[key] = witness, true
		unique = append(unique, witness)
	}
	proof.tuples = unique
	for _, node := range proof.closure.Nodes {
		var table, key string
		switch node.Identity.Kind {
		case "artifact":
			table, key = "fugue_platform_artifacts", node.Identity.ID
		case "release":
			table, key = "fugue_platform_artifact_releases", node.Identity.ID
		case "expectation":
			table, key = "fugue_platform_expected_consumer_sets", node.Identity.ID
		case "task":
			table, key = "fugue_node_update_tasks", node.Identity.ID
		case "input":
			// Inline historical inputs cannot be consumed until the existing
			// durable local-pin protocol can prove their independent bytes.
			if node.Format == "legacy-inline" {
				return nil, fmt.Errorf("activation input has no pinnable representation: %w", ErrConflict)
			}
			table, key = "fugue_platform_input_manifests", node.Identity.ID
			proof.inputs = append(proof.inputs, key)
			manifest, err := readInputManifest(ctx, tx, key)
			if err != nil {
				return nil, err
			}
			proof.inputManifests[key] = manifest
		case "generation":
			// Its unique(kind,scope,generation) row is the independently
			// resolved artifact descendant; that exact tuple is bound below.
			continue
		default:
			return nil, ErrConflict
		}
		if seen[table+"/"+key] {
			continue
		}
		seen[table+"/"+key] = true
		prospective := node.Identity.Kind == "task" && proof.newTask != nil && key == proof.newTask.ID
		if !prospective {
			witness := compilerInputActivationTuple{table: table, key: key}
			if err := readCompilerInputActivationTuple(ctx, tx, &witness, false); err != nil {
				return nil, err
			}
			proof.tuples = append(proof.tuples, witness)
		}
		if node.Identity.Kind == "task" {
			var taskType string
			var raw []byte
			if prospective {
				taskType, raw = proof.newTask.Type, newPayload
			} else {
				if err := tx.QueryRowContext(ctx, `SELECT task_type,left(COALESCE(payload_json,'{}'::jsonb)::text,65537) FROM fugue_node_update_tasks WHERE id=$1`, key).Scan(&taskType, &raw); err != nil {
					return nil, err
				}
			}
			var payload any
			if len(raw) > 65536 || json.Unmarshal(raw, &payload) != nil {
				return nil, ErrConflict
			}
			if compilerInputTaskUsesConfiguration(taskType, payload) && !seen["fugue_platform_lkg_snapshots/"+compilerInputTaskConfigurationSelectorKey()] {
				if !configurationRead {
					return nil, ErrConflict
				}
				if configurationSnapshot != nil {
					if !configurationVerified {
						return nil, fmt.Errorf("task configuration LKG integrity unavailable: %w", ErrConflict)
					}
					if configurationSnapshot.ExpiresAt.Before(proof.expires) {
						proof.expires = configurationSnapshot.ExpiresAt
					}
				}
				selector := compilerInputActivationTuple{table: "fugue_platform_lkg_snapshots", key: compilerInputTaskConfigurationSelectorKey()}
				if err := readCompilerInputActivationTuple(ctx, tx, &selector, false); err != nil {
					return nil, err
				}
				proof.tuples = append(proof.tuples, selector)
				seen[selector.table+"/"+selector.key] = true
			}
		}
	}
	sort.Strings(proof.inputs)
	sort.Slice(proof.tuples, func(i, j int) bool {
		a, b := proof.tuples[i], proof.tuples[j]
		return a.table < b.table || a.table == b.table && a.key < b.key
	})
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return proof, nil
}

func readCompilerInputTaskConfigurationSnapshot(ctx context.Context, tx *sql.Tx) (model.PlatformLKGSnapshot, error) {
	return scanPlatformLKGSnapshot(tx.QueryRowContext(ctx, `SELECT id,artifact_id,artifact_kind,scope_key,
 left(scope_json::text,16385),schema_version,generation,generation_sequence,content_hash,left(artifact_provenance_json::text,16385),
 verified_by_release_id,verification_evidence_hash,left(snapshot_provenance_json::text,16385),expires_at,created_at,updated_at
 FROM fugue_platform_lkg_snapshots WHERE artifact_kind=$1 AND scope_key='global'`, model.PlatformArtifactKindReleaseSet))
}

func resolveCompilerInputActivationInput(ctx context.Context, tx inputQueryer, ref inputarchive.HotReference) (inputarchive.HotNode, error) {
	m, err := readInputManifest(ctx, tx, ref.ID)
	if err != nil {
		return inputarchive.HotNode{}, err
	}
	if err := validateCompilerInputManifest(m); err != nil {
		return inputarchive.HotNode{}, err
	}
	return inputarchive.HotNode{Identity: inputarchive.HotReference{Kind: "input", ID: m.Digest},
		ContentHash: m.Digest, Format: m.Format, Location: m.Location, LogicalBytes: m.Size,
		References: []inputarchive.HotReference{}, NeedsLocalCopy: (m.Format == inputFormatV1 || m.Format == inputFormatCodecV2) && m.Location != "hot"}, nil
}

// Measure the actual selected metadata before transporting it. row(...)::text
// includes separators/escaping, so it conservatively bounds the same columns
// returned by the typed resolver. This does not read original input bytes.
func compilerInputActivationMetadataBytes(ctx context.Context, tx *sql.Tx, refs []inputarchive.HotReference) (int64, error) {
	var total int64
	for _, ref := range refs {
		var query string
		args := []any{ref.ID}
		switch ref.Kind {
		case "task":
			query = `SELECT octet_length(COALESCE(payload_json,'{}'::jsonb)::text) FROM fugue_node_update_tasks WHERE id=$1 AND status IN ('pending','running')`
		case "release":
			query = `SELECT octet_length(row(id,artifact_id,artifact_kind,scope_key,generation,candidate_generation,serving_unverified_generation,verified_lkg_generation,pinned_rollback_generation,rollback_target_generation)::text) FROM fugue_platform_artifact_releases WHERE id=$1`
		case "expectation":
			query = `SELECT octet_length(row(release_set_id,artifact_release_id,artifact_kind,scope_key,expected_generation)::text) FROM fugue_platform_expected_consumer_sets WHERE id=$1`
		case "generation":
			query = `SELECT octet_length(row(id,artifact_kind,scope_key,generation,content_hash)::text) FROM fugue_platform_artifacts WHERE generation=$1 AND artifact_kind=$2 AND scope_key=$3`
			args = append(args, ref.ArtifactKind, ref.ScopeKey)
		case "input":
			query = `SELECT octet_length(row(content_hash,format,size_bytes,chunk_size,chunk_count,created_at,storage_location)::text)+128 FROM fugue_platform_input_manifests WHERE content_hash=$1`
		default:
			return 0, ErrInvalidInput
		}
		var size int64
		err := tx.QueryRowContext(ctx, query, args...).Scan(&size)
		if errors.Is(err, sql.ErrNoRows) {
			// Let the graph resolver report the missing identity. An inline
			// input is independently refused as non-pinnable after resolution.
			continue
		}
		if err != nil {
			return 0, err
		}
		if size < 0 || size > 32<<20 || ref.Kind == "task" && size > 65536 || total > (32<<20)-size {
			return 0, ErrConflict
		}
		total += size
	}
	return total, nil
}

func readCompilerInputActivationArtifact(ctx context.Context, tx *sql.Tx, id string) (model.PlatformArtifact, int, error) {
	var contentBytes int
	scanner := &compilerInputActivationArtifactScanner{sqlScanner: tx.QueryRowContext(ctx, `SELECT id,artifact_kind,scope_key,
 left(scope_json::text,16385),schema_version,generation,generation_sequence,status,content_hash,
 CASE WHEN octet_length(content_json::text)<=$2 THEN content_json ELSE NULL END,
 left(validation_results_json::text,16385),compatibility_floor,left(metadata_json::text,16385),
 created_by_type,created_by_id,left(provenance_json::text,16385),created_at,updated_at,
 octet_length(content_json::text)
 FROM fugue_platform_artifacts WHERE id=$1`, id, compilerInputHotArtifactBytes), size: &contentBytes}
	artifact, err := scanPlatformArtifactWithDecoder(scanner, func(raw []byte) (map[string]any, error) {
		if contentBytes > compilerInputHotArtifactBytes || len(raw) == 0 {
			return nil, ErrConflict
		}
		var content map[string]any
		err := json.Unmarshal(raw, &content)
		return content, err
	})
	return artifact, contentBytes + scanner.metadataBytes, err
}

type compilerInputActivationArtifactScanner struct {
	sqlScanner
	size          *int
	metadataBytes int
}

func (s *compilerInputActivationArtifactScanner) Scan(dest ...any) error {
	if err := s.sqlScanner.Scan(append(dest, s.size)...); err != nil {
		return err
	}
	for _, index := range []int{3, 10, 12, 15} {
		raw := *dest[index].(*[]byte)
		if len(raw) > 16<<10 {
			return ErrConflict
		}
		s.metadataBytes += len(raw)
	}
	return nil
}

func readCompilerInputActivationTuple(ctx context.Context, tx *sql.Tx, tuple *compilerInputActivationTuple, lock bool) error {
	return readCompilerInputActivationTupleMode(ctx, tx, tuple, lock, false)
}

func readCompilerInputActivationTupleMode(ctx context.Context, tx *sql.Tx, tuple *compilerInputActivationTuple, lock, lkgReadOnly bool) error {
	if tuple.table == "fugue_platform_release_lanes" {
		if tuple.key == "" || len(tuple.key) > 2048 {
			return ErrInvalidInput
		}
		query := `SELECT xmin::text,ctid::text,tableoid::bigint,pg_relation_filenode(tableoid)::bigint FROM fugue_platform_release_lanes WHERE lane_key=$1`
		if lock {
			query += ` FOR SHARE`
		}
		read := func() error {
			return tx.QueryRowContext(ctx, query, tuple.key).Scan(&tuple.xmin, &tuple.ctid, &tuple.relation, &tuple.filenode)
		}
		err := read()
		if errors.Is(err, sql.ErrNoRows) && lock {
			// Only a missing lane needs table SHARE to exclude insertion.
			// Re-read after acquiring it: an insertion in that gap must not
			// be mistaken for the prepared absence witness.
			if _, err := tx.ExecContext(ctx, `LOCK TABLE fugue_platform_release_lanes IN SHARE ROW EXCLUSIVE MODE`); err != nil {
				return err
			}
			err = read()
		}
		if errors.Is(err, sql.ErrNoRows) {
			tuple.xmin, tuple.ctid = "0", "(0,0)"
			return tx.QueryRowContext(ctx, `SELECT 'fugue_platform_release_lanes'::regclass::oid::bigint,pg_relation_filenode('fugue_platform_release_lanes'::regclass)::bigint`).Scan(&tuple.relation, &tuple.filenode)
		}
		return err
	}
	if tuple.table == "fugue_platform_lkg_snapshots" {
		kind, scope, err := compilerInputLKGSelector(tuple.key)
		if err != nil {
			return ErrInvalidInput
		}
		if lock && !lkgReadOnly {
			// Acquire the eventual write lock before any row lock. Existing
			// selectors can then be written independently; absence still needs
			// a self-exclusive gap guard to prevent concurrent insertions.
			guard := `LOCK TABLE fugue_platform_lkg_snapshots IN ROW EXCLUSIVE MODE`
			if tuple.xmin == "0" || tuple.xmin == "" {
				guard = `LOCK TABLE fugue_platform_lkg_snapshots IN SHARE ROW EXCLUSIVE MODE`
			}
			if _, err := tx.ExecContext(ctx, guard); err != nil {
				return err
			}
		}
		q := `SELECT xmin::text,ctid::text,tableoid::bigint,pg_relation_filenode(tableoid)::bigint
 FROM fugue_platform_lkg_snapshots WHERE artifact_kind=$1 AND scope_key=$2`
		if lock && lkgReadOnly {
			// A present unique selector needs only a row guard. Unrelated
			// scopes may advance while this exact row and relation stay pinned.
			q += ` FOR SHARE`
		} else if lock {
			q += ` FOR UPDATE`
		}
		read := func() error {
			return tx.QueryRowContext(ctx, q, kind, scope).Scan(&tuple.xmin, &tuple.ctid, &tuple.relation, &tuple.filenode)
		}
		err = read()
		if errors.Is(err, sql.ErrNoRows) && lock {
			if tuple.xmin != "0" && tuple.xmin != "" {
				// A previously present row disappeared. Fail without upgrading
				// a row guard to a table guard behind a waiting writer.
				return ErrConflict
			}
			// Rows cannot fence absence. Exclude insertion, then reread: a row
			// inserted in the gap must invalidate the prepared absence witness.
			guard := `LOCK TABLE fugue_platform_lkg_snapshots IN SHARE MODE`
			if !lkgReadOnly {
				guard = `LOCK TABLE fugue_platform_lkg_snapshots IN SHARE ROW EXCLUSIVE MODE`
			}
			if _, err := tx.ExecContext(ctx, guard); err != nil {
				return err
			}
			err = read()
		}
		if errors.Is(err, sql.ErrNoRows) {
			// Bind absence to the physical relation as well, not a reusable
			// all-zero identity after DROP/recreate or TRUNCATE.
			tuple.xmin, tuple.ctid = "0", "(0,0)"
			return tx.QueryRowContext(ctx, `SELECT 'fugue_platform_lkg_snapshots'::regclass::oid::bigint,pg_relation_filenode('fugue_platform_lkg_snapshots'::regclass)::bigint`).Scan(&tuple.relation, &tuple.filenode)
		}
		return err
	}
	column := "id"
	switch tuple.table {
	case "fugue_platform_artifacts", "fugue_platform_artifact_releases", "fugue_platform_expected_consumer_sets", "fugue_node_update_tasks":
	case "fugue_platform_input_manifests":
		column = "content_hash"
	default:
		return ErrInvalidInput
	}
	query := `SELECT xmin::text,ctid::text,tableoid::bigint,pg_relation_filenode(tableoid)::bigint FROM ` + tuple.table + ` WHERE ` + column + `=$1`
	if lock {
		query += ` FOR SHARE`
	}
	return tx.QueryRowContext(ctx, query, tuple.key).Scan(&tuple.xmin, &tuple.ctid, &tuple.relation, &tuple.filenode)
}

// withPreparedCompilerInputActivation owns the entire short transaction so no
// caller can accidentally commit a checked prefix after any error. All inputs
// must have exact durable verified pins; every dependency tuple is rechecked
// and locked until apply installs its REAL retention root and commits. The
// callback must retain its normal authorization/TTL/convergence checks and may
// not perform network/body work. This API does not manufacture that authority.
func (s *Store) withPreparedCompilerInputActivation(ctx context.Context, proof *compilerInputActivationProof, pins []compilerInputPinBinding, apply func(context.Context, *sql.Tx) error) error {
	return s.consumePreparedCompilerInput(ctx, proof, pins, false, nil, apply)
}

// Pending creation adds a conservative retention root, not execution authority.
// It must permit cold inputs so the existing task-ID based restoration runner
// can prepare them. Only this prospective pending-task path may omit pins;
// claim/activation always use withPreparedCompilerInputActivation.
func (s *Store) withPreparedCompilerInputTaskCreation(ctx context.Context, proof *compilerInputActivationProof, apply func(context.Context, *sql.Tx) error) error {
	if proof == nil || proof.newTask == nil || proof.newTask.Status != model.NodeUpdateTaskStatusPending {
		return ErrInvalidInput
	}
	return s.consumePreparedCompilerInput(ctx, proof, nil, true, nil, apply)
}

func (s *Store) consumePreparedCompilerInput(ctx context.Context, proof *compilerInputActivationProof, pins []compilerInputPinBinding, pendingOnly bool, beforeInputLocks, apply func(context.Context, *sql.Tx) error) error {
	if s == nil || proof == nil || proof.store != s || proof.database != s.db || apply == nil ||
		!time.Now().Before(proof.expires) || pendingOnly != (proof.newTask != nil || proof.pendingRoot != nil) ||
		proof.pendingRoot != nil && (!pendingCompilerInputProofMatches(proof) || proof.newTask != nil || proof.ordinary || proof.fullInventory) ||
		!pendingOnly && len(pins) != len(proof.inputs) || !proof.closure.MetadataComplete && !(proof.ordinary && proof.capabilityOnly) || proof.fullInventory {
		return ErrInvalidInput
	}
	ordered := append([]compilerInputPinBinding(nil), pins...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Key < ordered[j].Key })
	digests := map[string]bool{}
	for i, pin := range ordered {
		if !compilerInputPinKeyPattern.MatchString(pin.Key) || pin.Token < 1 || !inputDigestPattern.MatchString(pin.Digest) ||
			digests[pin.Digest] || i > 0 && ordered[i-1].Key == pin.Key {
			return ErrInvalidInput
		}
		digests[pin.Digest] = true
	}
	for _, digest := range proof.inputs {
		if !pendingOnly && !digests[digest] {
			return ErrInvalidInput
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// Recheck revision at entry and atomically with commit. Do not hold this
	// read lock across apply: its ordinary signature checks take the same lock
	// and a queued writer would make a recursive read lock deadlock.
	s.platformArtifactKeyringMu.RLock()
	currentRevision := s.platformArtifactKeyringRevision
	s.platformArtifactKeyringMu.RUnlock()
	if currentRevision != proof.keyringRevision {
		return ErrConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := setCompilerInputAdmissionTimeouts(ctx, tx); err != nil {
		return err
	}
	// Publication must retain its existing producer/scope-before-row order.
	// This hook may acquire those locks, never activate or do body/network I/O;
	// every proof, epoch and pin check below still precedes apply.
	if beforeInputLocks != nil {
		if err := beforeInputLocks(ctx, tx); err != nil {
			return err
		}
	}
	epoch, err := lockCompilerInputAdmissionBarrier(ctx, tx)
	if err != nil {
		return err
	}
	if epoch != proof.sourceEpoch {
		return ErrConflict
	}
	if proof.ordinary {
		capability, err := compilerInputCapabilityPhase(ctx, tx, true)
		if err != nil {
			return err
		}
		if proof.capabilityOnly != capability {
			return ErrConflict
		}
	}
	var horizon int64
	if err := tx.QueryRowContext(ctx, `SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint`).Scan(&horizon); err != nil {
		return err
	}
	if horizon < proof.horizon || horizon-proof.horizon >= 1<<31 || !time.Now().Before(proof.expires) {
		return ErrConflict
	}
	if len(ordered) > 0 {
		if err := schemamigrate.VerifyCompilerInputLocalPins(ctx, tx); err != nil {
			return err
		}
	}
	for start := 0; start < len(ordered); start += 32 {
		if err := checkCompilerInputVerifiedPins(ctx, tx, ordered[start:min(start+32, len(ordered))], epoch); err != nil {
			return err
		}
	}
	hasLKG, absentLKG := false, false
	for _, original := range proof.tuples {
		if original.table == "fugue_platform_lkg_snapshots" {
			hasLKG = true
			absentLKG = absentLKG || original.xmin == "0"
		}
	}
	// All relation/gap guards precede all dependency row guards. In
	// particular a writer must not hold a row and then upgrade behind an
	// absent-selector reader that holds SHARE and is waiting for that row.
	guard := ""
	if proof.lkgReadOnly && absentLKG {
		guard = `LOCK TABLE fugue_platform_lkg_snapshots IN SHARE MODE`
	} else if !proof.lkgReadOnly && hasLKG {
		guard = `LOCK TABLE fugue_platform_lkg_snapshots IN ROW EXCLUSIVE MODE`
		if absentLKG {
			guard = `LOCK TABLE fugue_platform_lkg_snapshots IN SHARE ROW EXCLUSIVE MODE`
		}
	}
	if guard != "" {
		if _, err := tx.ExecContext(ctx, guard); err != nil {
			return err
		}
	}
	for _, original := range proof.tuples {
		current := original
		if err := readCompilerInputActivationTupleMode(ctx, tx, &current, true, proof.lkgReadOnly); err != nil {
			return fmt.Errorf("activation dependency no longer available: %w", ErrConflict)
		}
		if current != original {
			return fmt.Errorf("activation dependency changed after preparation: %w", ErrConflict)
		}
	}
	if err := apply(ctx, tx); err != nil {
		return err
	}
	if proof.pendingRoot != nil && proof.pendingRoot.checkAdmission != nil {
		// Configuration changes cannot race this final check and commit. The
		// source and allocation witnesses must also reject their own drift.
		s.pendingRootAdmissionMu.RLock()
		defer s.pendingRootAdmissionMu.RUnlock()
		if s.pendingRootAdmissionRevision != proof.pendingRoot.admissionRevision {
			return ErrConflict
		}
		if err := proof.pendingRoot.checkAdmission(ctx); err != nil {
			return err
		}
		if err := checkPendingRootLeaseTime(ctx, tx, proof.pendingRoot.policy.expiresUnix); err != nil {
			return err
		}
	}
	// Revocation/rotation during apply rolls back all its writes. Keep this
	// lock only through commit, after the callback's own signature checks.
	s.platformArtifactKeyringMu.RLock()
	defer s.platformArtifactKeyringMu.RUnlock()
	if s.platformArtifactKeyringRevision != proof.keyringRevision || !time.Now().Before(proof.expires) {
		return ErrConflict
	}
	return tx.Commit()
}
