// fugue-production-control records reviewable intent and independently signed
// approval and reads provider state for review. It has no production dispatcher.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"fugue/internal/platformcontrol"
	"fugue/internal/productioncontrol"
	"fugue/internal/productioncontrol/providergate"
	"fugue/internal/releaseguardian"
	"fugue/internal/releaseguardian/dnscontrol"
	"fugue/internal/releaseguardian/participation"
	"github.com/jackc/pgx/v5/stdlib"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const usage = `usage: fugue-production-control inspect SUBMISSION
       fugue-production-control sign SUBMISSION EXPECTED_DIGEST KEY_ID PRIVATE_PKCS8_PEM TTL
       fugue-production-control sign-execution-witness DRAFT REVIEWED_FILE_DIGEST OBSERVER_PKCS8_PEM TTL
       fugue-production-control journal-recovery-create JOURNAL_CONFIG PG_SERVICE_FILE PG_SERVICE NEW_PRIVATE_DIRECTORY
       fugue-production-control journal-recovery-checkpoint JOURNAL_CONFIG [POSTGRES_SNAPSHOT]
       fugue-production-control journal-recovery-verify JOURNAL_CONFIG CHECKPOINT_DIGEST
       fugue-production-control verify-release-recovery-mirror CONFIG
       fugue-production-control observe-sealed-release-recovery-pair NAMESPACE COMPONENT KUBECONFIG CONTEXT
       fugue-production-control verify-sealed-release-recovery-mirror CONFIG KUBECONFIG CONTEXT
       fugue-production-control capture-release-recovery-archive NAMESPACE COMPONENT KUBECONFIG CONTEXT NEW_PRIVATE_DIRECTORY
       fugue-production-control observe-release-registry-rehome ARCHIVE PIN GROUP KUBECONFIG CONTEXT
       fugue-production-control draft-release-registry-rehome JOURNAL_CONFIG KUBECONFIG CONTEXT ARCHIVE PIN GROUP REQUEST MIRROR_CONFIG
       fugue-production-control pack-release-registry-rehome ARCHIVE PIN SUBMISSION NEW_PRIVATE_DIRECTORY
       fugue-production-control stage-release-registry-rehome-input BUNDLE PIN current|rollback NEW_PRIVATE_DIRECTORY
       fugue-production-control observe-release-registry-rehome-pending JOURNAL_CONFIG KUBECONFIG CONTEXT ARCHIVE PIN INTENT_DIGEST ENROLLMENT_CONFIG
       fugue-production-control sign-registry-rehome-no-write DRAFT REVIEWED_FILE_DIGEST OBSERVER_PKCS8_PEM TTL
       fugue-production-control settle-registry-rehome-no-write JOURNAL_CONFIG KUBECONFIG CONTEXT ARCHIVE PIN INTENT_DIGEST ENROLLMENT_CONFIG ACTOR SIGNED_CLOSURE
       fugue-production-control observe-storage-audit-owner CONFIG
       fugue-production-control observe-sample-retention CONFIG
       fugue-production-control sample-retention-baseline-checkpoint CONFIG
       fugue-production-control sample-retention-baseline-import CONFIG WITNESS_DIGEST
       fugue-production-control shadow-storage-audit CONFIG KUBECONFIG CONTEXT SPEC CANDIDATE
       fugue-production-control shadow-storage-audit-native CONFIG KUBECONFIG CONTEXT SPEC CANDIDATE
       fugue-production-control render-storage-audit-projection-fence SPEC
       fugue-production-control draft-storage-audit JOURNAL_CONFIG REQUEST SHADOW
       fugue-production-control record-intent JOURNAL_CONFIG ACTOR SUBMISSION
       fugue-production-control record-approval JOURNAL_CONFIG APPROVAL
       fugue-production-control shadow-dns-policy HTTPS_ORIGIN READ_TOKEN_FILE NEXT_PRODUCER_ARTIFACT_ID
       fugue-production-control prepare-dns-policy HTTPS_ORIGIN READ_TOKEN_FILE DRAFT_TOKEN_FILE HOLD_CHANGE
       fugue-production-control draft-dns-policy JOURNAL_CONFIG HTTPS_ORIGIN READ_TOKEN_FILE NEXT_PRODUCER_ARTIFACT_ID REQUEST
       fugue-production-control draft-dns-policy-rollback JOURNAL_CONFIG HTTPS_ORIGIN READ_TOKEN_FILE DRAFT_TOKEN_FILE ORIGINAL_INTENT_DIGEST REQUEST
       fugue-production-control observe-dns-cutover JOURNAL_CONFIG
       fugue-production-control draft-dns-cutover CLUSTER REQUEST PLAN
       fugue-production-control draft-front-worker JOURNAL_CONFIG REQUEST SHADOW
       fugue-production-control render-front-worker-cutover SPEC
       fugue-production-control observe-front-worker-cutover SPEC KUBECONFIG CONTEXT
       fugue-production-control draft-participation CLUSTER REQUEST POLICY [PREVIOUS_GENERATION PREVIOUS_DIGEST]
       fugue-production-control store-participation JOURNAL_CONFIG ACTOR INTENT_DIGEST APPROVAL_DIGEST
       fugue-production-control read-participation JOURNAL_CONFIG [GENERATION]
       fugue-production-control preview-participation JOURNAL_CONFIG NODE SNAPSHOT
       fugue-production-control observe-participation-readers JOURNAL_CONFIG KUBECONFIG CONTEXT NODE
       fugue-production-control observe-participation-member JOURNAL_CONFIG KUBECONFIG CONTEXT NODE
       fugue-production-control shadow-participation-kubernetes JOURNAL_CONFIG KUBECONFIG CONTEXT NODE
       fugue-production-control issue-participation-observer JOURNAL_CONFIG NODE KEY_ID PRIVATE_SEED_FILE
       fugue-production-control shadow-participation-dns JOURNAL_CONFIG KUBECONFIG CONTEXT NODE PRIVATE_TOKEN_FILE
       fugue-production-control draft-participation-hold JOURNAL_CONFIG KUBECONFIG CONTEXT NODE REQUEST [PRIVATE_TOKEN_FILE]
       fugue-production-control draft-participation-hold-rollback JOURNAL_CONFIG KUBECONFIG CONTEXT ORIGINAL_INTENT_DIGEST REQUEST
       fugue-production-control preflight-participation-hold JOURNAL_CONFIG KUBECONFIG CONTEXT SUBMISSION [PRIVATE_TOKEN_FILE]
       fugue-production-control render-participation-freeze NODE
       fugue-production-control observe-participation-freeze JOURNAL_CONFIG KUBECONFIG CONTEXT NODE
       fugue-production-control observe-participation-offline-guard JOURNAL_CONFIG KUBECONFIG CONTEXT SUBMISSION WRITER
       fugue-production-control observe-participation-rollback-boundary JOURNAL_CONFIG KUBECONFIG CONTEXT SUBMISSION WRITER
       fugue-production-control observe-participation-offline-readback JOURNAL_CONFIG KUBECONFIG CONTEXT SUBMISSION
       fugue-production-control observe-participation-pending JOURNAL_CONFIG KUBECONFIG CONTEXT INTENT_DIGEST
       fugue-production-control migrate-journal JOURNAL_OWNER_CONFIG
       fugue-production-control migrate-storage-audit-provider PROVIDER_OWNER_CONFIG`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer, now func() time.Time) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	emit := json.NewEncoder(out).Encode
	switch args[0] {
	case "observe-release-registry-rehome", "draft-release-registry-rehome":
		return registryRehomeReviewCommand(ctx, args, out)
	case "pack-release-registry-rehome":
		return packRegistryRehomeRecovery(args[1:], out)
	case "stage-release-registry-rehome-input":
		return stageRegistryRehomeReleaseInput(args[1:], out)
	case "observe-release-registry-rehome-pending":
		return pendingRegistryRehomeCommand(ctx, args[1:], out, false)
	case "sign-registry-rehome-no-write":
		return signRegistryRehomeNoWrite(args[1:], out, now)
	case "settle-registry-rehome-no-write":
		return pendingRegistryRehomeCommand(ctx, args[1:], out, true)
	case "capture-release-recovery-archive":
		if len(args) != 6 {
			return errors.New(usage)
		}
		return captureReleaseRecoveryArchive(ctx, args[1], args[2], args[3], args[4], args[5], out)
	case "observe-sealed-release-recovery-pair":
		if len(args) != 5 {
			return errors.New(usage)
		}
		client, err := sealedRecoveryClient(args[3], args[4])
		if err != nil {
			return err
		}
		pair, err := observeSealedRecoveryPair(ctx, client, args[1], args[2])
		if err != nil {
			return err
		}
		return emit(pair)
	case "verify-sealed-release-recovery-mirror":
		if len(args) != 4 {
			return errors.New(usage)
		}
		return verifySealedReleaseRecoveryMirror(ctx, args[1], args[2], args[3], out)
	case "verify-release-recovery-mirror":
		if len(args) != 2 {
			return errors.New(usage)
		}
		return verifyReleaseRecoveryMirror(ctx, args[1], out)
	case "journal-recovery-create":
		if len(args) != 5 {
			return errors.New(usage)
		}
		return createJournalRecovery(ctx, args[1], args[2], args[3], args[4], out)
	case "journal-recovery-checkpoint", "journal-recovery-verify":
		if len(args) < 2 || len(args) > 3 || (args[0] == "journal-recovery-verify" && len(args) != 3) {
			return errors.New(usage)
		}
		return withJournalDeadline(ctx, args[1], true, 5*time.Minute, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			var checkpoint productioncontrol.JournalRecoveryCheckpoint
			var err error
			if args[0] == "journal-recovery-verify" {
				checkpoint, err = j.VerifyRecoveryCheckpoint(ctx, cluster, args[2])
			} else {
				var snapshot string
				if len(args) == 3 {
					snapshot = args[2]
				}
				checkpoint, err = j.RecoveryCheckpoint(ctx, cluster, snapshot)
			}
			if err != nil {
				return err
			}
			return emit(checkpoint)
		})
	case "render-storage-audit-projection-fence":
		if len(args) != 2 {
			return errors.New(usage)
		}
		return renderStorageAuditProjectionFence(args[1], out)
	case "shadow-storage-audit", "shadow-storage-audit-native":
		if len(args) != 6 {
			return errors.New(usage)
		}
		return shadowStorageAudit(ctx, args[1:], args[0] == "shadow-storage-audit-native", out)
	case "draft-storage-audit":
		if len(args) != 4 {
			return errors.New(usage)
		}
		return draftStorageAudit(ctx, args[1], args[2], args[3], out)
	case "observe-storage-audit-owner":
		if len(args) != 2 {
			return errors.New(usage)
		}
		return observeStorageAudit(ctx, args[1], out)
	case "observe-sample-retention":
		if len(args) != 2 {
			return errors.New(usage)
		}
		return observeSampleRetention(ctx, args[1], out)
	case "sample-retention-baseline-checkpoint", "sample-retention-baseline-import":
		persist := args[0] == "sample-retention-baseline-import"
		if (!persist && len(args) != 2) || (persist && len(args) != 3) {
			return errors.New(usage)
		}
		var evidence string
		if persist {
			evidence = args[2]
			if !sampleRetentionWitnessDigest.MatchString(evidence) {
				return errors.New("sample retention import requires an exact witness digest")
			}
		}
		return runSampleRetentionBaseline(ctx, args[1], evidence, out, now)
	case "sign-execution-witness":
		return signExecutionWitness(args[1:], out, now)
	case "render-front-worker-cutover", "observe-front-worker-cutover":
		return frontWorkerCutover(ctx, args, out)
	case "draft-front-worker":
		if len(args) != 4 {
			return errors.New(usage)
		}
		raw, err := readFile(args[3], productioncontrol.MaxPayloadBytes, false)
		if err != nil {
			return err
		}
		shadow, err := productioncontrol.DecodeFrontWorkerShadow(raw)
		if err != nil {
			return err
		}
		return withJournal(ctx, args[1], true, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			scope := productioncontrol.Scope{ClusterID: cluster,
				ResourceID: productioncontrol.FrontWorkerResource(shadow.Namespace, shadow.Journal.Recipient.Group)}
			state, err := j.ReadResource(ctx, scope)
			if err != nil {
				return err
			}
			if state.Pending != "" {
				return productioncontrol.ErrResourceBusy
			}
			submission, err := productioncontrol.DraftFrontWorker(cluster, args[2], state.LastVerified, shadow)
			if err != nil {
				return err
			}
			return emit(submission)
		})
	case "draft-dns-cutover":
		if len(args) != 4 {
			return errors.New(usage)
		}
		raw, err := readFile(args[3], productioncontrol.MaxPayloadBytes, false)
		if err != nil {
			return err
		}
		plan, err := productioncontrol.DecodeDNSCutoverPlan(raw)
		if err != nil {
			return err
		}
		s, err := productioncontrol.DraftDNSCutover(args[1], args[2], plan)
		if err != nil {
			return err
		}
		return emit(s)
	case "observe-dns-cutover":
		if len(args) != 2 {
			return errors.New(usage)
		}
		return withJournal(ctx, args[1], true, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			boundary, err := j.ReadGlobalProducerBoundary(ctx, dnscontrol.Scope(cluster))
			if err != nil {
				return fmt.Errorf("read DNS provider and journal boundary: %w", err)
			}
			return emit(boundary)
		})
	case "prepare-dns-policy":
		if len(args) != 5 {
			return errors.New(usage)
		}
		raw, err := readFile(args[4], 64<<10, false)
		if err != nil {
			return err
		}
		change, err := dnscontrol.DecodeHoldChange(raw)
		if err != nil {
			return err
		}
		readToken, err := readFile(args[2], 4096, true)
		if err != nil {
			return err
		}
		defer clear(readToken)
		draftToken, err := readFile(args[3], 4096, true)
		if err != nil {
			return err
		}
		defer clear(draftToken)
		reader, err := dnscontrol.NewReader(args[1], string(bytes.TrimSpace(readToken)))
		if err != nil {
			return err
		}
		preparer, err := dnscontrol.NewDraftPreparer(reader, string(bytes.TrimSpace(draftToken)))
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		o, err := preparer.Prepare(ctx, change, now)
		if err != nil {
			return err
		}
		return emitDNSShadow(args[1], o, now, emit)
	case "shadow-dns-policy", "draft-dns-policy":
		draft := args[0] == "draft-dns-policy"
		offset := 1
		expected := 4
		if draft {
			offset, expected = 2, 6
		}
		if len(args) != expected {
			return errors.New(usage)
		}
		token, err := readFile(args[offset+1], 4096, true)
		if err != nil {
			return err
		}
		defer clear(token)
		reader, err := dnscontrol.NewReader(args[offset], string(bytes.TrimSpace(token)))
		if err != nil {
			return err
		}
		if draft {
			return withJournalDeadline(ctx, args[1], true, 4*time.Minute, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
				s, err := reader.Draft(ctx, j, cluster, args[5], args[4], now)
				if err != nil {
					return err
				}
				return emit(s)
			})
		}
		ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
		defer cancel()
		o, err := reader.Observe(ctx, args[3], now)
		if err != nil {
			return err
		}
		return emitDNSShadow(args[offset], o, now, emit)
	case "draft-dns-policy-rollback":
		if len(args) != 7 {
			return errors.New(usage)
		}
		readToken, err := readFile(args[3], 4096, true)
		if err != nil {
			return err
		}
		defer clear(readToken)
		draftToken, err := readFile(args[4], 4096, true)
		if err != nil {
			return err
		}
		defer clear(draftToken)
		reader, err := dnscontrol.NewReader(args[2], string(bytes.TrimSpace(readToken)))
		if err != nil {
			return err
		}
		preparer, err := dnscontrol.NewDraftPreparer(reader, string(bytes.TrimSpace(draftToken)))
		if err != nil {
			return err
		}
		return withJournalDeadline(ctx, args[1], true, 5*time.Minute, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			submission, err := reader.DraftRollback(ctx, j, preparer, cluster, args[5], args[6], now)
			if err != nil {
				return err
			}
			return emit(submission)
		})
	case "render-participation-freeze":
		if len(args) != 2 {
			return errors.New(usage)
		}
		policy, binding, err := participation.FreezeNodePolicy(args[1])
		if err != nil {
			return err
		}
		return emit(struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Items      []any  `json:"items"`
		}{"v1", "List", []any{policy, binding}})
	case "observe-participation-pending":
		if len(args) != 5 {
			return errors.New(usage)
		}
		kubeConfig, err := readKubeObservationConfig(args[2], args[3])
		if err != nil {
			return err
		}
		return withJournal(ctx, args[1], true, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			pending, err := participation.ObservePendingOfflineHold(ctx, j, kubeConfig, cluster, args[4])
			if err != nil {
				return err
			}
			return emit(struct {
				Kind              string                                   `json:"kind"`
				ExecutionEvidence bool                                     `json:"executionEvidence"`
				JournalCluster    string                                   `json:"journalCluster"`
				KubeContext       string                                   `json:"kubeContext"`
				Pending           participation.PendingOfflineHoldReadback `json:"pending"`
			}{"participation_pending_offline_hold_readback", false, cluster, args[3], pending})
		})
	case "draft-participation-hold-rollback":
		if len(args) != 6 {
			return errors.New(usage)
		}
		kubeConfig, err := readKubeObservationConfig(args[2], args[3])
		if err != nil {
			return err
		}
		return withJournal(ctx, args[1], true, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			draft, err := participation.DraftOfflineHoldRollback(ctx, j, kubeConfig, cluster, args[4], args[5])
			if err != nil {
				return err
			}
			return emit(draft)
		})
	case "draft-participation":
		if len(args) != 4 && len(args) != 6 {
			return errors.New(usage)
		}
		var previous productioncontrol.Version
		if len(args) == 6 {
			generation, err := strconv.ParseInt(args[4], 10, 64)
			if err != nil || generation < 1 || args[5] == "" {
				return errors.New("invalid participation predecessor")
			}
			previous = productioncontrol.Version{Generation: generation, Digest: args[5]}
		}
		raw, err := readFile(args[3], productioncontrol.MaxParticipationPolicyBytes, false)
		if err != nil {
			return err
		}
		submission, err := productioncontrol.DraftParticipationTopology(args[1], args[2], previous, raw)
		if err != nil {
			return err
		}
		return emit(submission)
	case "store-participation":
		if len(args) != 5 {
			return errors.New(usage)
		}
		entry, err := record(ctx, args[1], func(ctx context.Context, j *productioncontrol.Journal, cluster string) (productioncontrol.Recorded, error) {
			return j.ActivateParticipationTopology(ctx, cluster, args[2], args[3], args[4])
		})
		if err != nil {
			return err
		}
		return emit(entry)
	case "read-participation":
		if len(args) != 2 && len(args) != 3 {
			return errors.New(usage)
		}
		var generation int64
		if len(args) == 3 {
			var err error
			generation, err = strconv.ParseInt(args[2], 10, 64)
			if err != nil || generation < 1 {
				return errors.New("invalid participation topology generation")
			}
		}
		return withJournal(ctx, args[1], true, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			var topology productioncontrol.VerifiedParticipationTopology
			var err error
			if generation == 0 {
				topology, err = j.ReadParticipationTopology(ctx, cluster)
			} else {
				topology, err = j.ReadParticipationTopologyVersion(ctx, cluster, generation)
			}
			if err != nil {
				return err
			}
			return emit(topology)
		})
	case "preview-participation":
		if len(args) != 4 {
			return errors.New(usage)
		}
		raw, err := readFile(args[3], participation.MaxReplayBytes, false)
		if err != nil {
			return err
		}
		return withJournal(ctx, args[1], true, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			topology, err := j.ReadParticipationTopology(ctx, cluster)
			if err != nil {
				return err
			}
			report, err := participation.ReplayParticipation(cluster, args[2], topology, raw, now())
			if err != nil {
				return err
			}
			return emit(report)
		})
	case "issue-participation-observer":
		if len(args) != 5 || !filepath.IsAbs(args[4]) {
			return errors.New(usage)
		}
		return withJournal(ctx, args[1], true, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			topology, err := j.ReadParticipationTopology(ctx, cluster)
			if err != nil {
				return err
			}
			if topology.Policy.Schema != productioncontrol.BoundParticipationPolicySchema {
				return errors.New("participation observer credentials require a cluster-bound topology")
			}
			for _, m := range topology.Policy.Members {
				if m.Node != args[2] {
					continue
				}
				claims, err := platformcontrol.ParticipationObserverClaims(m.Node, m.Group)
				if err != nil {
					return err
				}
				seed, err := readFile(args[4], 4096, true)
				if err != nil {
					return err
				}
				defer clear(seed)
				keys := platformcontrol.DeriveParticipationObserverKeyring(string(seed), args[3])
				token, err := platformcontrol.IssuePlatformComponentIdentity(keys, claims, now(), platformcontrol.ParticipationObserverTTL)
				if err != nil {
					return err
				}
				_, err = fmt.Fprintln(out, token)
				return err
			}
			return errors.New("participation observer target is not enrolled")
		})
	case "observe-participation-readers", "observe-participation-member", "shadow-participation-kubernetes",
		"shadow-participation-dns", "draft-participation-hold", "preflight-participation-hold",
		"observe-participation-freeze", "observe-participation-offline-guard",
		"observe-participation-offline-readback", "observe-participation-rollback-boundary":
		minArgs, maxArgs := 5, 5
		var tokenPath string
		var usesToken bool
		usesSubmission := args[0] == "preflight-participation-hold" || args[0] == "observe-participation-offline-guard" ||
			args[0] == "observe-participation-offline-readback" || args[0] == "observe-participation-rollback-boundary"
		switch args[0] {
		case "shadow-participation-dns":
			minArgs, maxArgs = 6, 6
			if len(args) == 6 {
				tokenPath = args[5]
				usesToken = true
			}
		case "draft-participation-hold":
			minArgs, maxArgs = 6, 7
			if len(args) == 7 {
				tokenPath = args[6]
				usesToken = true
			}
		case "preflight-participation-hold":
			maxArgs = 6
			if len(args) == 6 {
				tokenPath = args[5]
				usesToken = true
			}
		case "observe-participation-offline-guard", "observe-participation-rollback-boundary":
			minArgs, maxArgs = 6, 6
		}
		if len(args) < minArgs || len(args) > maxArgs {
			return errors.New(usage)
		}
		if usesToken && !filepath.IsAbs(tokenPath) {
			return errors.New("participation observation requires an absolute token file path")
		}
		node := args[4]
		var submission productioncontrol.Submission
		if usesSubmission {
			var err error
			submission, err = readSubmission(args[4])
			if err != nil {
				return err
			}
			node, err = participation.HoldTarget(submission)
			if err != nil {
				return err
			}
		}
		kubeConfig, err := readKubeObservationConfig(args[2], args[3])
		if err != nil {
			return err
		}
		return withJournal(ctx, args[1], true, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			if usesSubmission && submission.Intent.ClusterID != cluster {
				return errors.New("participation hold belongs to another journal cluster")
			}
			topology, err := j.ReadParticipationTopology(ctx, cluster)
			if err != nil {
				return err
			}
			observer, err := participation.NewBoundKubernetesObserver(kubeConfig, topology.Policy)
			if err != nil {
				return err
			}
			if err := observer.VerifyNamespace(ctx); err != nil {
				return err
			}
			report := participationObservationReport{JournalCluster: cluster, KubeContext: args[3], Topology: topology.Version}
			switch args[0] {
			case "observe-participation-readers":
				readers, err := observer.ObserveReaderRollouts(ctx, topology.Policy, node)
				if err != nil {
					return err
				}
				report.Kind, report.Readers = "participation_reader_observation", &readers
			case "observe-participation-member":
				member, err := observer.ObserveMemberObjects(ctx, topology.Policy, node)
				if err != nil {
					return err
				}
				report.Kind, report.Member = "participation_member_object_observation", &member
			case "shadow-participation-kubernetes", "shadow-participation-dns", "draft-participation-hold", "preflight-participation-hold":
				var shadow participation.KubernetesShadow
				if tokenPath == "" {
					shadow, err = observer.ShadowKubernetes(ctx, topology.Policy, node, now)
					report.Kind = "participation_kubernetes_only_shadow"
				} else {
					token, readErr := readFile(tokenPath, 4096, true)
					if readErr != nil {
						return readErr
					}
					defer clear(token)
					dns, createErr := participation.NewDNSObserver(topology.Policy.Origin, string(token))
					if createErr != nil {
						return createErr
					}
					shadow, err = observer.ShadowWithDNS(ctx, topology.Policy, node, dns, now)
					report.Kind = "participation_kubernetes_and_fugue_shadow"
				}
				if err != nil {
					return err
				}
				report.Shadow = &shadow
			case "observe-participation-freeze":
				freeze, err := observer.ObserveNodeFreeze(ctx, topology.Policy, node)
				if err != nil {
					return err
				}
				report.Kind, report.Freeze = "participation_freeze_configuration_observation", &freeze
			case "observe-participation-offline-guard":
				guard, err := observer.ObserveOfflineHoldGuard(ctx, topology, submission, args[5])
				if err != nil {
					return err
				}
				report.Kind, report.Freeze = "participation_offline_guard_configuration_observation", &guard
				report.IntentDigest, err = submission.Intent.Digest()
				if err != nil {
					return err
				}
			case "observe-participation-offline-readback":
				readback, err := observer.ObserveOfflineHoldReadback(ctx, topology, submission)
				if err != nil {
					return err
				}
				report.Kind, report.Readback = "participation_offline_hold_readback", &readback
				report.IntentDigest, err = submission.Intent.Digest()
				if err != nil {
					return err
				}
			case "observe-participation-rollback-boundary":
				boundary, err := observer.ObserveOfflineHoldRollbackBoundary(ctx, topology, submission, args[5], now())
				if err != nil {
					return err
				}
				report.Kind, report.RollbackBoundary = "participation_offline_hold_rollback_boundary", &boundary
				report.IntentDigest, err = submission.Intent.Digest()
				if err != nil {
					return err
				}
			}
			if err := observer.VerifyNamespace(ctx); err != nil {
				return err
			}
			latest, err := j.ReadParticipationTopology(ctx, cluster)
			if err != nil || latest.Version != topology.Version || latest.ApprovalDigest != topology.ApprovalDigest {
				return errors.New("approved participation topology could not be revalidated after observation")
			}
			if args[0] == "draft-participation-hold" || args[0] == "preflight-participation-hold" {
				head, err := j.ReadResource(ctx, participation.HoldResource(cluster, node))
				if err != nil {
					return errors.New("participation hold resource has no readable verified journal head")
				}
				if args[0] == "preflight-participation-hold" {
					if err := participation.PreflightHold(submission, latest, head, *report.Shadow, now()); err != nil {
						return err
					}
					report.Kind = "participation_hold_preflight"
					report.IntentDigest, err = submission.Intent.Digest()
					if err != nil {
						return err
					}
					return emit(report)
				}
				draft, err := participation.DraftHold(cluster, args[5], node, latest, head, *report.Shadow)
				if err != nil {
					return err
				}
				return emit(draft)
			}
			return emit(report)
		})
	case "migrate-journal", "migrate-storage-audit-provider":
		if len(args) != 2 {
			return errors.New(usage)
		}
		cfg, _, _, err := productioncontrol.LoadJournalEndpoint(args[1])
		if err != nil {
			return err
		}
		db := stdlib.OpenDB(*cfg)
		defer db.Close()
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		if args[0] == "migrate-storage-audit-provider" {
			return providergate.ApplyStorageAuditPolicySchema(ctx, db)
		}
		return productioncontrol.ApplySchema(ctx, db)
	case "inspect":
		if len(args) != 2 {
			return errors.New(usage)
		}
		s, err := readSubmission(args[1])
		if err != nil {
			return err
		}
		digest, err := s.Intent.Digest()
		if err != nil {
			return err
		}
		var hold json.RawMessage
		var dns *dnscontrol.Payload
		var handoff *productioncontrol.FrontWorkerShadow
		var storageAudit *productioncontrol.StorageAuditShadow
		var registryRehome *releaseguardian.RegistryRehomeReview
		if s.Intent.Operation == productioncontrol.ComponentRegistryMove {
			p, err := releaseguardian.InspectRegistryRehomeSubmission(s)
			if err != nil {
				return err
			}
			registryRehome = &p
		}
		if s.Intent.Operation == productioncontrol.NodeParticipation {
			hold, err = participation.InspectHold(s)
			if err != nil {
				return err
			}
		}
		if s.Intent.Operation == productioncontrol.DNSMembership {
			p, err := dnscontrol.Inspect(s)
			if err != nil {
				return err
			}
			dns = &p
		}
		if s.Intent.Operation == productioncontrol.FrontWorker {
			p, err := productioncontrol.InspectFrontWorker(s)
			if err != nil {
				return err
			}
			handoff = &p
		}
		if s.Intent.ResourceID == productioncontrol.StorageAuditPolicyResource {
			p, err := productioncontrol.InspectStorageAuditPolicy(s)
			if err != nil {
				return err
			}
			storageAudit = &p
		}
		return emit(struct {
			Digest         string                                `json:"digest"`
			Intent         productioncontrol.Intent              `json:"intent"`
			Hold           json.RawMessage                       `json:"hold,omitempty"`
			DNS            *dnscontrol.Payload                   `json:"dns,omitempty"`
			FrontWorker    *productioncontrol.FrontWorkerShadow  `json:"frontWorker,omitempty"`
			StorageAudit   *productioncontrol.StorageAuditShadow `json:"storageAudit,omitempty"`
			RegistryRehome *releaseguardian.RegistryRehomeReview `json:"registryRehome,omitempty"`
		}{digest, s.Intent, hold, dns, handoff, storageAudit, registryRehome})
	case "sign":
		if len(args) != 6 {
			return errors.New(usage)
		}
		s, err := readSubmission(args[1])
		if err != nil {
			return err
		}
		digest, err := s.Intent.Digest()
		if err != nil || digest != args[2] {
			return errors.New("submission differs from the exact reviewed intent digest")
		}
		if s.Intent.Operation == productioncontrol.ComponentRegistryMove {
			if _, err := releaseguardian.InspectRegistryRehomeSubmission(s); err != nil {
				return err
			}
		}
		if s.Intent.Operation == productioncontrol.NodeParticipation {
			if _, err := participation.InspectHold(s); err != nil {
				return err
			}
		}
		if s.Intent.Operation == productioncontrol.DNSMembership {
			if _, err := dnscontrol.Inspect(s); err != nil {
				return err
			}
		}
		if s.Intent.Operation == productioncontrol.FrontWorker {
			if _, err := productioncontrol.InspectFrontWorker(s); err != nil {
				return err
			}
		}
		if s.Intent.ResourceID == productioncontrol.StorageAuditPolicyResource {
			if _, err := productioncontrol.InspectStorageAuditPolicy(s); err != nil {
				return err
			}
		}
		ttl, err := time.ParseDuration(args[5])
		if err != nil || ttl < time.Second || ttl > productioncontrol.MaxApprovalTTL {
			return errors.New("invalid production approval lifetime")
		}
		key, err := readSigningKey(args[4])
		if err != nil {
			return err
		}
		defer clear(key)
		approval, err := productioncontrol.SignApproval(s.Intent, args[3], key, now(), ttl)
		if err != nil {
			return err
		}
		return emit(approval)
	case "record-intent":
		if len(args) != 4 {
			return errors.New(usage)
		}
		s, err := readSubmission(args[3])
		if err != nil {
			return err
		}
		if s.Intent.Operation == productioncontrol.ComponentRegistryMove {
			if _, err := releaseguardian.InspectRegistryRehomeSubmission(s); err != nil {
				return err
			}
		}
		record, err := record(ctx, args[1], func(ctx context.Context, j *productioncontrol.Journal, cluster string) (productioncontrol.Recorded, error) {
			if s.Intent.ClusterID != cluster {
				return productioncontrol.Recorded{}, errors.New("submission belongs to a different journal cluster")
			}
			return j.RecordIntent(ctx, args[2], s.Intent, s.Payload)
		})
		if err != nil {
			return err
		}
		return emit(record)
	case "record-approval":
		if len(args) != 3 {
			return errors.New(usage)
		}
		raw, err := readFile(args[2], 4096, false)
		if err != nil {
			return err
		}
		approval, err := productioncontrol.DecodeApproval(raw)
		if err != nil {
			return err
		}
		record, err := record(ctx, args[1], func(ctx context.Context, j *productioncontrol.Journal, _ string) (productioncontrol.Recorded, error) {
			return j.RecordApproval(ctx, approval)
		})
		if err != nil {
			return err
		}
		return emit(record)
	default:
		return errors.New(usage)
	}
}

func emitDNSShadow(origin string, o dnscontrol.Observation, now func() time.Time, emit func(any) error) error {
	change, policy, err := dnscontrol.ValidateSources(o.Sources, now())
	if err != nil {
		return err
	}
	shadow, err := dnscontrol.ValidatePreview(o.Before, o.After, o.Sources, policy, now())
	if err != nil {
		return err
	}
	change.Origin, change.ShadowDigest = origin, shadow.Digest
	return emit(struct {
		ExecutionEvidence bool               `json:"executionEvidence"`
		Change            dnscontrol.Payload `json:"change"`
		Shadow            dnscontrol.Shadow  `json:"shadow"`
	}{false, change, shadow})
}

func readKubeObservationConfig(path, context string) (*rest.Config, error) {
	if !filepath.IsAbs(path) || context == "" {
		return nil, errors.New("provider observation requires an absolute kubeconfig path and explicit context")
	}
	loader := &clientcmd.ClientConfigLoadingRules{ExplicitPath: path}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: context}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
	if err != nil {
		return nil, errors.New("could not load the requested read-only Kubernetes context")
	}
	return cfg, nil
}

type participationObservationReport struct {
	Kind              string                                     `json:"kind"`
	ExecutionEvidence bool                                       `json:"executionEvidence"`
	JournalCluster    string                                     `json:"journalCluster"`
	KubeContext       string                                     `json:"kubeContext"`
	Topology          productioncontrol.Version                  `json:"topology"`
	IntentDigest      string                                     `json:"intentDigest,omitempty"`
	Readers           *participation.ReaderObservation           `json:"readers,omitempty"`
	Member            *participation.MemberObservation           `json:"member,omitempty"`
	Shadow            *participation.KubernetesShadow            `json:"shadow,omitempty"`
	Freeze            *participation.FreezeObservation           `json:"freeze,omitempty"`
	Readback          *participation.OfflineHoldReadback         `json:"readback,omitempty"`
	RollbackBoundary  *participation.OfflineHoldRollbackBoundary `json:"rollbackBoundary,omitempty"`
}

func record(ctx context.Context, path string, fn func(context.Context, *productioncontrol.Journal, string) (productioncontrol.Recorded, error)) (productioncontrol.Recorded, error) {
	var entry productioncontrol.Recorded
	err := withJournal(ctx, path, false, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
		var err error
		entry, err = fn(ctx, j, cluster)
		return err
	})
	return entry, err
}

func withJournal(ctx context.Context, path string, readOnly bool, fn func(context.Context, *productioncontrol.Journal, string) error) error {
	return withJournalDeadline(ctx, path, readOnly, 15*time.Second, fn)
}

func withJournalDeadline(ctx context.Context, path string, readOnly bool, timeout time.Duration, fn func(context.Context, *productioncontrol.Journal, string) error) error {
	cfg, keys, cluster, err := productioncontrol.LoadJournalEndpoint(path)
	if err != nil {
		return err
	}
	if readOnly {
		if cfg.RuntimeParams == nil {
			cfg.RuntimeParams = make(map[string]string)
		}
		cfg.RuntimeParams["default_transaction_read_only"] = "on"
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	db.SetMaxOpenConns(1)
	j, err := productioncontrol.NewJournal(db, keys)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return errors.New("production journal is unavailable")
	}
	return fn(ctx, j, cluster)
}

func readSubmission(path string) (productioncontrol.Submission, error) {
	raw, err := readFile(path, productioncontrol.MaxSubmissionBytes, false)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	return productioncontrol.DecodeSubmission(raw)
}

func readSigningKey(path string) (ed25519.PrivateKey, error) {
	raw, err := readFile(path, 4096, true)
	if err != nil {
		return nil, errors.New("signing key must be a private owner-only regular file")
	}
	defer clear(raw)
	block, remainder := pem.Decode(raw)
	if block == nil || block.Type != "PRIVATE KEY" || len(block.Headers) != 0 || len(bytes.TrimSpace(remainder)) != 0 {
		return nil, errors.New("signing key is not one PKCS#8 PEM private key")
	}
	defer clear(block.Bytes)
	value, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	key, ok := value.(ed25519.PrivateKey)
	if err != nil || !ok {
		return nil, errors.New("signing key is not Ed25519")
	}
	return key, nil
}

func readFile(path string, limit int64, private bool) ([]byte, error) {
	valid := func(info os.FileInfo) bool {
		return info.Mode().IsRegular() && info.Size() > 0 && info.Size() <= limit && (!private || info.Mode().Perm()&0o077 == 0)
	}
	info, err := os.Lstat(path)
	if err != nil || !valid(info) {
		return nil, errors.New("production control input file is unavailable or invalid")
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("could not open production control input file")
	}
	defer f.Close()
	actual, err := f.Stat()
	if err != nil || !valid(actual) || !os.SameFile(info, actual) {
		return nil, errors.New("production control input file changed during open")
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || len(raw) == 0 || int64(len(raw)) > limit {
		if private {
			clear(raw)
		}
		return nil, errors.New("could not read bounded production control input file")
	}
	return raw, nil
}
