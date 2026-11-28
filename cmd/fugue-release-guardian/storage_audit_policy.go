package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/storageauditcontrol"
	"github.com/jackc/pgx/v5/stdlib"
	"k8s.io/client-go/tools/clientcmd"
)

const storageAuditPolicyDispatchUsage = "usage: fugue-release-guardian storage-audit-policy-dispatch JOURNAL_CONFIG KUBECONFIG CONTEXT INTENT_DIGEST APPROVAL_DIGEST"
const storageAuditPolicySettlementUsage = "usage: fugue-release-guardian storage-audit-policy-checkpoint CONFIG INTENT_DIGEST or storage-audit-policy-settle CONFIG INTENT_DIGEST WITNESS_DIGEST"

func runStorageAuditPolicyDispatch(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 5 || !filepath.IsAbs(args[0]) || !filepath.IsAbs(args[1]) || args[2] == "" {
		return errors.New(storageAuditPolicyDispatchUsage)
	}
	cfg, owners, cluster, err := productioncontrol.LoadJournalEndpoint(args[0])
	if err != nil {
		return err
	}
	kube, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: args[1]},
		&clientcmd.ConfigOverrides{CurrentContext: args[2]}).ClientConfig()
	if err != nil {
		return errors.New("could not load the storage audit projection reader")
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	journal, err := productioncontrol.NewJournal(db, owners)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	claim, created, err := journal.DispatchStorageAuditPolicy(ctx, cluster, "storage-audit-policy-writer", args[3], args[4],
		func(ctx context.Context, shadow productioncontrol.StorageAuditShadow) error {
			client, err := storageauditcontrol.NewReader(kube, shadow.Binding)
			if err != nil {
				return err
			}
			check, cancel := context.WithTimeout(ctx, 4*time.Second)
			defer cancel()
			return storageauditcontrol.VerifyRetiredProjection(check, client, shadow)
		})
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(struct {
		Claim   productioncontrol.Claim `json:"claim"`
		Created bool                    `json:"created"`
	}{claim, created})
}

type storageAuditPolicyObserverSettings struct {
	JournalFile       string            `json:"journalFile"`
	WitnessDirectory  string            `json:"witnessDirectory"`
	ObserverKeyID     string            `json:"observerKeyId"`
	ObserverPublicKey ed25519.PublicKey `json:"observerPublicKey"`
}

func runStorageAuditPolicySettlement(ctx context.Context, settle bool, args []string, out io.Writer) error {
	if (!settle && len(args) != 2) || (settle && len(args) != 3) || !exactSHA256Digest(args[1]) ||
		(settle && !exactSHA256Digest(args[2])) {
		return errors.New(storageAuditPolicySettlementUsage)
	}
	raw, err := readOfflineDispatchInput(args[0], 16<<10, false)
	if err != nil {
		return err
	}
	var settings storageAuditPolicyObserverSettings
	if err := decodeStrictJSON(raw, &settings); err != nil || !filepath.IsAbs(settings.JournalFile) || settings.ObserverKeyID == "" ||
		len(settings.ObserverPublicKey) != ed25519.PublicKeySize || (settle && !filepath.IsAbs(settings.WitnessDirectory)) {
		return errors.New("storage audit settlement requires a dedicated journal profile and a configured technical observer")
	}
	cfg, owners, cluster, err := productioncontrol.LoadJournalEndpoint(settings.JournalFile)
	if err != nil {
		return err
	}
	if !settle {
		if cfg.RuntimeParams == nil {
			cfg.RuntimeParams = make(map[string]string)
		}
		cfg.RuntimeParams["default_transaction_read_only"] = "on"
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	journal, err := productioncontrol.NewJournal(db, owners)
	if err != nil {
		return err
	}
	if err := journal.VerifyIndependentObserverKey(settings.ObserverPublicKey); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	scope := productioncontrol.Scope{ClusterID: cluster, ResourceID: productioncontrol.StorageAuditPolicyResource}
	claim, terminal, err := journal.ReadExecution(ctx, scope, args[1])
	if err != nil {
		return err
	}
	emit := json.NewEncoder(out).Encode
	if terminal != nil {
		if settle && terminal.EvidenceDigest != args[2] {
			return productioncontrol.ErrResultConflict
		}
		return emit(terminal)
	}
	live, err := journal.ReadStorageAuditPolicyTransition(ctx, claim)
	if err != nil {
		return err
	}
	if !settle {
		return emit(productioncontrol.ExecutionWitness{
			Kind: productioncontrol.TerminalWitness, KeyID: settings.ObserverKeyID, ClusterID: cluster, ResourceID: scope.ResourceID,
			Generation: live.Verified.Generation, VerifiedDigest: live.Verified.Digest,
			ObservationDigest: live.ObservationDigest, AuthorityDigest: live.AuthorityDigest, ObservedAt: time.Now().Unix(),
			IntentDigest: claim.IntentDigest, ApprovalDigest: claim.ApprovalDigest, Fence: claim.Fence, Outcome: productioncontrol.Applied,
		})
	}
	root, err := os.OpenRoot(settings.WitnessDirectory)
	if err != nil {
		return err
	}
	defer root.Close()
	execution, err := productioncontrol.NewStorageAuditPolicyExecutionJournal(journal, scope,
		nodeWorkerWitnessDirectory{root: root},
		map[string]productioncontrol.TrustedWitness{settings.ObserverKeyID: {Scope: scope, PublicKey: settings.ObserverPublicKey}})
	if err != nil {
		return err
	}
	result := productioncontrol.ExecutionResult{Outcome: productioncontrol.Applied, Verified: live.Verified, EvidenceDigest: args[2]}
	if _, err := execution.CompleteExecution(ctx, "storage-audit-policy-observer", claim, result); err != nil {
		return err
	}
	return emit(result)
}
