// fugue-production-control records reviewable intent and independently signed
// approval. It has no provider client, dispatcher, or automatic trigger.
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

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/participation"
	"github.com/jackc/pgx/v5/stdlib"
	"k8s.io/client-go/tools/clientcmd"
)

const usage = `usage: fugue-production-control inspect SUBMISSION
       fugue-production-control sign SUBMISSION EXPECTED_DIGEST KEY_ID PRIVATE_PKCS8_PEM TTL
       fugue-production-control record-intent JOURNAL_CONFIG ACTOR SUBMISSION
       fugue-production-control record-approval JOURNAL_CONFIG APPROVAL
       fugue-production-control draft-participation CLUSTER REQUEST POLICY [PREVIOUS_GENERATION PREVIOUS_DIGEST]
       fugue-production-control store-participation JOURNAL_CONFIG ACTOR INTENT_DIGEST APPROVAL_DIGEST
       fugue-production-control read-participation JOURNAL_CONFIG [GENERATION]
       fugue-production-control preview-participation JOURNAL_CONFIG NODE SNAPSHOT
       fugue-production-control observe-participation-readers JOURNAL_CONFIG KUBECONFIG CONTEXT NODE
       fugue-production-control observe-participation-member JOURNAL_CONFIG KUBECONFIG CONTEXT NODE
       fugue-production-control shadow-participation-kubernetes JOURNAL_CONFIG KUBECONFIG CONTEXT NODE
       fugue-production-control migrate-journal JOURNAL_OWNER_CONFIG`

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
	case "observe-participation-readers", "observe-participation-member", "shadow-participation-kubernetes":
		if len(args) != 5 {
			return errors.New(usage)
		}
		if !filepath.IsAbs(args[2]) || args[3] == "" {
			return errors.New("participation observation requires an absolute kubeconfig path and explicit context")
		}
		loader := &clientcmd.ClientConfigLoadingRules{ExplicitPath: args[2]}
		overrides := &clientcmd.ConfigOverrides{CurrentContext: args[3]}
		kubeConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
		if err != nil {
			return errors.New("could not load the requested read-only Kubernetes context")
		}
		observer, err := participation.NewKubernetesObserver(kubeConfig)
		if err != nil {
			return err
		}
		return withJournal(ctx, args[1], true, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			topology, err := j.ReadParticipationTopology(ctx, cluster)
			if err != nil {
				return err
			}
			report := participationObservationReport{JournalCluster: cluster, KubeContext: args[3], Topology: topology.Version}
			switch args[0] {
			case "observe-participation-readers":
				readers, err := observer.ObserveReaderRollouts(ctx, topology.Policy, args[4])
				if err != nil {
					return err
				}
				report.Kind, report.Readers = "participation_reader_observation", &readers
			case "observe-participation-member":
				member, err := observer.ObserveMemberObjects(ctx, topology.Policy, args[4])
				if err != nil {
					return err
				}
				report.Kind, report.Member = "participation_member_object_observation", &member
			case "shadow-participation-kubernetes":
				shadow, err := observer.ShadowKubernetes(ctx, topology.Policy, args[4], now)
				if err != nil {
					return err
				}
				latest, err := j.ReadParticipationTopology(ctx, cluster)
				if err != nil || latest.Version != topology.Version || latest.ApprovalDigest != topology.ApprovalDigest {
					return errors.New("approved participation topology could not be revalidated after observation")
				}
				report.Kind, report.Shadow = "participation_kubernetes_only_shadow", &shadow
			}
			return emit(report)
		})
	case "migrate-journal":
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
		return emit(struct {
			Digest string                   `json:"digest"`
			Intent productioncontrol.Intent `json:"intent"`
		}{digest, s.Intent})
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

type participationObservationReport struct {
	Kind              string                           `json:"kind"`
	ExecutionEvidence bool                             `json:"executionEvidence"`
	JournalCluster    string                           `json:"journalCluster"`
	KubeContext       string                           `json:"kubeContext"`
	Topology          productioncontrol.Version        `json:"topology"`
	Readers           *participation.ReaderObservation `json:"readers,omitempty"`
	Member            *participation.MemberObservation `json:"member,omitempty"`
	Shadow            *participation.KubernetesShadow  `json:"shadow,omitempty"`
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
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
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
		return nil, errors.New("approval key must be a private owner-only regular file")
	}
	defer clear(raw)
	block, remainder := pem.Decode(raw)
	if block == nil || block.Type != "PRIVATE KEY" || len(block.Headers) != 0 || len(bytes.TrimSpace(remainder)) != 0 {
		return nil, errors.New("approval key is not one PKCS#8 PEM private key")
	}
	defer clear(block.Bytes)
	value, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	key, ok := value.(ed25519.PrivateKey)
	if err != nil || !ok {
		return nil, errors.New("approval key is not Ed25519")
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
