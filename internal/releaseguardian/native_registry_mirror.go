package releaseguardian

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// RegistryRecoveryArtifact pairs a sealed historical artifact receipt with an
// independently provisioned recovery repository. It never rewrites a receipt.
type RegistryRecoveryArtifact struct {
	Source                declarativerelease.ArtifactReceipt
	SourceReceiptDigest   string
	DestinationRepository string
}

type RegistryRecoveryReadback struct {
	SourceReceiptDigest string                                  `json:"sourceReceiptDigest"`
	SourceImage         string                                  `json:"sourceImage"`
	RecoveryPin         string                                  `json:"recoveryPin"`
	RecoveryImage       declarativerelease.RegistryVerification `json:"recoveryImage"`
}

type RegistryRecoveryReport struct {
	Forward  RegistryRecoveryReadback `json:"forward"`
	Rollback RegistryRecoveryReadback `json:"rollback"`
}

// VerifyRecoveryMirror reads both exact images and their deterministic
// recovery tags without contacting the original registry. A successful
// readback is not permission to rewrite the live image, proof of node pull,
// or a guarantee that the registry will protect either tag from deletion.
func (r *IndependentRegistry) VerifyRecoveryMirror(ctx context.Context, forward, rollback RegistryRecoveryArtifact) (RegistryRecoveryReport, error) {
	if r == nil || r.transport == nil {
		return RegistryRecoveryReport{}, errors.New("native recovery registry is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, nativeRegistryDeadline)
	defer cancel()
	a, err := r.verifyRecoveryMirror(ctx, forward)
	if err != nil {
		return RegistryRecoveryReport{}, fmt.Errorf("verify forward recovery mirror: %w", err)
	}
	b, err := r.verifyRecoveryMirror(ctx, rollback)
	if err != nil {
		return RegistryRecoveryReport{}, fmt.Errorf("verify rollback recovery mirror: %w", err)
	}
	return RegistryRecoveryReport{Forward: a, Rollback: b}, nil
}

func (r *IndependentRegistry) verifyRecoveryMirror(ctx context.Context, input RegistryRecoveryArtifact) (RegistryRecoveryReadback, error) {
	sourceBytes, err := declarativerelease.CanonicalJSON(input.Source)
	if err != nil {
		return RegistryRecoveryReadback{}, err
	}
	source, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(sourceBytes))
	if err != nil || source.ReceiptDigest != input.SourceReceiptDigest {
		return RegistryRecoveryReadback{}, errors.New("recovery requires the independently pinned sealed source receipt")
	}
	repo, err := name.NewRepository(input.DestinationRepository, name.StrictValidation)
	if err != nil || repo.Name() != input.DestinationRepository || repo.RegistryStr() != r.host {
		return RegistryRecoveryReadback{}, errors.New("recovery repository is outside the configured independent registry")
	}
	pin := repo.Tag("fugue-recovery-sha256-" + source.TopDigest[len("sha256:"):])
	check := func() error {
		puller, err := remote.NewPuller(remote.WithAuth(r.auth), remote.WithTransport(r.transport))
		if err != nil {
			return err
		}
		top, err := puller.Get(ctx, pin)
		if err != nil {
			return fmt.Errorf("read independent recovery pin: %w", err)
		}
		if len(top.Manifest) == 0 || len(top.Manifest) > maxNativeManifestBytes || digest(top.Manifest) != source.TopDigest {
			return errors.New("recovery pin differs from the sealed source digest")
		}
		return nil
	}
	if err := check(); err != nil {
		return RegistryRecoveryReadback{}, err
	}
	// The projection is only passed to the blob verifier; it is never encoded
	// as a new receipt or treated as a new sealed release.
	projection := source
	projection.ImmutableRef = repo.Digest(source.TopDigest).Name()
	projection.Repository = repo.Name()
	readback, err := r.VerifyRuntimeArtifact(ctx, projection)
	if err != nil {
		return RegistryRecoveryReadback{}, err
	}
	if err := check(); err != nil {
		return RegistryRecoveryReadback{}, err
	}
	return RegistryRecoveryReadback{
		SourceReceiptDigest: source.ReceiptDigest, SourceImage: source.ImmutableRef,
		RecoveryPin: pin.Name(), RecoveryImage: readback,
	}, nil
}
