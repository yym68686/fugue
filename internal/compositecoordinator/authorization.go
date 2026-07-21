package compositecoordinator

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"io"
	"strconv"

	"fugue/internal/releasecontract"
)

var ErrInvalidAuthorization = fmt.Errorf("invalid composite no-op authorization")

// NoopAuthorization is an opaque, immutable proof that one exact prepared
// record passed the v2 envelope boundary. It intentionally exposes no plan,
// adapter, transition, execution, or write capability.
type NoopAuthorization struct {
	recordID       string
	recordDigest   string
	recordRevision int64
	planDigest     string
	envelopeDigest string
	generation     string
	fencingEpoch   string
	seal           [sha256.Size]byte
}

func (authorization NoopAuthorization) Mode() string {
	return releasecontract.CompositeTransactionModeNoop
}
func (authorization NoopAuthorization) RecordID() string       { return authorization.recordID }
func (authorization NoopAuthorization) RecordRevision() int64  { return authorization.recordRevision }
func (authorization NoopAuthorization) PlanDigest() string     { return authorization.planDigest }
func (authorization NoopAuthorization) EnvelopeDigest() string { return authorization.envelopeDigest }

// Verify proves the authorization still matches the exact prepared record.
// It does not authorize a state transition or any production write.
func (authorization NoopAuthorization) Verify(record Record) error {
	if VerifyRecord(record) != nil || record.State != StatePrepared ||
		authorization.recordID != record.ID || authorization.recordDigest != record.Digest ||
		authorization.recordRevision != record.Revision || authorization.planDigest != record.Plan.Digest ||
		authorization.generation != record.Plan.Generation || authorization.fencingEpoch != record.Plan.FencingEpoch ||
		authorization.envelopeDigest == "" {
		return ErrInvalidAuthorization
	}
	expected := noopAuthorizationSeal(authorization)
	if subtle.ConstantTimeCompare(authorization.seal[:], expected[:]) != 1 {
		return ErrInvalidAuthorization
	}
	return nil
}

// DecodeAndAuthorizeNoop is the only constructor. The trusted binding is
// derived from the durable record, never from caller-supplied hints.
func DecodeAndAuthorizeNoop(record Record, reader io.Reader) (NoopAuthorization, error) {
	if VerifyRecord(record) != nil || record.State != StatePrepared {
		return NoopAuthorization{}, ErrInvalidAuthorization
	}
	binding := releasecontract.CompositeTransactionBindingForRecord(
		record.ID, record.Digest, record.Revision, record.Plan,
	)
	envelope, err := releasecontract.DecodeAndVerifyCompositeTransactionEnvelope(reader, binding)
	if err != nil {
		return NoopAuthorization{}, fmt.Errorf("%w: %v", ErrInvalidAuthorization, err)
	}
	authorization := NoopAuthorization{
		recordID: record.ID, recordDigest: record.Digest, recordRevision: record.Revision,
		planDigest: record.Plan.Digest, envelopeDigest: envelope.Digest,
		generation: record.Plan.Generation, fencingEpoch: record.Plan.FencingEpoch,
	}
	authorization.seal = noopAuthorizationSeal(authorization)
	if err := authorization.Verify(record); err != nil {
		return NoopAuthorization{}, err
	}
	return authorization, nil
}

func noopAuthorizationSeal(authorization NoopAuthorization) [sha256.Size]byte {
	return sha256.Sum256([]byte(
		"fugue-composite-noop-authorization-v1\x00" +
			authorization.recordID + "\x00" + authorization.recordDigest + "\x00" +
			strconv.FormatInt(authorization.recordRevision, 10) + "\x00" +
			authorization.planDigest + "\x00" + authorization.envelopeDigest + "\x00" +
			authorization.generation + "\x00" + authorization.fencingEpoch,
	))
}
