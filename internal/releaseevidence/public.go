// Package releaseevidence defines the bounded, secret-free evidence that may
// leave the private release workspace. The public schema deliberately has no
// free-text fields: paths, commands, bindings, classifier reasons, manifests,
// and errors cannot be represented by this package.
package releaseevidence

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"fugue/internal/releasedomain"

	"golang.org/x/sys/unix"
)

const (
	PublicAPIVersion = "release-domain-public-evidence.fugue.dev/v1"
	PublicKind       = "ReleaseDomainPublicEvidence"

	maxPublicArtifactBytes = 32 << 10
	maxPublicTraceEvents   = 64
	atomicCreateAttempts   = 128
)

// Outcome is the only planner result that public release evidence can record.
// GenesisZero identifies the no-release baseline without inventing a Helm
// revision or treating it as an executable single-domain transaction.
type Outcome string

const (
	OutcomeZero        Outcome = "zero"
	OutcomeSingle      Outcome = "single"
	OutcomeMultiple    Outcome = "multiple"
	OutcomeUnknown     Outcome = "unknown"
	OutcomeGenesisZero Outcome = "genesis-zero"
)

// TracePhase mirrors the fixed transaction phases without exposing commands.
type TracePhase string

const (
	TracePhaseTransaction TracePhase = "transaction"
	TracePhasePrepare     TracePhase = "prepare"
	TracePhaseApply       TracePhase = "apply"
	TracePhaseVerify      TracePhase = "verify"
	TracePhaseRollback    TracePhase = "rollback"
)

// TraceState is one fixed transaction state. It cannot carry an error string.
type TraceState string

const (
	TraceStateStarted   TraceState = "started"
	TraceStateSucceeded TraceState = "succeeded"
	TraceStateFailed    TraceState = "failed"
)

// TraceEvent is a deliberately minimal public projection of a private trace.
type TraceEvent struct {
	Phase TracePhase `json:"phase"`
	State TraceState `json:"state"`
}

// PublicArtifact is the complete public evidence schema. Every string is
// either a fixed identity, a fixed enum, an exact Git object ID, or a digest.
// Domain and ReverseDomain are the only optional fields.
type PublicArtifact struct {
	APIVersion            string               `json:"apiVersion"`
	Kind                  string               `json:"kind"`
	RunID                 string               `json:"runId"`
	RunAttempt            uint64               `json:"runAttempt"`
	HeadSHA               string               `json:"headSHA"`
	HelmBaseRevision      uint64               `json:"helmBaseRevision"`
	HelmTargetRevision    uint64               `json:"helmTargetRevision"`
	Outcome               Outcome              `json:"outcome"`
	Domain                releasedomain.Domain `json:"domain,omitempty"`
	PlanDigest            string               `json:"planDigest"`
	ChangedEvidenceDigest string               `json:"changedEvidenceDigest"`
	BaseDigest            string               `json:"baseDigest"`
	TargetDigest          string               `json:"targetDigest"`
	RepeatDigest          string               `json:"repeatDigest"`
	OwnershipDigest       string               `json:"ownershipDigest"`
	ContextDigest         string               `json:"contextDigest"`
	ForwardDigest         string               `json:"forwardDigest"`
	ReverseDigest         string               `json:"reverseDigest"`
	ReverseAuthorized     bool                 `json:"reverseAuthorized"`
	ReverseDomain         releasedomain.Domain `json:"reverseDomain,omitempty"`
	ReverseObjectCount    uint64               `json:"reverseObjectCount"`
	Trace                 []TraceEvent         `json:"trace"`
	WriteBoundaryCrossed  bool                 `json:"writeBoundaryCrossed"`
	RollbackAttempted     bool                 `json:"rollbackAttempted"`
	RollbackCompleted     bool                 `json:"rollbackCompleted"`
	RollbackFailed        bool                 `json:"rollbackFailed"`
}

// Validate enforces the complete schema and its cross-field safety
// invariants. Validation errors never interpolate caller-controlled values.
func Validate(artifact PublicArtifact) error {
	if artifact.APIVersion != PublicAPIVersion || artifact.Kind != PublicKind {
		return fmt.Errorf("public evidence identity is unsupported")
	}
	if !isCanonicalPositiveDecimal(artifact.RunID) {
		return fmt.Errorf("public evidence runId must be a canonical positive decimal string")
	}
	if artifact.RunAttempt == 0 {
		return fmt.Errorf("public evidence runAttempt must be positive")
	}
	if !isLowerHex(artifact.HeadSHA, 40) {
		return fmt.Errorf("public evidence headSHA must be exact lowercase 40-hex")
	}

	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "planDigest", value: artifact.PlanDigest},
		{name: "changedEvidenceDigest", value: artifact.ChangedEvidenceDigest},
		{name: "baseDigest", value: artifact.BaseDigest},
		{name: "targetDigest", value: artifact.TargetDigest},
		{name: "repeatDigest", value: artifact.RepeatDigest},
		{name: "ownershipDigest", value: artifact.OwnershipDigest},
		{name: "contextDigest", value: artifact.ContextDigest},
		{name: "forwardDigest", value: artifact.ForwardDigest},
		{name: "reverseDigest", value: artifact.ReverseDigest},
	} {
		if !isCanonicalSHA256(field.value) {
			return fmt.Errorf("public evidence %s must be lowercase sha256:<64-hex>", field.name)
		}
	}

	switch artifact.Outcome {
	case OutcomeGenesisZero:
		if artifact.HelmBaseRevision != 0 || artifact.HelmTargetRevision != 0 {
			return fmt.Errorf("genesis-zero evidence must use Helm revisions 0 and 0")
		}
	case OutcomeZero, OutcomeMultiple, OutcomeUnknown:
		if artifact.HelmBaseRevision == 0 || artifact.HelmTargetRevision != artifact.HelmBaseRevision {
			return fmt.Errorf("non-writing evidence must bind one existing Helm revision")
		}
	case OutcomeSingle:
		if artifact.HelmBaseRevision == 0 || artifact.HelmTargetRevision == 0 || artifact.HelmTargetRevision != artifact.HelmBaseRevision+1 {
			return fmt.Errorf("single evidence must bind consecutive Helm revisions")
		}
	default:
		return fmt.Errorf("public evidence outcome is unsupported")
	}

	if artifact.Outcome == OutcomeSingle {
		if !isKnownDomain(artifact.Domain) {
			return fmt.Errorf("single evidence must contain one fixed domain")
		}
	} else if artifact.Domain != "" {
		return fmt.Errorf("non-single evidence must omit domain")
	}

	if artifact.Outcome != OutcomeUnknown && artifact.TargetDigest != artifact.RepeatDigest {
		return fmt.Errorf("non-unknown evidence requires identical target and repeat digests")
	}
	if artifact.ReverseAuthorized {
		if artifact.Outcome != OutcomeSingle || artifact.ReverseDomain != artifact.Domain || !isKnownDomain(artifact.ReverseDomain) {
			return fmt.Errorf("reverse authorization must bind the selected fixed domain")
		}
		if artifact.ReverseObjectCount == 0 {
			return fmt.Errorf("reverse authorization requires a positive object count")
		}
		if artifact.TargetDigest != artifact.RepeatDigest || artifact.ForwardDigest != artifact.ReverseDigest {
			return fmt.Errorf("reverse authorization digests do not prove deterministic symmetric ownership")
		}
	} else if artifact.ReverseDomain != "" || artifact.ReverseObjectCount != 0 {
		return fmt.Errorf("evidence without reverse authorization must omit reverse domain and object count")
	}

	if artifact.WriteBoundaryCrossed && (artifact.Outcome != OutcomeSingle || !artifact.ReverseAuthorized) {
		return fmt.Errorf("write boundary requires single outcome and reverse authorization")
	}
	if artifact.RollbackCompleted && artifact.RollbackFailed {
		return fmt.Errorf("rollback cannot be both completed and failed")
	}
	if (artifact.RollbackCompleted || artifact.RollbackFailed) && !artifact.RollbackAttempted {
		return fmt.Errorf("rollback result requires an attempted rollback")
	}
	if artifact.RollbackAttempted && !artifact.WriteBoundaryCrossed {
		return fmt.Errorf("rollback attempt requires a crossed write boundary")
	}

	if err := validateTrace(artifact); err != nil {
		return err
	}
	return nil
}

// Decode reads one bounded JSON document, rejects ambiguous JSON forms, and
// validates the complete public evidence contract.
func Decode(reader io.Reader) (PublicArtifact, error) {
	if isNilReader(reader) {
		return PublicArtifact{}, fmt.Errorf("public evidence reader is nil")
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxPublicArtifactBytes+1))
	if err != nil {
		return PublicArtifact{}, fmt.Errorf("read public evidence: %w", err)
	}
	if len(data) > maxPublicArtifactBytes {
		return PublicArtifact{}, fmt.Errorf("public evidence exceeds %d-byte limit", maxPublicArtifactBytes)
	}
	if !utf8.Valid(data) {
		return PublicArtifact{}, fmt.Errorf("decode public evidence: input contains invalid UTF-8")
	}
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return PublicArtifact{}, fmt.Errorf("decode public evidence: %w", err)
	}
	if err := validateStrictPublicJSON(data); err != nil {
		return PublicArtifact{}, fmt.Errorf("decode public evidence: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var artifact PublicArtifact
	if err := decoder.Decode(&artifact); err != nil {
		return PublicArtifact{}, fmt.Errorf("decode public evidence: %w", err)
	}
	if err := Validate(artifact); err != nil {
		return PublicArtifact{}, err
	}
	return artifact, nil
}

// WritePrivateAtomic validates and atomically publishes one exact-0600 public
// artifact. The destination is never followed if it is a symlink, and both
// the file and containing directory are fsynced before success is reported.
// Callers must expose or upload the path only after this function succeeds.
// As with the process-private release workspace itself, another hostile
// process running under the same effective UID is outside the trust boundary:
// such a process can inspect or mutate the caller directly on supported hosts.
func WritePrivateAtomic(filename string, artifact PublicArtifact) error {
	if err := Validate(artifact); err != nil {
		return err
	}
	encoded, err := json.Marshal(artifact)
	if err != nil {
		return fmt.Errorf("encode public evidence: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > maxPublicArtifactBytes {
		return fmt.Errorf("encoded public evidence exceeds %d-byte limit", maxPublicArtifactBytes)
	}
	output, err := createAtomicOutput(filename)
	if err != nil {
		return err
	}
	publishErr := output.publish(encoded)
	closeErr := output.close()
	return errors.Join(publishErr, closeErr)
}

func validateTrace(artifact PublicArtifact) error {
	if artifact.Trace == nil {
		return fmt.Errorf("public evidence trace must be a non-null array")
	}
	if len(artifact.Trace) > maxPublicTraceEvents {
		return fmt.Errorf("public evidence trace exceeds %d events", maxPublicTraceEvents)
	}
	if len(artifact.Trace) == 0 {
		if artifact.WriteBoundaryCrossed || artifact.RollbackAttempted || artifact.RollbackCompleted || artifact.RollbackFailed {
			return fmt.Errorf("write or rollback evidence requires a fixed trace")
		}
		return nil
	}
	if artifact.Outcome != OutcomeSingle {
		return fmt.Errorf("only single evidence may contain a transaction trace")
	}
	if artifact.Trace[0] != (TraceEvent{Phase: TracePhaseTransaction, State: TraceStateStarted}) {
		return fmt.Errorf("public evidence trace must begin with transaction started")
	}

	type phaseProgress struct {
		started   bool
		terminal  bool
		succeeded bool
		failed    bool
	}
	progress := map[TracePhase]*phaseProgress{
		TracePhaseTransaction: {},
		TracePhasePrepare:     {},
		TracePhaseApply:       {},
		TracePhaseVerify:      {},
		TracePhaseRollback:    {},
	}
	rollbackStarted := false
	for index, event := range artifact.Trace {
		phase, known := progress[event.Phase]
		if !known || !isKnownTraceState(event.State) {
			return fmt.Errorf("public evidence trace contains an unsupported phase or state")
		}
		transaction := progress[TracePhaseTransaction]
		if transaction.terminal {
			return fmt.Errorf("public evidence trace contains events after transaction terminal state")
		}
		if event.Phase == TracePhaseTransaction {
			switch event.State {
			case TraceStateStarted:
				if index != 0 || phase.started {
					return fmt.Errorf("public evidence trace repeats transaction started")
				}
				phase.started = true
			case TraceStateSucceeded:
				if !phase.started || !progress[TracePhaseVerify].succeeded || rollbackStarted {
					return fmt.Errorf("transaction success requires successful verification and no rollback")
				}
				phase.terminal, phase.succeeded = true, true
			case TraceStateFailed:
				if !phase.started {
					return fmt.Errorf("transaction failed before it started")
				}
				phase.terminal, phase.failed = true, true
			}
			if phase.terminal && index != len(artifact.Trace)-1 {
				return fmt.Errorf("transaction terminal state must be the final trace event")
			}
			continue
		}
		if !transaction.started || rollbackStarted && event.Phase != TracePhaseRollback {
			return fmt.Errorf("public evidence trace phase ordering is invalid")
		}

		switch event.State {
		case TraceStateStarted:
			if phase.started || phase.terminal {
				return fmt.Errorf("public evidence trace repeats a phase start")
			}
			switch event.Phase {
			case TracePhasePrepare:
				if progress[TracePhaseApply].started || progress[TracePhaseVerify].started || rollbackStarted {
					return fmt.Errorf("prepare phase ordering is invalid")
				}
			case TracePhaseApply:
				if !progress[TracePhasePrepare].succeeded || progress[TracePhaseVerify].started || rollbackStarted {
					return fmt.Errorf("apply phase ordering is invalid")
				}
			case TracePhaseVerify:
				if !progress[TracePhaseApply].succeeded || rollbackStarted {
					return fmt.Errorf("verify phase ordering is invalid")
				}
			case TracePhaseRollback:
				if !progress[TracePhaseApply].started {
					return fmt.Errorf("rollback cannot begin before apply")
				}
				rollbackStarted = true
			}
			phase.started = true
		case TraceStateSucceeded, TraceStateFailed:
			if !phase.started || phase.terminal {
				return fmt.Errorf("public evidence trace phase terminal state is invalid")
			}
			phase.terminal = true
			phase.succeeded = event.State == TraceStateSucceeded
			phase.failed = event.State == TraceStateFailed
		}
	}

	transaction := progress[TracePhaseTransaction]
	prepare := progress[TracePhasePrepare]
	apply := progress[TracePhaseApply]
	verify := progress[TracePhaseVerify]
	rollback := progress[TracePhaseRollback]
	if !transaction.terminal {
		return fmt.Errorf("public evidence trace must end in a transaction terminal state")
	}
	if prepare.started && !prepare.terminal {
		return fmt.Errorf("started prepare phase must have a terminal state")
	}
	if apply.started && !apply.terminal {
		return fmt.Errorf("started apply phase must have a terminal state")
	}
	if verify.started && !verify.terminal {
		return fmt.Errorf("started verify phase must have a terminal state")
	}
	if apply.started != artifact.WriteBoundaryCrossed {
		return fmt.Errorf("apply trace and writeBoundaryCrossed must agree")
	}
	if rollbackStarted != artifact.RollbackAttempted || rollback.succeeded != artifact.RollbackCompleted || rollback.failed != artifact.RollbackFailed {
		return fmt.Errorf("rollback trace and rollback flags must agree")
	}
	if rollback.started && !rollback.terminal {
		return fmt.Errorf("started rollback phase must have a terminal state")
	}
	if transaction.succeeded {
		if !verify.succeeded || rollback.started {
			return fmt.Errorf("successful transaction requires successful verification and no rollback")
		}
	} else if apply.started {
		if !rollback.terminal {
			return fmt.Errorf("failed transaction after apply requires a terminal rollback")
		}
	} else if rollback.started {
		return fmt.Errorf("failed transaction before apply cannot contain rollback")
	}
	return nil
}

func isKnownTraceState(state TraceState) bool {
	return state == TraceStateStarted || state == TraceStateSucceeded || state == TraceStateFailed
}

func isKnownDomain(domain releasedomain.Domain) bool {
	switch domain {
	case releasedomain.DomainNodeLocal,
		releasedomain.DomainAuthoritativeDNS,
		releasedomain.DomainControlPlane,
		releasedomain.DomainImageCache,
		releasedomain.DomainBackup:
		return true
	default:
		return false
	}
}

func isCanonicalPositiveDecimal(value string) bool {
	if !utf8.ValidString(value) || len(value) == 0 || len(value) > 20 || value[0] == '0' {
		return false
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}

func isLowerHex(value string, length int) bool {
	if !utf8.ValidString(value) || len(value) != length {
		return false
	}
	for _, digit := range value {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return false
		}
	}
	return true
}

func isCanonicalSHA256(value string) bool {
	return strings.HasPrefix(value, "sha256:") && isLowerHex(strings.TrimPrefix(value, "sha256:"), 64)
}

func isNilReader(reader io.Reader) bool {
	if reader == nil {
		return true
	}
	value := reflect.ValueOf(reader)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func validateStrictPublicJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := validateJSONValue(decoder, reflect.TypeOf(PublicArtifact{}), "publicEvidence"); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("decode trailing data: %w", err)
		}
		return fmt.Errorf("public evidence must contain exactly one JSON value")
	}
	return nil
}

func validateJSONValue(decoder *json.Decoder, expected reflect.Type, path string) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	switch expected.Kind() {
	case reflect.Struct:
		opening, ok := token.(json.Delim)
		if !ok || opening != '{' {
			return fmt.Errorf("%s must be a non-null object", path)
		}
		fields := make(map[string]reflect.StructField, expected.NumField())
		required := make(map[string]bool, expected.NumField())
		for index := 0; index < expected.NumField(); index++ {
			field := expected.Field(index)
			name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
			if field.PkgPath != "" || name == "-" {
				continue
			}
			fields[name] = field
			required[name] = !strings.Contains(","+options+",", ",omitempty,")
		}
		seen := make(map[string]struct{}, len(fields))
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("%s field name: %w", path, err)
			}
			name, ok := nameToken.(string)
			if !ok {
				return fmt.Errorf("%s field name must be a string", path)
			}
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("%s contains a duplicate field", path)
			}
			seen[name] = struct{}{}
			field, known := fields[name]
			if !known {
				return fmt.Errorf("%s contains an unknown field", path)
			}
			if err := validateJSONValue(decoder, field.Type, path+"."+name); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%s: close object: %w", path, err)
		}
		if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
			return fmt.Errorf("%s object is not closed", path)
		}
		for name, requiredField := range required {
			if _, present := seen[name]; requiredField && !present {
				return fmt.Errorf("%s is missing a required field", path)
			}
		}
		return nil
	case reflect.Slice, reflect.Array:
		opening, ok := token.(json.Delim)
		if !ok || opening != '[' {
			return fmt.Errorf("%s must be a non-null array", path)
		}
		index := 0
		for decoder.More() {
			if err := validateJSONValue(decoder, expected.Elem(), fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
			index++
		}
		closing, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%s: close array: %w", path, err)
		}
		if delimiter, ok := closing.(json.Delim); !ok || delimiter != ']' {
			return fmt.Errorf("%s array is not closed", path)
		}
		return nil
	case reflect.String:
		if _, ok := token.(string); !ok {
			return fmt.Errorf("%s must be a non-null string", path)
		}
		return nil
	case reflect.Bool:
		if _, ok := token.(bool); !ok {
			return fmt.Errorf("%s must be a non-null boolean", path)
		}
		return nil
	case reflect.Uint64:
		number, ok := token.(json.Number)
		if !ok {
			return fmt.Errorf("%s must be a non-null unsigned integer", path)
		}
		value, err := strconv.ParseUint(number.String(), 10, 64)
		if err != nil || strconv.FormatUint(value, 10) != number.String() {
			return fmt.Errorf("%s must be a canonical unsigned integer", path)
		}
		return nil
	default:
		return fmt.Errorf("%s has unsupported persisted JSON type", path)
	}
}

// encoding/json replaces malformed UTF-16 escapes with U+FFFD. Public
// evidence instead rejects them before decoding.
func validateJSONUnicodeEscapes(data []byte) error {
	for index := 0; index < len(data); index++ {
		if data[index] != '"' {
			continue
		}
		closed := false
		for index++; index < len(data); index++ {
			switch data[index] {
			case '"':
				closed = true
			case '\\':
				if index+1 >= len(data) {
					return fmt.Errorf("unterminated JSON escape")
				}
				escape := data[index+1]
				if escape != 'u' {
					if !strings.ContainsRune(`"\\/bfnrt`, rune(escape)) {
						return fmt.Errorf("invalid JSON escape")
					}
					index++
					continue
				}
				codePoint, ok := decodeHexQuad(data, index+2)
				if !ok {
					return fmt.Errorf("invalid JSON Unicode escape")
				}
				switch {
				case codePoint >= 0xd800 && codePoint <= 0xdbff:
					low, lowOK := decodeFollowingLowSurrogate(data, index+6)
					if !lowOK || low < 0xdc00 || low > 0xdfff {
						return fmt.Errorf("isolated high surrogate in JSON string")
					}
					index += 11
				case codePoint >= 0xdc00 && codePoint <= 0xdfff:
					return fmt.Errorf("isolated low surrogate in JSON string")
				default:
					index += 5
				}
			case 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
				0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
				0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f:
				return fmt.Errorf("unescaped control character in JSON string")
			}
			if closed {
				break
			}
		}
		if !closed {
			return fmt.Errorf("unterminated JSON string")
		}
	}
	return nil
}

func decodeHexQuad(data []byte, start int) (uint16, bool) {
	if start < 0 || start+4 > len(data) {
		return 0, false
	}
	var value uint16
	for _, digit := range data[start : start+4] {
		value <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			value |= uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			value |= uint16(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			value |= uint16(digit-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func decodeFollowingLowSurrogate(data []byte, start int) (uint16, bool) {
	if start+6 > len(data) || data[start] != '\\' || data[start+1] != 'u' {
		return 0, false
	}
	return decodeHexQuad(data, start+2)
}

type atomicOutput struct {
	requestedParent string
	resolvedParent  string
	base            string
	parent          *os.File
	parentStat      unix.Stat_t
	file            *os.File
	fileStat        unix.Stat_t
	temporaryName   string
	destinationStat unix.Stat_t
	destinationSet  bool
	published       bool
}

func createAtomicOutput(filename string) (*atomicOutput, error) {
	if strings.TrimSpace(filename) == "" {
		return nil, fmt.Errorf("public evidence output path is empty")
	}
	absolute, err := filepath.Abs(filename)
	if err != nil {
		return nil, fmt.Errorf("resolve public evidence output path: %w", err)
	}
	base := filepath.Base(absolute)
	if base == "." || base == ".." || base == string(filepath.Separator) {
		return nil, fmt.Errorf("public evidence output path must name one file")
	}
	requestedParent := filepath.Dir(absolute)
	requestedParentInfo, err := os.Lstat(requestedParent)
	if err != nil {
		return nil, fmt.Errorf("inspect public evidence output parent: %w", err)
	}
	if requestedParentInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("public evidence output parent must not be a symbolic link")
	}
	resolvedParent, err := filepath.EvalSymlinks(requestedParent)
	if err != nil {
		return nil, fmt.Errorf("resolve public evidence output parent: %w", err)
	}
	parentFD, err := unix.Open(resolvedParent, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("securely open public evidence output parent: %w", err)
	}
	parent := os.NewFile(uintptr(parentFD), resolvedParent)
	if parent == nil {
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("adopt public evidence output parent")
	}
	output := &atomicOutput{requestedParent: requestedParent, resolvedParent: resolvedParent, base: base, parent: parent}
	fail := func(failure error) (*atomicOutput, error) {
		return nil, errors.Join(failure, output.close())
	}
	if err := unix.Fstat(parentFD, &output.parentStat); err != nil {
		return fail(fmt.Errorf("inspect public evidence output parent: %w", err))
	}
	if err := validatePrivateParent(output.parentStat); err != nil {
		return fail(err)
	}
	if err := output.verifyParentIdentity(); err != nil {
		return fail(err)
	}

	destination, exists, err := inspectAt(parentFD, base)
	if err != nil {
		return fail(err)
	}
	if exists {
		if err := validatePrivateFile(destination, "existing public evidence output"); err != nil {
			return fail(err)
		}
		output.destinationStat, output.destinationSet = destination, true
	}
	for attempt := 0; attempt < atomicCreateAttempts; attempt++ {
		temporaryName, err := randomTemporaryName()
		if err != nil {
			return fail(err)
		}
		fileFD, openErr := unix.Openat(parentFD, temporaryName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
		if errors.Is(openErr, unix.EEXIST) {
			continue
		}
		if openErr != nil {
			return fail(fmt.Errorf("create fresh public evidence output: %w", openErr))
		}
		file := os.NewFile(uintptr(fileFD), temporaryName)
		if file == nil {
			_ = unix.Close(fileFD)
			unlinkErr := unix.Unlinkat(parentFD, temporaryName, 0)
			return fail(errors.Join(fmt.Errorf("adopt fresh public evidence output"), unlinkErr))
		}
		output.file, output.temporaryName = file, temporaryName
		if err := unix.Fchmod(fileFD, 0o600); err != nil {
			return fail(fmt.Errorf("set fresh public evidence output mode: %w", err))
		}
		if err := unix.Fstat(fileFD, &output.fileStat); err != nil {
			return fail(fmt.Errorf("inspect fresh public evidence output: %w", err))
		}
		if err := validatePrivateFile(output.fileStat, "fresh public evidence output"); err != nil {
			return fail(err)
		}
		if err := output.verifyTemporaryIdentity(); err != nil {
			return fail(err)
		}
		return output, nil
	}
	return fail(fmt.Errorf("create fresh public evidence output: collision limit exceeded"))
}

func (output *atomicOutput) publish(data []byte) error {
	if output == nil || output.parent == nil || output.file == nil || output.temporaryName == "" || output.published {
		return fmt.Errorf("public evidence output is not publishable")
	}
	if err := writeFull(output.file, data); err != nil {
		return fmt.Errorf("write public evidence output: %w", err)
	}
	if err := output.file.Sync(); err != nil {
		return fmt.Errorf("sync public evidence output: %w", err)
	}
	if err := output.verifyParentIdentity(); err != nil {
		return err
	}
	if err := output.verifyTemporaryIdentity(); err != nil {
		return err
	}
	current, exists, err := inspectAt(int(output.parent.Fd()), output.base)
	if err != nil {
		return err
	}
	if exists != output.destinationSet || exists && !samePrivateFile(output.destinationStat, current) {
		return fmt.Errorf("public evidence destination identity changed before publication")
	}
	if err := unix.Renameat(int(output.parent.Fd()), output.temporaryName, int(output.parent.Fd()), output.base); err != nil {
		return fmt.Errorf("atomically publish public evidence output: %w", err)
	}
	output.temporaryName, output.published = "", true
	if err := output.verifyPublishedIdentity(); err != nil {
		return err
	}
	if err := output.parent.Sync(); err != nil {
		return fmt.Errorf("sync public evidence output parent: %w", err)
	}
	if err := output.verifyParentIdentity(); err != nil {
		return err
	}
	return output.verifyPublishedIdentity()
}

func (output *atomicOutput) verifyParentIdentity() error {
	if output == nil || output.parent == nil {
		return fmt.Errorf("public evidence output parent is not open")
	}
	var opened unix.Stat_t
	if err := unix.Fstat(int(output.parent.Fd()), &opened); err != nil {
		return fmt.Errorf("reinspect public evidence output parent: %w", err)
	}
	if !sameFileIdentity(output.parentStat, opened) || opened.Mode != output.parentStat.Mode || opened.Uid != output.parentStat.Uid {
		return fmt.Errorf("public evidence output parent identity or mode changed")
	}
	if err := validatePrivateParent(opened); err != nil {
		return err
	}
	// Only the already-opened direct parent is trusted for filesystem
	// operations. Rechecking its two direct path spellings detects replacement
	// races; it deliberately does not extend the trust boundary to ancestors.
	for _, path := range []string{output.requestedParent, output.resolvedParent} {
		var current unix.Stat_t
		if err := unix.Lstat(path, &current); err != nil {
			return fmt.Errorf("reinspect public evidence output parent path: %w", err)
		}
		if current.Mode&unix.S_IFMT == unix.S_IFLNK {
			return fmt.Errorf("public evidence output parent path must not be a symbolic link")
		}
		if err := validatePrivateParent(current); err != nil {
			return fmt.Errorf("public evidence output parent path changed: %w", err)
		}
		if !sameFileIdentity(output.parentStat, current) || current.Mode != output.parentStat.Mode || current.Uid != output.parentStat.Uid {
			return fmt.Errorf("public evidence output parent path identity or mode changed")
		}
	}
	return nil
}

func (output *atomicOutput) verifyTemporaryIdentity() error {
	if output == nil || output.parent == nil || output.file == nil || output.temporaryName == "" {
		return fmt.Errorf("fresh public evidence output is not open")
	}
	var opened unix.Stat_t
	if err := unix.Fstat(int(output.file.Fd()), &opened); err != nil {
		return fmt.Errorf("reinspect opened public evidence output: %w", err)
	}
	pathStat, exists, err := inspectAt(int(output.parent.Fd()), output.temporaryName)
	if err != nil {
		return err
	}
	if !exists || !samePrivateFile(output.fileStat, opened) || !samePrivateFile(output.fileStat, pathStat) {
		return fmt.Errorf("fresh public evidence output identity or attributes changed")
	}
	return nil
}

func (output *atomicOutput) verifyPublishedIdentity() error {
	if output == nil || output.parent == nil || output.file == nil || !output.published {
		return fmt.Errorf("public evidence output is not published")
	}
	var opened unix.Stat_t
	if err := unix.Fstat(int(output.file.Fd()), &opened); err != nil {
		return fmt.Errorf("reinspect published public evidence output: %w", err)
	}
	pathStat, exists, err := inspectAt(int(output.parent.Fd()), output.base)
	if err != nil {
		return err
	}
	if !exists || !samePrivateFile(output.fileStat, opened) || !samePrivateFile(output.fileStat, pathStat) {
		return fmt.Errorf("published public evidence output identity or attributes changed")
	}
	return nil
}

func (output *atomicOutput) close() error {
	if output == nil {
		return nil
	}
	var result error
	if output.temporaryName != "" && output.parent != nil {
		var opened unix.Stat_t
		if output.file == nil {
			result = errors.Join(result, fmt.Errorf("failed public evidence temporary output is not open"))
		} else if err := unix.Fstat(int(output.file.Fd()), &opened); err != nil {
			result = errors.Join(result, fmt.Errorf("inspect failed public evidence temporary output: %w", err))
		} else if pathStat, exists, err := inspectAt(int(output.parent.Fd()), output.temporaryName); err != nil {
			result = errors.Join(result, err)
		} else if exists && sameFileIdentity(opened, pathStat) {
			result = errors.Join(result, unix.Unlinkat(int(output.parent.Fd()), output.temporaryName, 0))
		} else if exists {
			result = errors.Join(result, fmt.Errorf("failed public evidence temporary output identity changed"))
		}
		output.temporaryName = ""
	}
	if output.file != nil {
		result = errors.Join(result, output.file.Close())
		output.file = nil
	}
	if output.parent != nil {
		result = errors.Join(result, output.parent.Close())
		output.parent = nil
	}
	return result
}

func inspectAt(parentFD int, name string) (unix.Stat_t, bool, error) {
	var stat unix.Stat_t
	err := unix.Fstatat(parentFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return unix.Stat_t{}, false, nil
	}
	if err != nil {
		return unix.Stat_t{}, false, fmt.Errorf("inspect public evidence path without following links: %w", err)
	}
	return stat, true, nil
}

func validatePrivateParent(stat unix.Stat_t) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0o022 != 0 {
		return fmt.Errorf("public evidence output parent must be a current-user private directory")
	}
	return nil
}

func validatePrivateFile(stat unix.Stat_t, description string) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o7777 != 0o600 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return fmt.Errorf("%s must be a current-user regular file with exact mode 0600 and one link", description)
	}
	return nil
}

func sameFileIdentity(left, right unix.Stat_t) bool {
	return left.Dev == right.Dev && left.Ino == right.Ino
}

func samePrivateFile(left, right unix.Stat_t) bool {
	return sameFileIdentity(left, right) && left.Mode == right.Mode && left.Uid == right.Uid && left.Nlink == right.Nlink && right.Mode&unix.S_IFMT == unix.S_IFREG && right.Mode&0o7777 == 0o600 && right.Nlink == 1
}

func randomTemporaryName() (string, error) {
	var entropy [16]byte
	if _, err := io.ReadFull(rand.Reader, entropy[:]); err != nil {
		return "", fmt.Errorf("generate public evidence temporary name: %w", err)
	}
	return fmt.Sprintf(".fugue-release-public-evidence-%x.tmp", entropy[:]), nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
