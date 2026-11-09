package edgecontrol

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

const (
	GroupOriginalExportSchema = "fugue.group-original-export.v1"
	groupOriginalRecordBytes  = 8 << 20
	groupOriginalBatchBytes   = 32 << 20
	groupOriginalEntries      = 32768
)

var ErrGroupOriginalExport = errors.New("edge-control original evidence export unavailable")

// GroupOriginalExport contains exact selected source values, not a new
// attestation, serving state, or a dependency/retirement admission. The native
// checksum binds the complete typed file; it cannot authenticate its origin.
// Unselected entries are syntax/checksum checked, not semantically admitted.
type GroupOriginalExport struct {
	Schema                 string                    `json:"schema"`
	GroupID                string                    `json:"edge_group_id"`
	Revision               uint64                    `json:"revision"`
	StateDigest            string                    `json:"state_digest"`
	FileSHA256             string                    `json:"file_sha256"`
	EncodedBytes           int64                     `json:"encoded_bytes"`
	DecodedBytes           int64                     `json:"decoded_bytes"`
	SigningKeyringRevision uint64                    `json:"signing_keyring_revision"`
	ShadowEntries          int                       `json:"shadow_entries"`
	AuthorityEntries       int                       `json:"authority_entries"`
	Generations            []GroupOriginalGeneration `json:"generations"`
	ChecksumVerified       bool                      `json:"checksum_verified"`
	StateContentsValidated bool                      `json:"state_contents_validated"`
	AdmissionAuthenticated bool                      `json:"admission_authenticated"`
	DependencyClosure      bool                      `json:"dependency_closure"`
	Uncovered              []string                  `json:"uncovered"`
}

type GroupOriginalGeneration struct {
	Generation    string                      `json:"generation"`
	Shadow        []GroupShadowLedgerEntry    `json:"shadow"`
	Publications  []GroupAuthorityLedgerEntry `json:"publications"`
	SignedBodies  []GroupOriginalSignedBody   `json:"signed_bodies"`
	CompileInputs []json.RawMessage           `json:"compile_inputs"`
}

type GroupOriginalSignedBody struct {
	Source               string                `json:"source"`
	Digest               string                `json:"digest"`
	Bundle               model.EdgeRouteBundle `json:"bundle"`
	SignatureVerified    bool                  `json:"signature_verified"`
	PublicationSequences []uint64              `json:"publication_sequences"`
}

type groupOriginalParser struct {
	ctx context.Context
	r   *bufio.Reader
}

// ExportGroupOriginalEvidence scans at most 32MiB encoded AND decoded, with
// an 8MiB individual value/frame and a 32MiB retained selection/output bound.
// It never constructs a serving store, locks, writes, signs, contacts a DB,
// or exports a whole state file. Only explicitly selected generations leave
// the process. The old 8MiB whole-file diagnostic remains unchanged.
func ExportGroupOriginalEvidence(ctx context.Context, stateFile, groupID, keyringDir string, generations []string) (GroupOriginalExport, error) {
	var empty GroupOriginalExport
	if ctx == nil || groupID == "" || groupID != normalizeGroupID(groupID) || len(groupID) > 128 || !edgeGroupIDPattern.MatchString(groupID) ||
		!filepath.IsAbs(stateFile) || filepath.Clean(stateFile) != stateFile || !filepath.IsAbs(keyringDir) || filepath.Clean(keyringDir) != keyringDir || len(generations) == 0 || len(generations) > 32 {
		return empty, ErrGroupOriginalExport
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	wanted := map[string]int{}
	out := GroupOriginalExport{Schema: GroupOriginalExportSchema, GroupID: groupID, Generations: make([]GroupOriginalGeneration, len(generations)), Uncovered: []string{
		"Independent source-file identity and policy custody are required; its checksum is not authority authentication.",
		"Unselected state contents and the complete live-root dependency closure are not semantically validated.",
		"Missing historical bodies, inventory/route-intent sources and original publication signatures remain missing.",
		"Current-policy admission, persistent reactivation and physical retirement remain unproven.",
	}}
	for i, g := range generations {
		if g == "" || g != strings.TrimSpace(g) || len(g) > 512 || strings.ContainsAny(g, "\r\n\t") {
			return empty, ErrGroupOriginalExport
		}
		if _, ok := wanted[g]; ok {
			return empty, ErrGroupOriginalExport
		}
		wanted[g] = i
		out.Generations[i] = GroupOriginalGeneration{Generation: g, Shadow: []GroupShadowLedgerEntry{}, Publications: []GroupAuthorityLedgerEntry{}, SignedBodies: []GroupOriginalSignedBody{}, CompileInputs: []json.RawMessage{}}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	fd, err := syscall.Open(stateFile, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return empty, ErrGroupOriginalExport
	}
	f := os.NewFile(uintptr(fd), stateFile)
	defer f.Close()
	before, err := f.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0o077 != 0 || before.Size() <= 0 || before.Size() > groupOriginalBatchBytes {
		return empty, ErrGroupOriginalExport
	}
	keyFile := filepath.Join(keyringDir, groupID+".json")
	keys, err := loadGroupBundleSigningKeyring(keyFile, groupID)
	if err != nil {
		return empty, ErrGroupOriginalExport
	}
	primary, _ := base64.RawURLEncoding.DecodeString(keys.PrimaryKey)
	defer zeroBytes(primary)
	previous, _ := base64.RawURLEncoding.DecodeString(keys.PreviousKey)
	defer zeroBytes(previous)
	verifier := bundleauth.NewKeyring(string(primary), keys.PrimaryKeyID, string(previous), keys.PreviousKeyID, keys.RevokedKeyIDs)
	wireHash := sha256.New()
	encodedLimit := &io.LimitedReader{R: f, N: groupOriginalBatchBytes + 1}
	encoded := bufio.NewReader(io.TeeReader(encodedLimit, wireHash))
	magic, err := encoded.Peek(2)
	if err != nil {
		return empty, ErrGroupOriginalExport
	}
	var source io.Reader = encoded
	if bytes.Equal(magic, []byte{0x1f, 0x8b}) {
		z, e := gzip.NewReader(encoded)
		if e != nil {
			return empty, ErrGroupOriginalExport
		}
		defer z.Close()
		z.Multistream(false)
		source = z
	}
	decodedLimit := &io.LimitedReader{R: source, N: groupOriginalBatchBytes + 1}
	p := groupOriginalParser{ctx: ctx, r: bufio.NewReader(decodedLimit)}
	// Marshal one typed field/array element at a time in native struct order.
	// No full raw source, decoded state, or full canonical copy is retained.
	digest := sha256.New()
	_, _ = digest.Write([]byte{'{'})
	stateType := reflect.TypeOf(persistentGroupState{})
	fields := map[string]int{}
	for i := 0; i < stateType.NumField(); i++ {
		fields[strings.Split(stateType.Field(i).Tag.Get("json"), ",")[0]] = i
	}
	last := -1
	first := true
	seen := map[string]bool{}
	retained := 0
	entryCount := 0
	// Only selected cross-reference witnesses remain resident. Unselected
	// history is checked and discarded one record at a time.
	shadows := map[uint64]GroupShadowLedgerEntry{}
	if p.expect('{') != nil {
		return empty, ErrGroupOriginalExport
	}
	for {
		ch, e := p.peek()
		if e != nil {
			return empty, ErrGroupOriginalExport
		}
		if ch == '}' {
			_, _ = p.r.ReadByte()
			break
		}
		if !first {
			if p.expect(',') != nil {
				return empty, ErrGroupOriginalExport
			}
		}
		keyRaw, e := p.value(128)
		if e != nil {
			return empty, ErrGroupOriginalExport
		}
		var key string
		if json.Unmarshal(keyRaw, &key) != nil || p.expect(':') != nil {
			return empty, ErrGroupOriginalExport
		}
		index, ok := fields[key]
		if !ok || index <= last {
			return empty, ErrGroupOriginalExport
		}
		last = index
		seen[key] = true
		if !first {
			_, _ = digest.Write([]byte{','})
		}
		first = false
		k, _ := json.Marshal(key)
		_, _ = digest.Write(k)
		_, _ = digest.Write([]byte{':'})
		field := stateType.Field(index)
		if field.Type.Kind() == reflect.Slice {
			if p.expect('[') != nil {
				return empty, ErrGroupOriginalExport
			}
			_, _ = digest.Write([]byte{'['})
			count := 0
			for {
				ch, e := p.peek()
				if e != nil {
					return empty, ErrGroupOriginalExport
				}
				if ch == ']' {
					_, _ = p.r.ReadByte()
					break
				}
				if count > 0 {
					if p.expect(',') != nil {
						return empty, ErrGroupOriginalExport
					}
					_, _ = digest.Write([]byte{','})
				}
				count++
				entryCount++
				limit := groupOriginalEntries
				if key == "candidate_history" {
					limit = retainedGroupCandidateBundles
				}
				if key == "retention_inputs" {
					limit = 32
				}
				if count > limit || entryCount > groupOriginalEntries {
					return empty, ErrGroupOriginalExport
				}
				raw, e := p.value(groupOriginalRecordBytes)
				if e != nil {
					return empty, ErrGroupOriginalExport
				}
				v := reflect.New(field.Type.Elem())
				canonical, e := groupOriginalDecode(raw, v.Interface())
				if e != nil {
					return empty, ErrGroupOriginalExport
				}
				_, _ = digest.Write(canonical)
				switch item := v.Elem().Interface().(type) {
				case GroupShadowLedgerEntry:
					if item.Sequence != uint64(count) || item.GroupID != groupID || item.Schema != GroupShadowLedgerSchemaV1 {
						return empty, ErrGroupOriginalExport
					}
					if i, ok := wanted[item.BundleGeneration]; ok {
						out.Generations[i].Shadow = append(out.Generations[i].Shadow, item)
						retained += len(canonical)
						item.Bundle = nil
						shadows[item.Sequence] = item
					}
				case GroupAuthorityLedgerEntry:
					if item.Sequence != uint64(count) || item.GroupID != groupID || item.Schema != GroupAuthorityLedgerSchemaV1 {
						return empty, ErrGroupOriginalExport
					}
					if i, ok := wanted[item.BundleGeneration]; ok {
						s, found := shadows[item.CandidateLedgerSequence]
						if item.Status != GroupAuthorityStatusPublished || !found || item.Authority != "edge-control" || !item.PublicationEnabled || item.RecordedAt.IsZero() || item.FailureCode != "" ||
							!groupAuthorityDigestPattern.MatchString(item.PublishedBundleDigest) || !groupAuthorityKeyIDPattern.MatchString(item.SigningKeyID) || item.LastPublishedBundleGeneration != item.BundleGeneration ||
							s.Status != GroupShadowStatusCompiled || s.BundleGeneration != item.BundleGeneration || s.RouteIntentGeneration != item.RouteIntentGeneration || s.InventoryGeneration != item.InventoryGeneration {
							return empty, ErrGroupOriginalExport
						}
						out.Generations[i].Publications = append(out.Generations[i].Publications, item)
						retained += len(canonical)
					}
				case GroupCandidateBundle:
					if validateGroupCandidateBundle(groupID, item) != nil {
						return empty, ErrGroupOriginalExport
					}
					retained += groupOriginalAddBody(&out, wanted, key, item.Bundle, verifier)
					if item.CurrentBundle != nil {
						retained += groupOriginalAddBody(&out, wanted, key+".current_bundle", *item.CurrentBundle, verifier)
					}
				case GroupCompileInput:
					if i, ok := wanted[item.Entry.BundleGeneration]; ok {
						shadow, found := shadows[item.Entry.Sequence]
						original := item.Entry
						original.Bundle, original.BundleArchived = nil, shadow.BundleArchived
						if !found || validateGroupCompileInput(item, groupID) != nil || !sameGroupCompileValue(shadow, original) {
							return empty, ErrGroupOriginalExport
						}
						out.Generations[i].CompileInputs = append(out.Generations[i].CompileInputs, json.RawMessage(canonical))
						retained += len(canonical)
					}
				}
				if retained > groupOriginalBatchBytes {
					return empty, ErrGroupOriginalExport
				}
			}
			// Native omitempty does not hash explicitly empty optional arrays.
			if count == 0 {
				return empty, ErrGroupOriginalExport
			}
			_, _ = digest.Write([]byte{']'})
			if key == "ledger" {
				out.ShadowEntries = count
			}
			if key == "authority_ledger" {
				out.AuthorityEntries = count
			}
		} else {
			raw, e := p.value(groupOriginalRecordBytes)
			if e != nil {
				return empty, ErrGroupOriginalExport
			}
			v := reflect.New(field.Type)
			canonical, e := groupOriginalDecode(raw, v.Interface())
			if e != nil {
				return empty, ErrGroupOriginalExport
			}
			if strings.Contains(field.Tag.Get("json"), ",omitempty") && v.Elem().IsZero() {
				return empty, ErrGroupOriginalExport
			}
			switch key {
			case "schema":
				if v.Elem().String() != persistentGroupStateSchemaV1 {
					return empty, ErrGroupOriginalExport
				}
			case "edge_group_id":
				if v.Elem().String() != groupID {
					return empty, ErrGroupOriginalExport
				}
			case "revision":
				out.Revision = v.Elem().Uint()
				if out.Revision == 0 {
					return empty, ErrGroupOriginalExport
				}
			case "digest":
				out.StateDigest = v.Elem().String()
				canonical = []byte(`""`)
			case "published_bundle":
				pub := v.Elem().Interface().(*GroupPublishedBundle)
				if pub == nil || validateGroupPublishedBundle(groupID, *pub) != nil {
					return empty, ErrGroupOriginalExport
				}
				retained += groupOriginalAddBody(&out, wanted, key, pub.Bundle, verifier)
			case "candidate_bundle":
				candidate := v.Elem().Interface().(*GroupCandidateBundle)
				if candidate == nil || validateGroupCandidateBundle(groupID, *candidate) != nil {
					return empty, ErrGroupOriginalExport
				}
				retained += groupOriginalAddBody(&out, wanted, key, candidate.Bundle, verifier)
				if candidate.CurrentBundle != nil {
					retained += groupOriginalAddBody(&out, wanted, key+".current_bundle", *candidate.CurrentBundle, verifier)
				}
			}
			if retained > groupOriginalBatchBytes {
				return empty, ErrGroupOriginalExport
			}
			_, _ = digest.Write(canonical)
		}
	}
	_, _ = digest.Write([]byte{'}'})
	if _, err := p.peek(); err != io.EOF || decodedLimit.N <= 0 {
		return empty, ErrGroupOriginalExport
	}
	if _, err := encoded.Peek(1); err != io.EOF || encodedLimit.N <= 0 {
		return empty, ErrGroupOriginalExport
	}
	for _, key := range []string{"schema", "edge_group_id", "revision", "digest"} {
		if !seen[key] {
			return empty, ErrGroupOriginalExport
		}
	}
	if out.StateDigest != "sha256:"+hex.EncodeToString(digest.Sum(nil)) {
		return empty, ErrGroupOriginalExport
	}
	for i := range out.Generations {
		g := &out.Generations[i]
		for j := range g.SignedBodies {
			body := &g.SignedBodies[j]
			for _, pub := range g.Publications {
				if pub.PublishedBundleDigest == body.Digest && pub.SigningKeyID == body.Bundle.KeyID {
					body.PublicationSequences = append(body.PublicationSequences, pub.Sequence)
				}
			}
		}
		// A generation is the exported artifact unit, including all original
		// ledger rows, signed bodies and publication witnesses for that unit.
		raw, err := json.Marshal(g)
		if err != nil || len(raw) > groupOriginalRecordBytes {
			return empty, ErrGroupOriginalExport
		}
	}
	after, aerr := f.Stat()
	link, lerr := os.Lstat(stateFile)
	current, kerr := loadGroupBundleSigningKeyring(keyFile, groupID)
	if aerr != nil || lerr != nil || kerr != nil || !reflect.DeepEqual(keys, current) || !os.SameFile(before, after) || !os.SameFile(before, link) || link.Mode()&os.ModeSymlink != 0 ||
		before.Size() != after.Size() || before.ModTime() != after.ModTime() || before.Mode() != after.Mode() || ctx.Err() != nil {
		return empty, ErrGroupOriginalExport
	}
	out.FileSHA256 = "sha256:" + hex.EncodeToString(wireHash.Sum(nil))
	out.EncodedBytes = groupOriginalBatchBytes + 1 - encodedLimit.N
	out.DecodedBytes = groupOriginalBatchBytes + 1 - decodedLimit.N
	if out.EncodedBytes != before.Size() {
		return empty, ErrGroupOriginalExport
	}
	out.SigningKeyringRevision = keys.generation
	out.ChecksumVerified = true
	report, e := json.Marshal(out)
	if e != nil || len(report) > groupOriginalBatchBytes {
		return empty, ErrGroupOriginalExport
	}
	return out, nil
}

func groupOriginalAddBody(out *GroupOriginalExport, wanted map[string]int, source string, b model.EdgeRouteBundle, keys bundleauth.Keyring) int {
	i, ok := wanted[b.Generation]
	if !ok {
		return 0
	}
	digest := signedGroupBundleDigest(b)
	for _, body := range out.Generations[i].SignedBodies {
		if body.Digest == digest {
			return 0
		}
	}
	body := GroupOriginalSignedBody{Source: source, Digest: digest, Bundle: b, SignatureVerified: b.EdgeGroupID == out.GroupID && !b.GeneratedAt.IsZero() && bundleauth.VerifyEdgeRouteBundleWithKeyring(b, keys, b.GeneratedAt) == nil, PublicationSequences: []uint64{}}
	out.Generations[i].SignedBodies = append(out.Generations[i].SignedBodies, body)
	raw, _ := json.Marshal(body)
	return len(raw)
}

func groupOriginalDecode(raw []byte, target any) ([]byte, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil || d.Decode(new(any)) != io.EOF {
		return nil, ErrGroupOriginalExport
	}
	// Dereference the decoding pointer so omitempty/native field encodings
	// remain identical to the serving store's canonical checksum.
	v := reflect.ValueOf(target).Elem().Interface()
	raw, err := json.Marshal(v)
	if err != nil || len(raw) > groupOriginalRecordBytes {
		return nil, ErrGroupOriginalExport
	}
	return raw, nil
}

func (p *groupOriginalParser) peek() (byte, error) {
	for {
		if err := p.ctx.Err(); err != nil {
			return 0, err
		}
		b, err := p.r.Peek(1)
		if err != nil {
			return 0, err
		}
		switch b[0] {
		case ' ', '\n', '\r', '\t':
			_, _ = p.r.ReadByte()
		default:
			return b[0], nil
		}
	}
}
func (p *groupOriginalParser) expect(want byte) error {
	b, err := p.peek()
	if err != nil || b != want {
		return ErrGroupOriginalExport
	}
	_, err = p.r.ReadByte()
	return err
}

// Frame before decoding: even a hostile single string/object cannot grow
// json.Decoder's buffer to the whole 32MiB scan budget. Depth is bounded too.
func (p *groupOriginalParser) value(limit int) ([]byte, error) {
	if _, err := p.peek(); err != nil {
		return nil, err
	}
	var raw []byte
	var stack [64]byte
	depth := 0
	quoted, escape := false, false
	for {
		if err := p.ctx.Err(); err != nil {
			return nil, err
		}
		b, err := p.r.Peek(1)
		if err != nil {
			if err == io.EOF && len(raw) > 0 && !quoted && depth == 0 {
				break
			}
			return nil, ErrGroupOriginalExport
		}
		ch := b[0]
		if len(raw) > 0 && !quoted && depth == 0 {
			break
		}
		_, _ = p.r.ReadByte()
		raw = append(raw, ch)
		if len(raw) > limit {
			return nil, ErrGroupOriginalExport
		}
		if quoted {
			if escape {
				escape = false
			} else if ch == '\\' {
				escape = true
			} else if ch == '"' {
				quoted = false
			}
			continue
		}
		switch ch {
		case '"':
			quoted = true
		case '{', '[':
			if depth == len(stack) {
				return nil, ErrGroupOriginalExport
			}
			stack[depth] = ch
			depth++
		case '}', ']':
			if depth == 0 || (ch == '}' && stack[depth-1] != '{') || (ch == ']' && stack[depth-1] != '[') {
				return nil, ErrGroupOriginalExport
			}
			depth--
		default:
			if depth == 0 {
				for {
					next, e := p.r.Peek(1)
					if e == io.EOF {
						break
					}
					if e != nil {
						return nil, ErrGroupOriginalExport
					}
					if strings.ContainsRune(",]} \t\r\n", rune(next[0])) {
						break
					}
					if err := p.ctx.Err(); err != nil {
						return nil, err
					}
					v, _ := p.r.ReadByte()
					raw = append(raw, v)
					if len(raw) > limit {
						return nil, ErrGroupOriginalExport
					}
				}
			}
		}
	}
	if !json.Valid(raw) {
		return nil, ErrGroupOriginalExport
	}
	return raw, nil
}
