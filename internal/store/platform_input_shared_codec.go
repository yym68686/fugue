package store

import (
	"crypto/sha256"
	"encoding/binary"
)

// V3 changes physical storage only. The signed digest still covers the exact
// existing canonical input stream. Array-object boundaries share identical
// evidence records, retaining every delimiter, identity and timestamp. Small
// objects are compressed together in bounded packs rather than separately.
const inputFormatSharedV3 = "runtime-snapshot-shared-v3"
const sharedBlockMin = 256
const sharedBlockMax = 32768
const sharedRecipeLimit = inputChunkBytes/64 + 2

type sharedRef struct {
	Hash [sha256.Size]byte
	Size int
}
type sharedRecipeRef struct {
	ID   int64
	Size int
}
type sharedObject struct {
	Ref  sharedRef
	Data []byte
}

func sharedBlocks(raw []byte, emit func([]byte) error) error {
	var splitter sharedSplitter
	return splitter.blocks(raw, emit)
}

type sharedSplitter struct {
	quoted, escaped bool
	stack           []byte
}

func (s *sharedSplitter) blocks(raw []byte, emit func([]byte) error) error {
	start := 0
	for i, b := range raw {
		boundary := false
		if s.escaped {
			s.escaped = false
		} else if s.quoted && b == '\\' {
			s.escaped = true
		} else if b == '"' {
			s.quoted = !s.quoted
		} else if !s.quoted {
			if b == '{' || b == '[' {
				s.stack = append(s.stack, b)
				if len(s.stack) > 10000 {
					return ErrInvalidInput
				}
			} else if b == '}' || b == ']' {
				if len(s.stack) == 0 {
					return ErrInvalidInput
				}
				if b == '}' && s.stack[len(s.stack)-1] != '{' || b == ']' && s.stack[len(s.stack)-1] != '[' {
					return ErrInvalidInput
				}
				s.stack = s.stack[:len(s.stack)-1]
				boundary = b == '}' && len(s.stack) > 0 && s.stack[len(s.stack)-1] == '['
			}
		}
		n := i + 1 - start
		if n < sharedBlockMax && (!boundary || n < sharedBlockMin) {
			continue
		}
		if err := emit(raw[start : i+1]); err != nil {
			return err
		}
		start = i + 1
	}
	if start < len(raw) {
		return emit(raw[start:])
	}
	return nil
}

func encodeSharedRecipe(refs []sharedRecipeRef, want int) ([]byte, error) {
	if want < 1 || want > inputChunkBytes || len(refs) < 1 || len(refs) > sharedRecipeLimit {
		return nil, ErrConflict
	}
	out := make([]byte, 5, 5+12*len(refs))
	out[0] = 3
	binary.BigEndian.PutUint32(out[1:], uint32(want))
	total := 0
	for _, ref := range refs {
		if ref.ID < 1 || ref.Size < 1 || ref.Size > sharedBlockMax {
			return nil, ErrConflict
		}
		total += ref.Size
		out = binary.BigEndian.AppendUint32(out, uint32(ref.Size))
		out = binary.BigEndian.AppendUint64(out, uint64(ref.ID))
	}
	if total != want {
		return nil, ErrConflict
	}
	return out, nil
}

func decodeSharedRecipe(raw []byte, want int) ([]sharedRecipeRef, error) {
	if want < 1 || want > inputChunkBytes || len(raw) < 17 || raw[0] != 3 || (len(raw)-5)%12 != 0 || (len(raw)-5)/12 > sharedRecipeLimit || int(binary.BigEndian.Uint32(raw[1:5])) != want {
		return nil, ErrConflict
	}
	refs := make([]sharedRecipeRef, 0, (len(raw)-5)/12)
	total := 0
	for offset := 5; offset < len(raw); offset += 12 {
		ref := sharedRecipeRef{Size: int(binary.BigEndian.Uint32(raw[offset : offset+4])), ID: int64(binary.BigEndian.Uint64(raw[offset+4 : offset+12]))}
		if ref.ID < 1 || ref.Size < 1 || ref.Size > sharedBlockMax {
			return nil, ErrConflict
		}
		total += ref.Size
		refs = append(refs, ref)
	}
	if total != want {
		return nil, ErrConflict
	}
	return refs, nil
}
