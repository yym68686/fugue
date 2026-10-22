package store

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
)

const inputFormatV1 = "runtime-snapshot-json-v1"
const inputFormatCodecV2 = "runtime-snapshot-json-chunk-codec-v2"
const inputCodecHeaderBytes = 5

// V2 hashes the stored envelope in chunk_hash; the manifest still hashes the
// complete decoded canonical JSON. Byte 0 is 0 (raw) or 1 (RFC 1950 zlib),
// followed by the big-endian decoded length and exactly one payload. A raw
// fallback fits the existing bytea bound when chunk_size <= 262139.
// This reader-first release does not change schemas, policy or production writes.
func decodeInputChunk(format string, stored []byte, want int) ([]byte, error) {
	invalid := func() ([]byte, error) {
		return nil, fmt.Errorf("compiler input chunk codec/length differs: %w", ErrConflict)
	}
	if want < 1 || want > inputChunkBytes {
		return invalid()
	}
	if format == inputFormatV1 {
		if len(stored) != want {
			return invalid()
		}
		return stored, nil
	}
	if format != inputFormatCodecV2 || want > inputChunkBytes-inputCodecHeaderBytes || len(stored) < inputCodecHeaderBytes || len(stored) > want+inputCodecHeaderBytes || int(binary.BigEndian.Uint32(stored[1:5])) != want {
		return invalid()
	}
	payload := stored[inputCodecHeaderBytes:]
	switch stored[0] {
	case 0:
		if len(payload) != want {
			return invalid()
		}
		return payload, nil
	case 1:
		input := bytes.NewReader(payload)
		z, err := zlib.NewReader(input)
		if err != nil {
			return invalid()
		}
		// Both compressed input and decoded output are bounded per chunk. Reading
		// one extra byte detects expansion; EOF also validates the zlib checksum.
		raw, err := io.ReadAll(io.LimitReader(z, int64(want)+1))
		closeErr := z.Close()
		if err != nil || closeErr != nil || len(raw) != want || input.Len() != 0 {
			return invalid()
		}
		return raw, nil
	default:
		return invalid()
	}
}
