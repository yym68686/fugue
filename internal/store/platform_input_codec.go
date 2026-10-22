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
// Writers activate independently after compatible readers and schema exist.
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

// Reuse one bounded compressor per input, not one large allocation per chunk.
type inputChunkEncoder struct {
	buffer       bytes.Buffer
	writer       *zlib.Writer
	verifier     io.ReadCloser
	verifyBuffer []byte
}

func (e *inputChunkEncoder) encode(raw []byte) ([]byte, error) {
	if len(raw) == 0 || len(raw) > inputChunkBytes-inputCodecHeaderBytes {
		return nil, ErrInvalidInput
	}
	e.buffer.Reset()
	e.buffer.Write(make([]byte, inputCodecHeaderBytes))
	if e.writer == nil {
		var err error
		e.writer, err = zlib.NewWriterLevel(&e.buffer, zlib.BestSpeed)
		if err != nil {
			return nil, err
		}
	} else {
		e.writer.Reset(&e.buffer)
	}
	if _, err := e.writer.Write(raw); err != nil {
		return nil, err
	}
	if err := e.writer.Close(); err != nil {
		return nil, err
	}
	codec := byte(1)
	if e.buffer.Len() >= len(raw)+inputCodecHeaderBytes {
		e.buffer.Reset()
		e.buffer.Write(make([]byte, inputCodecHeaderBytes))
		e.buffer.Write(raw)
		codec = 0
	}
	stored := e.buffer.Bytes()
	stored[0] = codec
	binary.BigEndian.PutUint32(stored[1:5], uint32(len(raw)))
	// Validate the exact output before its physical hash becomes a witness.
	if codec == 0 {
		if !bytes.Equal(stored[inputCodecHeaderBytes:], raw) {
			return nil, ErrConflict
		}
	} else {
		input := bytes.NewReader(stored[inputCodecHeaderBytes:])
		var err error
		if e.verifier == nil {
			e.verifier, err = zlib.NewReader(input)
		} else {
			err = e.verifier.(zlib.Resetter).Reset(input, nil)
		}
		if err != nil {
			return nil, err
		}
		if cap(e.verifyBuffer) < len(raw) {
			e.verifyBuffer = make([]byte, len(raw))
		}
		decoded := e.verifyBuffer[:len(raw)]
		_, readErr := io.ReadFull(e.verifier, decoded)
		var extra [1]byte
		n, endErr := e.verifier.Read(extra[:])
		closeErr := e.verifier.Close()
		if readErr != nil || n != 0 || endErr != io.EOF || closeErr != nil || input.Len() != 0 || !bytes.Equal(decoded, raw) {
			return nil, ErrConflict
		}
	}
	return stored, nil
}
