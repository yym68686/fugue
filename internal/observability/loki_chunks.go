package observability

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"
)

// Loki's default maximum is measured on each decoded log entry, not the push
// request. Keep this bound even when a server permits larger entries.
const maxLokiLogLineBytes = 256 << 10

// lokiLogLines preserves ordinary log lines byte-for-byte. Oversized lines are
// losslessly framed as JSON fragments so one large message or attribute cannot
// cause Loki to reject a batch. Joining message fields in log_chunk_index order
// reconstructs the original serialized lokiLine; log_chunk_sha256 verifies it.
// Chunk metadata stays in the body, never in high-cardinality stream labels.
func lokiLogLines(event Event) ([]string, error) {
	line, err := json.Marshal(lokiLogLine(event))
	if err != nil {
		return nil, err
	}
	if len(line) <= maxLokiLogLineBytes {
		return []string{string(line)}, nil
	}

	chunk := lokiLine{
		Timestamp: lokiLogLine(event).Timestamp,
		Kind:      EventKindLog,
		Source:    "telemetry_chunk",
		Attributes: map[string]string{
			"log_chunk_encoding": "loki-line-json-v1",
			"log_chunk_sha256":   fmt.Sprintf("%x", sha256.Sum256(line)),
			"log_chunk_index":    "18446744073709551615",
			"log_chunk_count":    "18446744073709551615",
		},
	}
	// Reserve the maximum decimal index/count widths and the optional message
	// field. A source byte needs at most six bytes when JSON escaped (\u00xx).
	chunk.Message = "x"
	framing, err := json.Marshal(chunk)
	if err != nil {
		return nil, err
	}
	chunkBytes := (maxLokiLogLineBytes - len(framing)) / 6
	var fragments []string
	for len(line) > 0 {
		n := min(len(line), chunkBytes)
		if n < len(line) {
			for !utf8.RuneStart(line[n]) {
				n--
			}
		}
		fragments = append(fragments, string(line[:n]))
		line = line[n:]
	}
	chunk.Attributes["log_chunk_count"] = strconv.Itoa(len(fragments))
	lines := make([]string, 0, len(fragments))
	for i, fragment := range fragments {
		chunk.Attributes["log_chunk_index"] = strconv.Itoa(i)
		chunk.Message = fragment
		encoded, err := json.Marshal(chunk)
		if err != nil {
			return nil, err
		}
		lines = append(lines, string(encoded))
	}
	return lines, nil
}
