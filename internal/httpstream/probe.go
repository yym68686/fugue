package httpstream

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"fugue/internal/model"
)

const (
	defaultMaxChunks     = 5
	defaultMaxChunkBytes = 2048
)

type ProbeOptions struct {
	Target        string
	Accept        string
	MaxChunks     int
	MaxChunkBytes int
}

func Probe(client *http.Client, req *http.Request, opts ProbeOptions) model.HTTPStreamProbe {
	probe := model.HTTPStreamProbe{
		Target:  strings.TrimSpace(opts.Target),
		Accept:  strings.TrimSpace(opts.Accept),
		Headers: map[string][]string{},
		Timing:  model.HTTPStreamTiming{},
	}
	if req != nil && req.URL != nil {
		probe.URL = req.URL.String()
	}
	if client == nil {
		probe.Error = "http client is not configured"
		return probe
	}
	if req == nil {
		probe.Error = "request is required"
		return probe
	}
	if opts.MaxChunks <= 0 {
		opts.MaxChunks = defaultMaxChunks
	}
	if opts.MaxChunkBytes <= 0 {
		opts.MaxChunkBytes = defaultMaxChunkBytes
	}

	startedAt := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		probe.Error = err.Error()
		probe.Timing.TotalTimeMS = time.Since(startedAt).Milliseconds()
		return probe
	}
	defer resp.Body.Close()

	probe.Status = resp.Status
	probe.StatusCode = resp.StatusCode
	probe.Headers = cloneHeaderValues(resp.Header)
	probe.Timing.HeadersObserved = true
	probe.Timing.TimeToHeadersMS = time.Since(startedAt).Milliseconds()

	isSSE := looksLikeSSE(resp.Header, probe.Accept)
	buf := make([]byte, 2048)
	var sseBuffer bytes.Buffer
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			probe.BodyBytes += len(chunk)
			probe.ChunkCount++
			if !probe.Timing.BodyByteObserved {
				probe.Timing.BodyByteObserved = true
				probe.Timing.TimeToFirstBodyMS = time.Since(startedAt).Milliseconds()
			}
			if isSSE {
				sseBuffer.Write(chunk)
				for {
					frame, kind, ok := extractSSEFrame(&sseBuffer)
					if !ok {
						break
					}
					if kind == "sse_event" && !probe.Timing.SSEEventObserved {
						probe.Timing.SSEEventObserved = true
						probe.Timing.TimeToFirstSSEMS = time.Since(startedAt).Milliseconds()
					}
					if len(probe.FirstChunks) < opts.MaxChunks {
						probe.FirstChunks = append(probe.FirstChunks, sampleChunk(len(probe.FirstChunks), kind, frame, opts.MaxChunkBytes))
					}
				}
			} else if len(probe.FirstChunks) < opts.MaxChunks {
				probe.FirstChunks = append(probe.FirstChunks, sampleChunk(len(probe.FirstChunks), "body_chunk", chunk, opts.MaxChunkBytes))
			}
		}

		if readErr == nil {
			continue
		}

		probe.Timing.TotalTimeMS = time.Since(startedAt).Milliseconds()
		if errors.Is(readErr, io.EOF) {
			if isSSE && sseBuffer.Len() > 0 && len(probe.FirstChunks) < opts.MaxChunks {
				frame := append([]byte(nil), sseBuffer.Bytes()...)
				probe.FirstChunks = append(probe.FirstChunks, sampleChunk(len(probe.FirstChunks), classifySSEFrame(frame), frame, opts.MaxChunkBytes))
			}
			return probe
		}
		if probe.Timing.HeadersObserved && !probe.Timing.BodyByteObserved && looksLikeTimeout(readErr, req.Context()) {
			probe.HeadersOnlyStall = true
		}
		probe.Error = readErr.Error()
		return probe
	}
}

func looksLikeSSE(headers http.Header, accept string) bool {
	contentType := strings.ToLower(strings.TrimSpace(headers.Get("Content-Type")))
	accept = strings.ToLower(strings.TrimSpace(accept))
	return strings.Contains(contentType, "text/event-stream") || accept == "text/event-stream"
}

func cloneHeaderValues(headers http.Header) map[string][]string {
	if headers == nil {
		return map[string][]string{}
	}
	out := make(map[string][]string, len(headers))
	for key, values := range headers {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func sampleChunk(index int, kind string, payload []byte, maxBytes int) model.HTTPStreamChunkSample {
	if maxBytes <= 0 {
		maxBytes = defaultMaxChunkBytes
	}
	truncated := len(payload) > maxBytes
	visible := payload
	if truncated {
		visible = payload[:maxBytes]
	}
	encoding := "utf-8"
	rendered := string(visible)
	if !utf8.Valid(visible) {
		encoding = "base64"
		rendered = base64.StdEncoding.EncodeToString(visible)
	}
	return model.HTTPStreamChunkSample{
		Index:     index,
		Kind:      strings.TrimSpace(kind),
		Encoding:  encoding,
		Payload:   rendered,
		SizeBytes: len(payload),
		Truncated: truncated,
	}
}

func extractSSEFrame(buffer *bytes.Buffer) ([]byte, string, bool) {
	data := buffer.Bytes()
	advance := sseFrameAdvance(data)
	if advance == 0 {
		return nil, "", false
	}
	frame := append([]byte(nil), data[:advance]...)
	buffer.Next(advance)
	return frame, classifySSEFrame(frame), true
}

func sseFrameAdvance(data []byte) int {
	position := 0
	for position < len(data) {
		lineEnd, nextPos, ok := nextLine(data, position)
		if !ok {
			return 0
		}
		if lineEnd == position {
			return nextPos
		}
		position = nextPos
	}
	return 0
}

func nextLine(data []byte, start int) (int, int, bool) {
	for index := start; index < len(data); index++ {
		if data[index] != '\n' {
			continue
		}
		lineEnd := index
		if lineEnd > start && data[lineEnd-1] == '\r' {
			lineEnd--
		}
		return lineEnd, index + 1, true
	}
	return 0, 0, false
}

func classifySSEFrame(frame []byte) string {
	trimmed := bytes.TrimSpace(frame)
	if len(trimmed) == 0 {
		return "sse_comment"
	}
	hasComment := false
	hasEventField := false
	lines := splitFrameLines(frame)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		if line[0] == ':' {
			hasComment = true
			continue
		}
		field, _, _ := bytes.Cut(line, []byte(":"))
		field = bytes.TrimSpace(field)
		switch string(field) {
		case "data", "event", "id", "retry":
			hasEventField = true
		}
	}
	switch {
	case hasEventField:
		return "sse_event"
	case hasComment:
		return "sse_comment"
	default:
		return "body_chunk"
	}
}

func splitFrameLines(frame []byte) [][]byte {
	lines := make([][]byte, 0)
	start := 0
	for start < len(frame) {
		lineEnd, nextPos, ok := nextLine(frame, start)
		if !ok {
			lines = append(lines, append([]byte(nil), bytes.TrimRight(frame[start:], "\r\n")...))
			break
		}
		lines = append(lines, append([]byte(nil), frame[start:lineEnd]...))
		start = nextPos
	}
	return lines
}

func looksLikeTimeout(err error, ctx context.Context) bool {
	if err == nil {
		return false
	}
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return looksLikeTimeout(urlErr.Err, ctx)
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "deadline exceeded") || strings.Contains(message, "timeout")
}
