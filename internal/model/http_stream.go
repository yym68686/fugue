package model

type HTTPStreamChunkSample struct {
	Index     int    `json:"index"`
	Kind      string `json:"kind"`
	Encoding  string `json:"encoding"`
	Payload   string `json:"payload"`
	SizeBytes int    `json:"size_bytes"`
	Truncated bool   `json:"truncated,omitempty"`
}

type HTTPStreamTiming struct {
	HeadersObserved   bool  `json:"headers_observed"`
	BodyByteObserved  bool  `json:"body_byte_observed"`
	SSEEventObserved  bool  `json:"sse_event_observed"`
	TimeToHeadersMS   int64 `json:"time_to_headers_ms,omitempty"`
	TimeToFirstBodyMS int64 `json:"time_to_first_body_byte_ms,omitempty"`
	TimeToFirstSSEMS  int64 `json:"time_to_first_sse_event_ms,omitempty"`
	TotalTimeMS       int64 `json:"total_time_ms,omitempty"`
}

type HTTPStreamProbe struct {
	Target           string                  `json:"target"`
	Accept           string                  `json:"accept"`
	URL              string                  `json:"url"`
	Status           string                  `json:"status,omitempty"`
	StatusCode       int                     `json:"status_code,omitempty"`
	Headers          map[string][]string     `json:"headers,omitempty"`
	Error            string                  `json:"error,omitempty"`
	HeadersOnlyStall bool                    `json:"headers_only_stall,omitempty"`
	BodyBytes        int                     `json:"body_bytes,omitempty"`
	ChunkCount       int                     `json:"chunk_count,omitempty"`
	Timing           HTTPStreamTiming        `json:"timing"`
	FirstChunks      []HTTPStreamChunkSample `json:"first_chunks,omitempty"`
}
