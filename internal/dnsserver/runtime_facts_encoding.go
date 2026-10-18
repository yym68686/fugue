package dnsserver

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"sync"
)

// The writer retains only bounded compression scratch. Resetting to Discard
// before returning it prevents a pooled writer from retaining a response buffer.
var runtimeFactsGzipWriters = sync.Pool{New: func() any {
	writer, err := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
	if err != nil {
		panic(err) // BestSpeed is a valid, compile-time compression level.
	}
	return writer
}}

func gzipRuntimeFacts(raw []byte) ([]byte, error) {
	var buffer bytes.Buffer
	buffer.Grow(min(len(raw), 64<<10))
	writer := runtimeFactsGzipWriters.Get().(*gzip.Writer)
	writer.Reset(&buffer)
	defer func() {
		writer.Reset(io.Discard)
		runtimeFactsGzipWriters.Put(writer)
	}()
	if _, err := writer.Write(raw); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func acceptsRuntimeFactsGzip(header http.Header) bool {
	quality, wildcard := -1, -1
	for _, value := range header.Values("Accept-Encoding") {
		for _, entry := range strings.Split(value, ",") {
			parts := strings.Split(entry, ";")
			coding := strings.TrimSpace(parts[0])
			if !strings.EqualFold(coding, "gzip") && coding != "*" {
				continue
			}
			q := 1000
			if len(parts) != 1 {
				name, value, found := strings.Cut(strings.TrimSpace(parts[1]), "=")
				q = runtimeFactsEncodingQuality(strings.TrimSpace(value))
				if len(parts) != 2 || !found || !strings.EqualFold(strings.TrimSpace(name), "q") {
					q = 0
				}
			}
			target := &quality
			if coding == "*" {
				target = &wildcard
			}
			// An explicit refusal wins if a client sends duplicate codings.
			if *target < 0 || q < *target {
				*target = q
			}
		}
	}
	if quality >= 0 {
		return quality > 0
	}
	return wildcard > 0
}

// HTTP qvalues are 0 or 1 with at most three fractional decimal digits.
// Malformed preferences conservatively retain the uncompressed representation.
func runtimeFactsEncodingQuality(value string) int {
	whole, fraction, _ := strings.Cut(value, ".")
	if whole != "0" && whole != "1" || len(fraction) > 3 {
		return 0
	}
	q, place := 0, 100
	for _, digit := range fraction {
		if digit < '0' || digit > '9' || whole == "1" && digit != '0' {
			return 0
		}
		q += int(digit-'0') * place
		place /= 10
	}
	if whole == "1" {
		return 1000
	}
	return q
}
