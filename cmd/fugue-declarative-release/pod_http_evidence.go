package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"
)

type podHTTPFailure struct {
	ObservedAt         time.Time `json:"observed_at"`
	PodName            string    `json:"pod_name"`
	PodUID             string    `json:"pod_uid,omitempty"`
	Address            string    `json:"address"`
	Path               string    `json:"path"`
	Phase              string    `json:"phase"`
	Status             int       `json:"http_status"`
	CapturedBytes      int       `json:"captured_bytes"`
	CapturedBodyDigest string    `json:"captured_body_digest"`
	BodyComplete       bool      `json:"body_complete"`
	ErrorClass         string    `json:"error_class,omitempty"`
	cause              error
}

func (failure *podHTTPFailure) Error() string {
	raw, _ := json.Marshal(failure)
	return "pod HTTP observation failed: " + string(raw)
}

func (failure *podHTTPFailure) Unwrap() error { return failure.cause }

func podHTTPFailureEvidence(endpoint podHTTPEndpoint, path, phase string, status int, body []byte, complete bool, cause error) error {
	errorClass := ""
	var networkError net.Error
	if cause != nil {
		switch {
		case errors.Is(cause, context.Canceled):
			errorClass = "canceled"
		case errors.Is(cause, context.DeadlineExceeded):
			errorClass = "deadline_exceeded"
		case errors.As(cause, &networkError) && networkError.Timeout():
			errorClass = "timeout"
		default:
			errorClass = "transport_or_read_failure"
		}
	}
	return &podHTTPFailure{
		ObservedAt: time.Now().UTC(), PodName: endpoint.Name, PodUID: endpoint.UID,
		Address: net.JoinHostPort(endpoint.IP, fmt.Sprint(endpoint.Port)), Path: path, Phase: phase,
		Status: status, CapturedBytes: len(body), CapturedBodyDigest: fmt.Sprintf("sha256:%x", sha256.Sum256(body)),
		BodyComplete: complete, ErrorClass: errorClass, cause: cause,
	}
}
