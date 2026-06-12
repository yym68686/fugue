package viewmodel

import (
	"errors"
	"strings"
)

type StateKind string

const (
	StateReady      StateKind = "ready"
	StateEmpty      StateKind = "empty"
	StateError      StateKind = "error"
	StatePermission StateKind = "permission"
)

type State struct {
	Kind    StateKind `json:"kind"`
	Message string    `json:"message,omitempty"`
}

func ReadyState() State {
	return State{Kind: StateReady}
}

func EmptyState(message string) State {
	return State{Kind: StateEmpty, Message: strings.TrimSpace(message)}
}

func ErrorState(err error) State {
	if err == nil {
		return State{Kind: StateError}
	}
	return State{Kind: StateError, Message: strings.TrimSpace(err.Error())}
}

func PermissionState(message string) State {
	return State{Kind: StatePermission, Message: strings.TrimSpace(message)}
}

func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPermissionDenied) {
		return true
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "permission") || strings.Contains(text, "forbidden") || strings.Contains(text, "unauthorized")
}

var ErrPermissionDenied = errors.New("permission denied")
