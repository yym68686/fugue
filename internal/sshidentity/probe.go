package sshidentity

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"golang.org/x/crypto/ssh"
)

type Observation struct {
	Algorithm           string    `json:"algorithm,omitempty"`
	Fingerprint         string    `json:"fingerprint,omitempty"`
	KeyExchangeVerified bool      `json:"key_exchange_verified"`
	IdentityMatched     bool      `json:"identity_matched"`
	ObservedAt          time.Time `json:"observed_at"`
	Reason              string    `json:"reason"`
}

var (
	ErrInvalidProbe     = errors.New("SSH probe requires an explicit target, host identity, and bounded timeout")
	ErrIdentityMismatch = errors.New("SSH server identity does not match expected identity")
	ErrHandshake        = errors.New("SSH host identity handshake failed")
	errProbeComplete    = errors.New("SSH identity probe complete before authentication")
)

func Probe(ctx context.Context, target string, expected ssh.PublicKey, timeout time.Duration) (Observation, error) {
	observation := Observation{ObservedAt: time.Now().UTC(), Reason: "unverified"}
	host, portText, err := net.SplitHostPort(target)
	port, parseErr := strconv.Atoi(portText)
	if ctx == nil || expected == nil || err != nil || host == "" || parseErr != nil || port < 1 || port > 65535 || timeout <= 0 || timeout > 30*time.Second {
		return observation, ErrInvalidProbe
	}
	probeContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	connection, err := (&net.Dialer{}).DialContext(probeContext, "tcp", target)
	if err != nil {
		observation.Reason = "target_unreachable"
		if probeContext.Err() != nil {
			return observation, probeContext.Err()
		}
		return observation, ErrHandshake
	}
	defer connection.Close()
	stopCancellation := context.AfterFunc(probeContext, func() { _ = connection.Close() })
	defer stopCancellation()
	deadline, _ := probeContext.Deadline()
	if err := connection.SetDeadline(deadline); err != nil {
		return observation, ErrHandshake
	}
	algorithms := []string{expected.Type()}
	if expected.Type() == ssh.KeyAlgoRSA {
		algorithms = []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256}
	}
	results := make(chan Observation, 1)
	config := &ssh.ClientConfig{
		User:              "fugue-identity-probe",
		HostKeyAlgorithms: algorithms,
		HostKeyCallback: func(hostname string, remote net.Addr, observed ssh.PublicKey) error {
			result := Observation{
				Algorithm:           observed.Type(),
				Fingerprint:         ssh.FingerprintSHA256(observed),
				KeyExchangeVerified: true,
				IdentityMatched:     bytes.Equal(observed.Marshal(), expected.Marshal()),
				ObservedAt:          time.Now().UTC(),
				Reason:              "identity_verified",
			}
			if !result.IdentityMatched {
				result.Reason = "identity_mismatch"
			}
			results <- result
			return errProbeComplete
		},
	}
	client, _, _, handshakeErr := ssh.NewClientConn(connection, target, config)
	if client != nil {
		_ = client.Close()
	}
	select {
	case result := <-results:
		if !result.IdentityMatched {
			return result, ErrIdentityMismatch
		}
		return result, nil
	default:
	}
	observation.Reason = "handshake_failed"
	if probeContext.Err() != nil {
		return observation, probeContext.Err()
	}
	if timeoutError, ok := handshakeErr.(net.Error); ok && timeoutError.Timeout() {
		return observation, context.DeadlineExceeded
	}
	return observation, ErrHandshake
}
