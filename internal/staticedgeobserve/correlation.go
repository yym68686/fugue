package staticedgeobserve

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const CorrelationHeader = "X-Fugue-Observation"

type Parent struct {
	RequestID string `json:"request_id"`
	SpanID    string `json:"span_id"`
	NodeID    string `json:"node_id"`
	Issued    int64  `json:"issued"`
}

// Correlation authenticates metadata, never permission to route a request.
// Entry handlers always replace external headers. Non-entry handlers require
// both an authorized transport peer and a valid MAC before accepting a parent.
func SignParent(p Parent, key []byte) (string, error) {
	if len(key) < 32 || !ValidID(p.RequestID) || !ValidID(p.SpanID) || !ValidID(p.NodeID) {
		return "", errors.New("invalid correlation key or identity")
	}
	b, _ := json.Marshal(p)
	m := hmac.New(sha256.New, key)
	m.Write(b)
	return base64.RawURLEncoding.EncodeToString(b) + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil)), nil
}
func VerifyParent(value string, key []byte, peerAuthorized bool, now time.Time) (Parent, error) {
	var p Parent
	if !peerAuthorized || len(key) < 32 || len(value) > 1024 {
		return p, errors.New("untrusted correlation")
	}
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return p, errors.New("invalid correlation")
	}
	b, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return p, errors.New("invalid correlation")
	}
	mac, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return p, errors.New("invalid correlation")
	}
	m := hmac.New(sha256.New, key)
	m.Write(b)
	if !hmac.Equal(m.Sum(nil), mac) {
		return p, errors.New("invalid correlation authentication")
	}
	if json.Unmarshal(b, &p) != nil || !ValidID(p.RequestID) || !ValidID(p.SpanID) || !ValidID(p.NodeID) || p.Issued > now.Unix()+60 || now.Unix()-p.Issued > 86400 {
		return Parent{}, errors.New("invalid or expired correlation")
	}
	return p, nil
}
