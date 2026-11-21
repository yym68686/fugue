package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCheckPackagesRejectsMismatches(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	out := filepath.Join(t.TempDir(), "trust")
	if err := createTrust(pilot(), out, now, 3*time.Hour); err != nil {
		t.Fatal(err)
	}
	get := func(path ...string) string {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(append([]string{out}, path...)...))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	var receipt trustReceipt
	if err := json.Unmarshal([]byte(get("trust-receipt.json")), &receipt); err != nil {
		t.Fatal(err)
	}
	in := checkInput{CA: get("issuer", "ca.pem"), Root: receipt.Root, PolicyVerify: receipt.PolicyVerify}
	for _, l := range receipt.Leaves {
		in.Packages = append(in.Packages, leafPackage{peerIdentity: l.peerIdentity, Leaf: l.Leaf,
			Cert: get(l.Directory, "leaf.pem"), Key: get(l.Directory, "key.pem")})
	}
	check := func(i checkInput, clock time.Time) error {
		t.Helper()
		raw, err := json.Marshal(i)
		if err != nil {
			t.Fatal(err)
		}
		return checkPackages(bytes.NewReader(raw), clock)
	}
	if err := check(in, now); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*checkInput){
		"another slot's private key": func(i *checkInput) { i.Packages[0].Key = i.Packages[1].Key },
		"another slot's SAN":         func(i *checkInput) { i.Packages[0].TLSName = "foreign.example.test" },
		"issuer key":                 func(i *checkInput) { i.Packages[0].Key = get("issuer", "ca-key.pem") },
		"extra issuer material":      func(i *checkInput) { i.Packages[0].Key += get("issuer", "policy-signer.pem") },
		"extra CA":                   func(i *checkInput) { i.CA += i.CA },
		"wrong fingerprint":          func(i *checkInput) { i.Packages[0].Leaf = i.Packages[1].Leaf },
	} {
		t.Run(name, func(t *testing.T) {
			x := in
			x.Packages = append([]leafPackage(nil), in.Packages...)
			mutate(&x)
			if err := check(x, now); err == nil {
				t.Fatal("foreign or ambiguous private material passed")
			}
		})
	}
	if err := check(in, now.Add(time.Hour)); err == nil {
		t.Fatal("allowed package without sufficient release validity")
	}
}
