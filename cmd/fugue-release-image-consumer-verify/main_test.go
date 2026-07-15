package main

import (
	"bytes"
	"testing"
)

func completeTargetArgs() []string {
	return []string{
		"--lock", "/private/lock.json",
		"--helm-release-json", "/private/target.json",
		"--repeated-helm-release-json", "/private/repeated.json",
		"--expected-lock-digest", "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		"--expected-head-sha", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"--expected-repository", "acme/fugue",
		"--expected-run-id", "123",
		"--expected-run-attempt", "1",
		"--release-fullname", "fugue-fugue",
		"--expected-release-name", "fugue",
		"--expected-namespace", "fugue-system",
		"--expected-live-revision", "7",
	}
}

func TestParseTargetFlagsRequiresCompleteUniqueFence(t *testing.T) {
	var stderr bytes.Buffer
	options, err := parseTargetFlags(completeTargetArgs(), &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if options.expectedLiveRevision != 7 || options.releaseFullname != "fugue-fugue" {
		t.Fatalf("unexpected options: %#v", options)
	}
	for name, args := range map[string][]string{
		"missing":    completeTargetArgs()[:2],
		"duplicate":  append(completeTargetArgs(), "--lock=/other/lock.json"),
		"revision":   append(completeTargetArgs()[:len(completeTargetArgs())-1], "0"),
		"positional": append(completeTargetArgs(), "extra"),
	} {
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			if _, err := parseTargetFlags(args, &output); err == nil {
				t.Fatal("invalid flags were accepted")
			}
		})
	}
}

func TestRunRejectsMissingAndUnknownSubcommandsWithoutStdout(t *testing.T) {
	for _, args := range [][]string{nil, {"unknown"}, {"target-render"}} {
		var stdout, stderr bytes.Buffer
		if exit := run(args, &stdout, &stderr); exit != 1 || stdout.Len() != 0 || stderr.Len() == 0 {
			t.Fatalf("args=%v exit=%d stdout=%q stderr=%q", args, exit, stdout.String(), stderr.String())
		}
	}
}
