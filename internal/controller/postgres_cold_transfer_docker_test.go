package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Exercises the actual producer shell and evidence decoder across two different
// image user/group databases. Neither production Kubernetes nor the network is used.
func TestColdTransferNumericOwnershipAcrossImages(t *testing.T) {
	if os.Getenv("FUGUE_COLD_DOCKER_TEST") != "1" {
		t.Skip("requires disposable local Docker volumes")
	}
	const pg = "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie"
	const receiver = "public.ecr.aws/docker/library/busybox:1.36"
	run := func(args ...string) []byte {
		t.Helper()
		out, err := exec.Command("docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker: %v: %s", err, out)
		}
		return out
	}
	prefix := fmt.Sprintf("fugue-cold-test-%d", time.Now().UnixNano())
	source, target := prefix+"-source", prefix+"-target"
	for _, volume := range []string{source, target} {
		run("volume", "create", volume)
		v := volume
		t.Cleanup(func() { run("volume", "rm", v) })
	}
	run("run", "--rm", "--network", "none", "--user", "0:0", "--entrypoint", "sh", "-v", source+":/data", pg, "-ec", `chown 26:26 /data; runuser -u postgres -- initdb -D /data/pgdata -A trust --no-locale >/dev/null; chown -R 26:26 /data/pgdata`)
	probe := func(volume string) coldFileEvidence {
		out := run("run", "--rm", "--network", "none", "--user", "26:26", "--entrypoint", "python3", "-v", volume+":/data:ro", pg, "-c", coldProbeProgram, "seed", "/data/pgdata")
		var ev coldFileEvidence
		if err := json.Unmarshal(out, &ev); err != nil {
			t.Fatalf("decode real probe: %v", err)
		}
		if len(ev.MetadataEntries) < 1 {
			t.Fatal("missing metadata witnesses")
		}
		return ev
	}
	before := probe(source)
	transfer := func(command string) coldFileEvidence {
		producer := exec.Command("docker", "run", "--rm", "--network", "none", "--user", "26:26", "--entrypoint", "sh", "-v", source+":/data:ro", pg, "-ec", command, "cold-copy", "/data/pgdata")
		reader, err := producer.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		producer.Stderr = &stderr
		if err := producer.Start(); err != nil {
			t.Fatal(err)
		}
		consumer := exec.Command("docker", "run", "--rm", "--network", "none", "-i", "--entrypoint", "sh", "-v", target+":/data", receiver, "-ec", `tar -xpf - -C /data`)
		consumer.Stdin = reader
		out, err := consumer.CombinedOutput()
		reader.Close()
		if producerErr := producer.Wait(); producerErr != nil {
			t.Fatalf("producer: %v %s", producerErr, stderr.String())
		}
		if err != nil {
			t.Fatalf("consumer: %v %s", err, out)
		}
		return probe(target)
	}
	old := transfer(strings.Replace(coldTarCommand, " --numeric-owner", "", 1))
	if old.ContentDigest != before.ContentDigest || old.MetadataDigest == before.MetadataDigest {
		t.Fatal("failed to reproduce identical content with changed ownership")
	}
	detail := coldMetadataMismatchDetail(before.MetadataEntries, old.MetadataEntries)
	if !strings.Contains(detail, "gid=26") || !strings.Contains(detail, "gid=32") {
		t.Fatal("wrong ownership witness", detail)
	}
	fixed := transfer(coldTarCommand)
	if fixed.FileDigest != before.FileDigest || fixed.ControlDigest != before.ControlDigest {
		t.Fatal("numeric-owner transfer changed files/ownership", coldMetadataMismatchDetail(before.MetadataEntries, fixed.MetadataEntries))
	}
	if probe(source).FileDigest != before.FileDigest {
		t.Fatal("source modified")
	}
	t.Log("old command changed GID 26 to 32; production numeric-owner command preserves full file manifest and source")
}
