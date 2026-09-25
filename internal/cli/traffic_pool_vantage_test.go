package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTrustedVantageSSHRequiresPinnedDirectHost(t *testing.T) {
	dir := t.TempDir()
	known := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(known, []byte("192.0.2.30 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPinnedFixtureKey\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ssh := "#!/bin/sh\nprintf 'hostname 192.0.2.30\nport 22\nuserknownhostsfile " + known + "\n'\n"
	sshKeygen := "#!/bin/sh\nprintf '192.0.2.30 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPinnedFixtureKey\\n'\n"
	for name, script := range map[string]string{"ssh": ssh, "ssh-keygen": sshKeygen} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	trusted, err := trustedVantageSSH("remote-probe")
	if err != nil || trusted.Hostname != "192.0.2.30" || !strings.Contains(trusted.KnownHosts, "ssh-ed25519") {
		t.Fatalf("pinned host rejected: %+v %v", trusted, err)
	}
	if _, err := trustedVantageSSH("-oProxyCommand=bad"); err == nil {
		t.Fatal("SSH option injection accepted")
	}
	if err := os.WriteFile(filepath.Join(dir, "ssh-keygen"), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := trustedVantageSSH("remote-probe"); err == nil {
		t.Fatal("untrusted host accepted")
	}
}
