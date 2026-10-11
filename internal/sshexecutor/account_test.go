package sshexecutor

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/user"
	"strings"
	"testing"
	"time"

	"fugue/internal/managedssh"
)

func TestAccountLoginShellUsesBoundAccountNotInheritedEnvironment(test *testing.T) {
	account := &user.User{Username: "workspace", Uid: "1000", HomeDir: "/workspace"}
	for _, selected := range []string{"/bin/bash", "/bin/ash", "/bin/zsh", "/bin/false", ""} {
		content := "other:x:1001:1001::/other:/bin/sh\nworkspace:x:1000:1000::/workspace:" + selected + "\n"
		actual, err := readAccountLoginShell(strings.NewReader(content), account)
		if selected == "" {
			selected = "/bin/sh"
		}
		if err != nil || actual != selected {
			test.Fatal("account shell was replaced by the bootstrap shell", actual, err)
		}
	}
	for _, content := range []string{"", "workspace:x:1001:1001::/workspace:/bin/bash\n", "workspace:x:1000:1000::/other:/bin/bash\n", "workspace:x:1000:1000::/workspace:bash\n", "workspace:x:1000:1000::/workspace:/bin/../bin/bash\n", "workspace:x:1000:1000::/workspace:/bin/bash\nworkspace:x:1000:1000::/workspace:/bin/sh\n"} {
		if _, err := readAccountLoginShell(strings.NewReader(content), account); err == nil {
			test.Fatal("missing, substituted, or ambiguous account was accepted")
		}
	}
}

func TestBashSessionPreservesAccountEnvironmentAndLanguage(test *testing.T) {
	if _, err := os.Stat("/bin/bash"); err != nil {
		test.Skip("requires Bash for the non-POSIX account shell regression")
	}
	target := processTarget(test)
	process, err := NewProcess(target, "/bin/bash")
	if err != nil {
		test.Fatal(err)
	}
	test.Setenv("SHELL", "/bin/sh")
	test.Setenv("HOME", "/incorrect-container-home")
	request := managedssh.SessionRequest{Kind: "exec", Command: "values=(one two); [[ ${values[1]} == two ]] || exit 9; printf '%s\\n' \"$SHELL\" \"$HOME\" \"$USER\" \"$LOGNAME\"", Environment: map[string]string{"SHELL": "/bin/false", "HOME": "/client-home", "USER": "foreign", "LOGNAME": "foreign"}}
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	exit, err := process.Run(ctx, target, request, managedssh.SessionIO{Stdin: strings.NewReader(""), Stdout: &output, Stderr: &output})
	if err != nil || exit.Code != 0 || output.String() != "/bin/bash\n"+process.home+"\n"+process.user+"\n"+process.user+"\n" {
		test.Fatal("Bash command or authoritative account environment was lost", output.String(), exit, err)
	}
	request.Kind, request.Command = "shell", ""
	process.shell = "/bin/sh"
	output.Reset()
	exit, err = process.Run(ctx, target, request, managedssh.SessionIO{Stdin: strings.NewReader("printf 'LOGIN_NAME=%s\\n' \"$0\"\nexit\n"), Stdout: &output, Stderr: io.Discard})
	if err != nil || exit.Code != 0 || !strings.Contains(output.String(), "LOGIN_NAME=-sh") {
		test.Fatal("shell request was not started as a login shell", output.String(), exit, err)
	}
}
