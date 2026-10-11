package managedssh

import (
	"context"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func TestDrainRefusesNewConnectionsWithoutClosingExistingSessions(test *testing.T) {
	configuration, signer, user := testConfiguration(test)
	server, address := serveForTest(test, &testExecutor{}, &testAudit{}, configuration, signer)
	connection := connectForTest(test, address, configuration.Target.User, signer, user)
	defer connection.Close()
	configuration.Generation++
	configuration.Draining = true
	if err := server.Activate(context.Background(), configuration, signer); err != nil {
		test.Fatal(err)
	}
	status := server.Status()
	if !status.Draining || status.ActiveConnections != 1 {
		test.Fatal("drain disrupted existing transport", status)
	}
	session, err := connection.NewSession()
	if err != nil {
		test.Fatal("drain closed an existing connection", err)
	}
	if err := session.Run("true"); err != nil {
		test.Fatal(err)
	}
	if unexpected, err := ssh.Dial("tcp", address, &ssh.ClientConfig{User: configuration.Target.User, Auth: []ssh.AuthMethod{ssh.PublicKeys(user)}, HostKeyCallback: ssh.FixedHostKey(signer.PublicKey()), Timeout: time.Second}); err == nil {
		unexpected.Close()
		test.Fatal("draining server admitted a new transport")
	}
}
