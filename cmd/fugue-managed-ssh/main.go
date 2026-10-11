package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/sshkube"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var listen, health, artifact, trust, private, lkg, auditPath, helperPath, helperDigest, kubeconfig, shell string
	flag.StringVar(&listen, "listen", ":2222", "SSH listener")
	flag.StringVar(&health, "health-listen", "127.0.0.1:7837", "Health listener")
	flag.StringVar(&artifact, "artifact", "", "Signed managed SSH serving artifact")
	flag.StringVar(&trust, "trust", "", "Configuration verification public keys")
	flag.StringVar(&private, "host-key", "", "Protected application host private key")
	flag.StringVar(&lkg, "lkg", "", "Last verified serving artifact path")
	flag.StringVar(&auditPath, "audit", "", "Append-only audit path")
	flag.StringVar(&helperPath, "helper", "", "Static Linux runtime helper")
	flag.StringVar(&helperDigest, "helper-digest", "", "Expected helper SHA256 digest")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Explicit Kubernetes credentials; empty uses in-cluster credentials")
	flag.StringVar(&shell, "shell", "/bin/sh", "Bootstrap shell for staging the helper; sessions use the target account login shell")
	flag.Parse()
	for _, value := range []string{artifact, trust, private, lkg, auditPath, helperPath, helperDigest} {
		if strings.TrimSpace(value) == "" {
			log.Fatal("all serving configuration, identity, audit, and helper paths are required")
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, listen, health, artifact, trust, private, lkg, auditPath, helperPath, helperDigest, kubeconfig, shell); err != nil {
		log.Printf("managed SSH stopped: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, listen, health, artifact, trust, private, lkg, auditPath, helperPath, helperDigest, kubeconfig, shell string) error {
	var configuration *rest.Config
	var err error
	if kubeconfig != "" {
		configuration, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		configuration, err = rest.InClusterConfig()
	}
	if err != nil {
		return err
	}
	helper, err := os.ReadFile(helperPath)
	if err != nil {
		return err
	}
	executor, err := sshkube.NewKubernetes(configuration, helper, helperDigest, shell)
	if err != nil {
		return err
	}
	audit, err := managedssh.OpenFileAudit(auditPath)
	if err != nil {
		return err
	}
	defer audit.Close()
	server, err := managedssh.NewServer(executor, audit)
	if err != nil {
		return err
	}
	loader := managedssh.Loader{Server: server, ArtifactPath: artifact, TrustPath: trust, PrivateKeyPath: private, LKGPath: lkg}
	if err := loader.Load(ctx, false); err != nil {
		if restoreErr := loader.Load(ctx, true); restoreErr != nil {
			return fmt.Errorf("candidate and verified fallback unavailable: %w", errors.Join(err, restoreErr))
		}
		log.Print("candidate rejected; restored verified serving artifact")
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	healthListener, err := net.Listen("tcp", health)
	if err != nil {
		return err
	}
	defer healthListener.Close()
	mux := http.NewServeMux()
	verificationSlots := make(chan struct{}, 1)
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, request *http.Request) {
		select {
		case verificationSlots <- struct{}{}:
			defer func() { <-verificationSlots }()
		default:
			http.Error(writer, "runtime verification is busy", http.StatusTooManyRequests)
			return
		}
		verificationContext, cancel := context.WithTimeout(request.Context(), 10*time.Second)
		defer cancel()
		evidence, err := server.VerifyRuntime(verificationContext, os.Getenv("FUGUE_SSH_SERVER_POD_UID"))
		if err != nil {
			http.Error(writer, "runtime verification unavailable", http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(writer).Encode(evidence)
	})
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		status := server.Status()
		if !status.Serving {
			writer.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(writer).Encode(status)
	})
	healthServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	healthDone := make(chan error, 1)
	go func() { healthDone <- healthServer.Serve(healthListener) }()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = healthServer.Shutdown(shutdown)
		<-healthDone
	}()
	reloadContext, stopReload := context.WithCancel(ctx)
	defer stopReload()
	reloadDone := make(chan struct{})
	go func() {
		defer close(reloadDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		previousFailure := ""
		for {
			select {
			case <-reloadContext.Done():
				return
			case <-ticker.C:
				err := loader.Load(reloadContext, false)
				if err != nil {
					if err.Error() != previousFailure {
						log.Printf("candidate rejected; retaining verified state: %v", err)
					}
					previousFailure = err.Error()
				} else if previousFailure != "" {
					log.Print("serving configuration recovered")
					previousFailure = ""
				}
			}
		}
	}()
	servingContext, cancelServing := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelServing()
	servingDone := make(chan error, 1)
	go func() { servingDone <- server.Serve(servingContext, listener) }()
	select {
	case err = <-servingDone:
	case <-ctx.Done():
		_ = listener.Close()
		select {
		case err = <-servingDone:
		case <-time.After(50 * time.Second):
			cancelServing()
			err = <-servingDone
		}
	}
	stopReload()
	<-reloadDone
	return err
}
