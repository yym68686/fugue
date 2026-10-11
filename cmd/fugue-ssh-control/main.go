package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"fugue/internal/model"
	"fugue/internal/sshcontrol"
	"fugue/internal/store"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var namespace, runtimeFile, signingFile, identityFile, kubeconfig, health string
	var once bool
	flag.StringVar(&namespace, "namespace", "fugue-ssh", "Dedicated managed SSH namespace")
	flag.StringVar(&runtimeFile, "runtime-config", "", "Independent runtime and immutable release configuration file")
	flag.StringVar(&signingFile, "signing-keyring", "", "Protected configuration signing keyring file")
	flag.StringVar(&identityFile, "identity-keyring", "", "Protected identity encryption keyring file")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Explicit Kubernetes configuration; empty uses in-cluster credentials")
	flag.StringVar(&health, "health-listen", ":7838", "Controller health listener")
	flag.BoolVar(&once, "once", false, "Reconcile one snapshot of configured intents")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, namespace, runtimeFile, signingFile, identityFile, kubeconfig, health, once); err != nil {
		log.Printf("managed SSH controller stopped: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, namespace, runtimeFile, signingFile, identityFile, kubeconfig, health string, once bool) error {
	databaseURL := os.Getenv("FUGUE_DATABASE_URL")
	if databaseURL == "" || namespace == "" || runtimeFile == "" || signingFile == "" || identityFile == "" {
		return fmt.Errorf("database, namespace, runtime config, identity keyring, and signing keyring are required")
	}
	stateStore := store.New("", databaseURL)
	if err := stateStore.Init(); err != nil {
		return fmt.Errorf("managed SSH store initialization failed")
	}
	stateStore.ConfigureSSHIdentityKeyFile(identityFile)
	var configuration *rest.Config
	var err error
	if kubeconfig == "" {
		configuration, err = rest.InClusterConfig()
	} else {
		configuration, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		return err
	}
	configuration.Timeout = 15 * time.Second
	runtime, err := sshcontrol.NewKubernetesRuntime(configuration, runtimeFile)
	if err != nil {
		return err
	}
	reconciler := &sshcontrol.Reconciler{Store: stateStore, Runtime: runtime, Namespace: namespace, SigningKeyFile: signingFile}
	var completed, failed atomic.Uint64
	var lastSweep atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"status": "ok", "reconciliations": completed.Load(), "failures": failed.Load(), "last_sweep_unix": lastSweep.Load()})
	})
	healthServer := &http.Server{Addr: health, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	healthErrors := make(chan error, 1)
	if !once {
		go func() { healthErrors <- healthServer.ListenAndServe() }()
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = healthServer.Shutdown(shutdown)
		}()
	}
	sweep := func() error {
		intents, err := stateStore.ListManagedSSHIntents()
		if err != nil {
			return fmt.Errorf("managed SSH intent inventory unavailable")
		}
		jobs := make(chan model.ManagedSSHIntent)
		var workers sync.WaitGroup
		var errorsMu sync.Mutex
		var failures []error
		for worker := 0; worker < 8; worker++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for intent := range jobs {
					appContext, cancel := context.WithTimeout(ctx, 45*time.Second)
					err := reconciler.Reconcile(appContext, intent.TenantID, intent.AppID)
					cancel()
					completed.Add(1)
					if err != nil {
						failed.Add(1)
						log.Printf("managed SSH reconciliation failed; app_id=%s error=%v", intent.AppID, err)
						errorsMu.Lock()
						failures = append(failures, fmt.Errorf("app %s reconciliation failed", intent.AppID))
						errorsMu.Unlock()
					}
				}
			}()
		}
		for _, intent := range intents {
			select {
			case <-ctx.Done():
				close(jobs)
				workers.Wait()
				return ctx.Err()
			case jobs <- intent:
			}
		}
		close(jobs)
		workers.Wait()
		lastSweep.Store(time.Now().Unix())
		return errors.Join(failures...)
	}
	for {
		if err := sweep(); err != nil {
			if once {
				return err
			}
			log.Printf("managed SSH sweep incomplete: %v", err)
		}
		if once {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-healthErrors:
			return err
		case <-time.After(10 * time.Second):
		}
	}
}
