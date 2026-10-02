package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"fugue/internal/edgeimagegc"
	"k8s.io/client-go/rest"
)

func main() {
	apply := flag.Bool("apply", false, "Delete eligible exact image IDs; default observes only")
	once := flag.Bool("once", false, "Run one bounded sweep")
	repos := flag.String("repositories", "", "Comma-separated repository ownership allowlist")
	namespace := flag.String("namespace", "fugue-system", "Namespace holding immutable release records")
	hostRoot := flag.String("host-root", "/host", "Read-only host root mount for CRI client")
	statePath := flag.String("state", "/state/image-gc.json", "Persistent unused-since evidence")
	minimumAge := flag.Duration("minimum-unused-age", 24*time.Hour, "Continuously unreferenced age before deletion")
	interval := flag.Duration("interval", time.Hour, "Sweep interval")
	maxDeletes := flag.Int("maximum-deletes", 4, "Maximum exact images deleted per sweep")
	listen := flag.String("listen", ":7842", "Health and metrics bind address")
	flag.Parse()
	if *interval < time.Minute {
		log.Fatal("interval must be at least one minute")
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}
	cfg.Timeout = 15 * time.Second
	client, err := rest.HTTPClientFor(cfg)
	if err != nil {
		log.Fatal(err)
	}
	runtime := &edgeimagegc.CommandRuntime{Client: client, APIURL: cfg.Host, Namespace: *namespace, HostRoot: *hostRoot}
	policy := edgeimagegc.Policy{Apply: *apply, Repositories: strings.Split(*repos, ","), MinimumUnusedAge: *minimumAge, MaximumObservationGap: 2 * (*interval), MaximumDeletes: *maxDeletes}
	state, err := edgeimagegc.LoadState(*statePath)
	if err != nil {
		log.Fatalf("cannot load image GC evidence: %v", err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	var mu sync.Mutex
	var last edgeimagegc.Result
	var success time.Time
	var failures, deleted uint64
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ready := !success.IsZero() && time.Since(success) < 2*(*interval)
		mu.Unlock()
		if !ready {
			http.Error(w, "inventory unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		var stamp int64
		if !success.IsZero() {
			stamp = success.Unix()
		}
		fmt.Fprintf(w, "fugue_edge_image_gc_last_success_timestamp_seconds %d\nfugue_edge_image_gc_errors_total %d\nfugue_edge_image_gc_deleted_total %d\nfugue_edge_image_gc_inventory %d\nfugue_edge_image_gc_protected %d\nfugue_edge_image_gc_waiting %d\nfugue_edge_image_gc_candidates %d\n", stamp, failures, deleted, last.Inventory, last.Protected, last.Waiting, len(last.Candidates))
	})
	server := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("health server failed: %v", err)
			cancel()
		}
	}()
	defer server.Close()
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		sweepCtx, stop := context.WithTimeout(ctx, 5*time.Minute)
		result, sweepErr := edgeimagegc.Sweep(sweepCtx, runtime, policy, &state, time.Now().UTC())
		stop()
		// An evidence gap resets the age clock; time without observations must not
		// count towards continuously unused residency.
		if sweepErr != nil {
			state.UnusedSince = map[string]time.Time{}
		}
		saveErr := edgeimagegc.SaveState(*statePath, state)
		mu.Lock()
		last = result
		deleted += uint64(len(result.Deleted))
		failures += uint64(len(result.Failures))
		if sweepErr == nil && saveErr == nil {
			success = time.Now()
		} else {
			failures++
		}
		mu.Unlock()
		_ = json.NewEncoder(os.Stdout).Encode(result)
		if sweepErr != nil {
			log.Printf("image GC deferred: %v", sweepErr)
		}
		if saveErr != nil {
			log.Fatalf("image GC cannot persist evidence: %v", saveErr)
		}
		if *once {
			if sweepErr != nil {
				os.Exit(1)
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
