package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"fugue/internal/releaseguardian"
)

type authorityRuntime struct {
	controller *releaseguardian.AuthorityController
	groups     []string
	queues     map[string]chan struct{}
	once       sync.Once
}

func newAuthorityRuntime(store *releaseguardian.AuthorityStore, value string) (*authorityRuntime, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	verifiers := map[string]releaseguardian.CandidateCanaryVerifier{}
	var groups []string
	for _, raw := range strings.Split(value, ";") {
		fields := strings.Split(raw, ",")
		if len(fields) != 3 {
			return nil, errors.New("authority group must be group,key-id,verification-key-file")
		}
		group, keyID, keyFile := strings.TrimSpace(fields[0]), strings.TrimSpace(fields[1]), strings.TrimSpace(fields[2])
		if _, exists := verifiers[group]; exists || !filepath.IsAbs(keyFile) || filepath.Clean(keyFile) != keyFile {
			return nil, errors.New("authority group configuration is invalid")
		}
		key, err := os.ReadFile(keyFile)
		if err != nil || len(key) != 32 {
			return nil, errors.New("authority canary verification key is unavailable")
		}
		verifiers[group] = releaseguardian.CandidateCanaryVerifier{KeyID: keyID, Key: key}
		groups = append(groups, group)
	}
	controller, err := releaseguardian.NewAuthorityController(store, verifiers)
	if err != nil {
		return nil, err
	}
	runtime := &authorityRuntime{controller: controller, groups: groups, queues: map[string]chan struct{}{}}
	for _, group := range groups {
		runtime.queues[group] = make(chan struct{}, 1)
	}
	return runtime, nil
}

func (runtime *authorityRuntime) Start(ctx context.Context) {
	if runtime == nil {
		return
	}
	runtime.once.Do(func() {
		for _, group := range runtime.groups {
			group := group
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-runtime.queues[group]:
						receipt, changed, err := runtime.controller.Reconcile(ctx, group)
						if err != nil {
							fmt.Fprintf(os.Stderr, "authority reconcile %s: %v\n", group, err)
						} else if changed {
							fmt.Fprintf(os.Stderr, "authority reconcile %s: action=%s receipt=%s\n", group, receipt.Action, receipt.ReceiptDigest)
						}
					}
				}
			}()
		}
		runtime.EnqueueAll()
	})
}

func (runtime *authorityRuntime) EnqueueAll() {
	if runtime == nil {
		return
	}
	for _, group := range runtime.groups {
		select {
		case runtime.queues[group] <- struct{}{}:
		default:
		}
	}
}
