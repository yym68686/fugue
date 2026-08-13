package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"fugue/internal/releaseguardian"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type authorityRuntime struct {
	controller *releaseguardian.AuthorityController
	groups     []string
	queues     map[string]chan struct{}
	once       sync.Once
}

func newAuthorityRuntime(store *releaseguardian.AuthorityStore, value string) (*authorityRuntime, error) {
	return newAuthorityRuntimeWithActivators(store, nil, nil, "", value, "")
}

func newAuthorityRuntimeWithActivators(store *releaseguardian.AuthorityStore, client kubernetes.Interface, kubeConfig *rest.Config, namespace, value, activatorValue string) (*authorityRuntime, error) {
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
	activators := map[string]releaseguardian.FrontAuthorityActivator{}
	activatorValue = strings.TrimSpace(activatorValue)
	if activatorValue != "" && (client == nil || kubeConfig == nil || namespace == "") {
		return nil, errors.New("authority activator runtime dependency is unavailable")
	}
	var activatorEntries []string
	if activatorValue != "" {
		activatorEntries = strings.Split(activatorValue, ";")
	}
	for _, raw := range activatorEntries {
		fields := strings.Split(raw, ",")
		if len(fields) != 8 {
			return nil, errors.New("authority activator must be group,endpoint,keyring-file,nodes,address,host,path,body-digest")
		}
		group := strings.TrimSpace(fields[0])
		nodes, nodesErr := strconv.Atoi(strings.TrimSpace(fields[3]))
		if nodesErr != nil || nodes < 1 || nodes > 100 || activators[group] != nil || verifiers[group].Key == nil {
			return nil, errors.New("authority activator configuration is invalid")
		}
		front, err := newFrontAuthorityActivator(client, &kubePodExecutor{config: kubeConfig, client: client}, frontAuthorityConfig{
			GroupID: group, Namespace: namespace, ExpectedNodes: nodes, RouteAddress: strings.TrimSpace(fields[4]),
			RouteHost: strings.TrimSpace(fields[5]), RoutePath: strings.TrimSpace(fields[6]), RouteBodyDigest: strings.TrimSpace(fields[7]),
		}, os.Getenv("FUGUE_RELEASE_GUARDIAN_POD_UID")+":"+group)
		if err != nil {
			return nil, err
		}
		groupActivator, err := newGroupAuthorityActivator(front, groupAuthorityConfig{GroupID: group,
			Endpoint: strings.TrimSpace(fields[1]), KeyringFile: strings.TrimSpace(fields[2])})
		if err != nil {
			return nil, err
		}
		activators[group] = groupActivator
	}
	var controller *releaseguardian.AuthorityController
	var err error
	if len(activators) == 0 {
		controller, err = releaseguardian.NewAuthorityController(store, verifiers)
	} else if len(activators) != len(verifiers) {
		return nil, errors.New("each authority verifier requires one production activator")
	} else {
		controller, err = releaseguardian.NewAuthorityControllerWithActivators(store, verifiers, activators)
	}
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
