package api

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	appImageRegistryGCContainerName = "registry"
	appImageRegistryGCConfigPath    = "/etc/docker/registry/config.yml"
	appImageRegistryGCTimeout       = 2 * time.Minute
)

func (s *Server) runAppImageRegistryGarbageCollect(ctx context.Context) error {
	namespace, err := s.controlPlaneNamespaceForRegistryGC()
	if err != nil {
		return err
	}

	listerFactory := s.newFilesystemPodLister
	if listerFactory == nil {
		listerFactory = func(namespace string) (filesystemPodLister, error) {
			return newKubeLogsClient(namespace)
		}
	}
	lister, err := listerFactory(namespace)
	if err != nil {
		return fmt.Errorf("registry cleanup is not available: %w", err)
	}

	pods, err := lister.listPodsBySelector(ctx, namespace, s.registryGCPodSelector())
	if err != nil {
		return fmt.Errorf("list registry pods: %w", err)
	}
	pod, err := selectLatestRunningRegistryPod(pods)
	if err != nil {
		return err
	}

	runner := s.filesystemExecRunner
	if runner == nil {
		runner = kubeFilesystemExecRunner{}
	}

	commandCtx, cancel := context.WithTimeout(ctx, appImageRegistryGCTimeout)
	defer cancel()

	if _, err := runner.Run(
		commandCtx,
		namespace,
		pod.Metadata.Name,
		appImageRegistryGCContainerName,
		nil,
		"registry",
		"garbage-collect",
		"--delete-untagged",
		appImageRegistryGCConfigPath,
	); err != nil {
		return fmt.Errorf("run registry garbage collector in pod %s: %w", pod.Metadata.Name, err)
	}

	return nil
}

func (s *Server) registryGCPodSelector() string {
	parts := []string{"app.kubernetes.io/component=registry"}
	if release := strings.TrimSpace(s.controlPlaneReleaseInstance); release != "" {
		parts = append(parts, "app.kubernetes.io/instance="+release)
	}
	return strings.Join(parts, ",")
}

func (s *Server) controlPlaneNamespaceForRegistryGC() (string, error) {
	if namespace := strings.TrimSpace(s.controlPlaneNamespace); namespace != "" {
		return namespace, nil
	}

	namespace, err := kubeNamespace()
	if err != nil {
		return "", fmt.Errorf("resolve control plane namespace: %w", err)
	}
	return namespace, nil
}

func selectLatestRunningRegistryPod(pods []kubePodInfo) (kubePodInfo, error) {
	if len(pods) == 0 {
		return kubePodInfo{}, fmt.Errorf("registry pod is not available")
	}

	sortPodsByCreation(pods)
	for index := len(pods) - 1; index >= 0; index-- {
		if strings.EqualFold(strings.TrimSpace(pods[index].Status.Phase), "Running") {
			return pods[index], nil
		}
	}
	return kubePodInfo{}, fmt.Errorf("registry pod is not running")
}
