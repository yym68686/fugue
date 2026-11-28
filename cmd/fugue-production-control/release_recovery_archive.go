package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"fugue/internal/releaseguardian"
)

const sealedReleaseArchiveName = "sealed-release.json"

func captureReleaseRecoveryArchive(ctx context.Context, namespace, component, kubeconfig, contextName, dir string, out io.Writer) error {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return errors.New("release recovery archive requires a new absolute private directory")
	}
	client, err := sealedRecoveryClient(kubeconfig, contextName)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	raw, report, err := releaseguardian.CaptureReleaseRecoveryArchive(ctx, client, namespace, component)
	if err != nil {
		return err
	}
	defer clear(raw)
	if err := writePrivateReleaseArchive(dir, raw); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

func writePrivateReleaseArchive(dir string, raw []byte) error {
	return writePrivateReleaseRecovery(dir, sealedReleaseArchiveName, raw)
}

func writePrivateReleaseRecovery(dir, filename string, raw []byte) error {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return errors.New("release recovery requires a new absolute private directory")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return errors.New("cannot create a new private release recovery directory")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	partial := filename + ".partial"
	file, err := root.OpenFile(partial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			_ = root.Remove(partial)
			_ = root.Remove(filename)
		}
	}()
	_, err = file.Write(raw)
	if err == nil {
		err = file.Sync()
	}
	if err := errors.Join(err, file.Close()); err != nil {
		return err
	}
	if err := root.Rename(partial, filename); err != nil {
		return err
	}
	top, err := root.Open(".")
	if err != nil {
		return err
	}
	if err := errors.Join(top.Sync(), top.Close()); err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(dir))
	if err != nil {
		return err
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		return err
	}
	published = true
	return nil
}
