package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"fugue/internal/model"
)

// putObjectStream uses bounded memory and never spools an unknown-size backup to
// the API node's disk. A producer error aborts the upload before publication.
func (b *dataObjectBackend) putObjectStream(ctx context.Context, key string, body io.Reader, partSize int64) (err error) {
	if partSize < dataMultipartMinPartSize || partSize > dataMultipartPartSize {
		return fmt.Errorf("invalid backup streaming part size: %d", partSize)
	}
	buffer := make([]byte, int(partSize))
	n, readErr := io.ReadFull(body, buffer)
	if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
		return b.putObjectSingle(ctx, key, bytes.NewReader(buffer[:n]), int64(n))
	}
	if readErr != nil {
		return readErr
	}
	uploadID, err := b.createMultipartUpload(ctx, key)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dataMultipartAbortTimeout)
			defer cancel()
			if abortErr := b.abortMultipartUpload(abortCtx, key, uploadID); abortErr != nil {
				err = errors.Join(err, fmt.Errorf("abort backup stream: %w", abortErr))
			}
		}
	}()
	var parts []model.DataTransferPart
	for number := int32(1); ; number++ {
		if int64(number) > dataMultipartMaxParts {
			return fmt.Errorf("backup stream exceeds multipart limit")
		}
		etag, uploadErr := b.putMultipartPart(ctx, key, uploadID, number, bytes.NewReader(buffer[:n]), int64(n))
		if uploadErr != nil {
			return uploadErr
		}
		parts = append(parts, model.DataTransferPart{PartNumber: number, Size: int64(n), ETag: etag, Completed: true})
		if readErr == io.ErrUnexpectedEOF {
			break
		}
		n, readErr = io.ReadFull(body, buffer)
		if readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return readErr
		}
	}
	if err := b.completeMultipartUpload(ctx, key, uploadID, parts); err != nil {
		return err
	}
	complete = true
	return nil
}

func (tx *backupObjectUploadTransaction) streamPGDump(ctx context.Context, executable, databaseURL, key string) (int64, string, error) {
	if tx == nil || tx.backend == nil || !backupFailedRunObjectKeyAllowed(tx.run, key) {
		return 0, "", fmt.Errorf("backup stream is not scoped to its run")
	}
	tx.keys = append(tx.keys, key)
	producerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	cmd := exec.CommandContext(producerCtx, executable, "--format=custom", "--no-owner", "--no-privileges", databaseURL)
	cmd.Stdout = writer
	stderr := &backupLimitedOutput{limit: 4096}
	cmd.Stderr = stderr
	cmd.WaitDelay = 5 * time.Second
	finished := make(chan error, 1)
	go func() {
		err := cmd.Run()
		if err != nil {
			err = fmt.Errorf("pg_dump failed: %w: %s", err, strings.TrimSpace(stderr.String()))
		}
		_ = writer.CloseWithError(err)
		finished <- err
	}()
	digest := sha256.New()
	count := &countingReader{r: io.TeeReader(reader, digest)}
	err := tx.backend.putObjectStream(ctx, key, count, dataMultipartPartSize)
	if err != nil {
		cancel()
		_ = reader.CloseWithError(err)
	}
	producerErr := <-finished
	if err != nil || producerErr != nil {
		return 0, "", errors.Join(err, producerErr)
	}
	return count.n, hex.EncodeToString(digest.Sum(nil)), nil
}

type backupLimitedOutput struct {
	bytes.Buffer
	limit int
}

func (b *backupLimitedOutput) Write(p []byte) (int, error) {
	n := len(p)
	if remaining := b.limit - b.Len(); remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = b.Buffer.Write(p)
	}
	return n, nil
}
