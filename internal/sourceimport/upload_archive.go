package sourceimport

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type UploadArchiveFormat string

const (
	UploadArchiveFormatTarGz UploadArchiveFormat = "tar.gz"
	UploadArchiveFormatZip   UploadArchiveFormat = "zip"
)

func DetectUploadArchiveFormat(filename string, archiveData []byte) (UploadArchiveFormat, error) {
	formatFromName := uploadArchiveFormatFromName(filename)
	if formatFromName == "" {
		return "", fmt.Errorf("archive filename must end with .zip, .tgz, or .tar.gz")
	}

	formatFromData := uploadArchiveFormatFromBytes(archiveData)
	if formatFromData == "" {
		return "", fmt.Errorf("archive must be a .zip, .tgz, or .tar.gz file")
	}
	if formatFromName != formatFromData {
		return "", fmt.Errorf("archive filename extension does not match uploaded file contents")
	}
	return formatFromData, nil
}

func uploadArchiveFormatFromName(filename string) UploadArchiveFormat {
	lower := strings.ToLower(strings.TrimSpace(filename))
	switch {
	case strings.HasSuffix(lower, ".tgz"), strings.HasSuffix(lower, ".tar.gz"):
		return UploadArchiveFormatTarGz
	case strings.HasSuffix(lower, ".zip"):
		return UploadArchiveFormatZip
	default:
		return ""
	}
}

func uploadArchiveFormatFromBytes(archiveData []byte) UploadArchiveFormat {
	switch {
	case len(archiveData) >= 2 && bytes.HasPrefix(archiveData, []byte{0x1f, 0x8b}):
		return UploadArchiveFormatTarGz
	case len(archiveData) >= 4 &&
		(bytes.HasPrefix(archiveData, []byte("PK\x03\x04")) ||
			bytes.HasPrefix(archiveData, []byte("PK\x05\x06")) ||
			bytes.HasPrefix(archiveData, []byte("PK\x07\x08"))):
		return UploadArchiveFormatZip
	default:
		return ""
	}
}

func extractUploadArchive(dstDir, archiveFilename string, archiveData []byte) (string, error) {
	format, err := DetectUploadArchiveFormat(archiveFilename, archiveData)
	if err != nil {
		return "", err
	}

	switch format {
	case UploadArchiveFormatTarGz:
		if err := extractTarGzArchive(dstDir, archiveData); err != nil {
			return "", err
		}
	case UploadArchiveFormatZip:
		if err := extractZipArchive(dstDir, archiveData); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("unsupported uploaded archive format %q", format)
	}

	return normalizeExtractedUploadArchiveRoot(dstDir)
}

func extractTarGzArchive(dstDir string, archiveData []byte) error {
	gzipReader, err := gzip.NewReader(bytes.NewReader(archiveData))
	if err != nil {
		return fmt.Errorf("open uploaded archive: %w", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return fmt.Errorf("read uploaded archive: %w", err)
		}

		name, skip, err := normalizeUploadArchiveEntryName(header.Name)
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		targetPath, err := secureExtractedUploadPath(dstDir, name)
		if err != nil {
			return err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, 0o755); err != nil {
				return fmt.Errorf("mkdir %q: %w", name, err)
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
				return fmt.Errorf("mkdir parent for %q: %w", name, err)
			}
			mode := os.FileMode(header.Mode) & 0o777
			if mode == 0 {
				mode = 0o644
			}
			file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
			if err != nil {
				return fmt.Errorf("create %q: %w", name, err)
			}
			if _, err := io.Copy(file, tarReader); err != nil {
				file.Close()
				return fmt.Errorf("write %q: %w", name, err)
			}
			if err := file.Close(); err != nil {
				return fmt.Errorf("close %q: %w", name, err)
			}
		default:
			// Skip special files and links during local inspection extraction.
		}
	}
}

func extractZipArchive(dstDir string, archiveData []byte) error {
	reader, err := zip.NewReader(bytes.NewReader(archiveData), int64(len(archiveData)))
	if err != nil {
		return fmt.Errorf("open uploaded archive: %w", err)
	}

	for _, file := range reader.File {
		name, skip, err := normalizeUploadArchiveEntryName(file.Name)
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		targetPath, err := secureExtractedUploadPath(dstDir, name)
		if err != nil {
			return err
		}

		info := file.FileInfo()
		switch {
		case info.IsDir():
			if err := os.MkdirAll(targetPath, 0o755); err != nil {
				return fmt.Errorf("mkdir %q: %w", name, err)
			}
		case info.Mode().IsRegular():
			if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
				return fmt.Errorf("mkdir parent for %q: %w", name, err)
			}
			mode := info.Mode().Perm()
			if mode == 0 {
				mode = 0o644
			}
			src, err := file.Open()
			if err != nil {
				return fmt.Errorf("open %q: %w", name, err)
			}
			dst, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
			if err != nil {
				src.Close()
				return fmt.Errorf("create %q: %w", name, err)
			}
			if _, err := io.Copy(dst, src); err != nil {
				src.Close()
				dst.Close()
				return fmt.Errorf("write %q: %w", name, err)
			}
			if err := src.Close(); err != nil {
				dst.Close()
				return fmt.Errorf("close archive entry %q: %w", name, err)
			}
			if err := dst.Close(); err != nil {
				return fmt.Errorf("close %q: %w", name, err)
			}
		default:
			// Skip special files and links during local inspection extraction.
		}
	}

	return nil
}

func normalizeExtractedUploadArchiveRoot(rootDir string) (string, error) {
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return "", fmt.Errorf("read extracted archive root: %w", err)
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("uploaded archive does not contain any files")
	}
	if len(entries) == 1 && entries[0].IsDir() {
		return filepath.Join(rootDir, entries[0].Name()), nil
	}
	return rootDir, nil
}

func normalizeUploadArchiveEntryName(name string) (string, bool, error) {
	normalized := strings.ReplaceAll(strings.TrimSpace(name), "\\", "/")
	normalized = strings.TrimPrefix(normalized, "./")
	if normalized == "" {
		return "", true, nil
	}

	cleaned := path.Clean(normalized)
	if cleaned == "." || cleaned == "" {
		return "", true, nil
	}
	if strings.HasPrefix(cleaned, "/") || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", false, fmt.Errorf("archive entry %q escapes destination", name)
	}
	if shouldSkipUploadArchiveEntry(cleaned) {
		return "", true, nil
	}
	return cleaned, false, nil
}

func secureExtractedUploadPath(dstDir, relPath string) (string, error) {
	targetPath := filepath.Join(dstDir, filepath.FromSlash(relPath))
	cleanTargetPath := filepath.Clean(targetPath)
	cleanDstDir := filepath.Clean(dstDir)
	if cleanTargetPath != cleanDstDir &&
		!strings.HasPrefix(cleanTargetPath, cleanDstDir+string(filepath.Separator)) {
		return "", fmt.Errorf("archive entry %q escapes destination", relPath)
	}
	return cleanTargetPath, nil
}

func shouldSkipUploadArchiveEntry(name string) bool {
	parts := strings.Split(strings.TrimSpace(name), "/")
	for _, part := range parts {
		if part == "__MACOSX" {
			return true
		}
	}
	base := parts[len(parts)-1]
	return base == ".DS_Store" || strings.HasPrefix(base, "._")
}

func uploadArchiveBaseName(filename string) string {
	name := strings.TrimSpace(filename)
	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".tar.gz"):
		name = name[:len(name)-len(".tar.gz")]
	case strings.HasSuffix(lower, ".tgz"):
		name = name[:len(name)-len(".tgz")]
	case strings.HasSuffix(lower, ".zip"):
		name = name[:len(name)-len(".zip")]
	}
	name = strings.TrimSuffix(name, filepath.Ext(name))
	return name
}
