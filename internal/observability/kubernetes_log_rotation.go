package observability

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
)

const rotationReadLimit = 64 << 20

var rotationUID = regexp.MustCompile("^[a-zA-Z0-9-]+$")
var rotationSuffix = regexp.MustCompile("^[0-9]{8}-[0-9]{6}$")

// The Pod UID and container restart number bind retained files to the same
// source as the API cursor. No caller-supplied path is accepted.
func rotationLogDirectory(t kubernetesLogTarget) (string, string, bool) {
	if len(validation.IsDNS1123Subdomain(t.pod.Spec.NodeName)) != 0 || t.pod.Spec.NodeName == "" ||
		len(validation.IsDNS1123Subdomain(t.pod.Name)) != 0 || t.pod.Name == "" ||
		len(validation.IsDNS1123Label(t.pod.Namespace)) != 0 || t.pod.Namespace == "" ||
		len(validation.IsDNS1123Label(t.container)) != 0 || t.container == "" ||
		len(t.pod.UID) > 128 || !rotationUID.MatchString(string(t.pod.UID)) {
		return "", "", false
	}
	statuses := append(append(append([]corev1.ContainerStatus(nil), t.pod.Status.ContainerStatuses...), t.pod.Status.InitContainerStatuses...), t.pod.Status.EphemeralContainerStatuses...)
	for _, s := range statuses {
		if s.Name != t.container {
			continue
		}
		restart := int(s.RestartCount)
		if t.previous {
			if s.LastTerminationState.Terminated == nil {
				return "", "", false
			}
			restart--
		}
		if restart < 0 {
			return "", "", false
		}
		path := "/api/v1/nodes/" + url.PathEscape(t.pod.Spec.NodeName) + "/proxy/logs/pods/" +
			url.PathEscape(t.pod.Namespace+"_"+t.pod.Name+"_"+string(t.pod.UID)) + "/" + url.PathEscape(t.container) + "/"
		return path, strconv.Itoa(restart) + ".log", true
	}
	return "", "", false
}

type rotationLogFile struct{ base, name string }

func parseRotationLogFiles(body io.Reader, prefix string) ([]rotationLogFile, error) {
	raw, err := io.ReadAll(io.LimitReader(body, (64<<10)+1))
	if err != nil {
		return nil, fmt.Errorf("read retained log directory: %w", err)
	}
	if len(raw) > 64<<10 {
		return nil, fmt.Errorf("retained log directory exceeds bound")
	}
	files := map[string]string{}
	z := html.NewTokenizer(strings.NewReader(string(raw)))
	for {
		kind := z.Next()
		if kind == html.ErrorToken {
			if z.Err() != io.EOF {
				return nil, z.Err()
			}
			break
		}
		if kind != html.StartTagToken {
			continue
		}
		tag := z.Token()
		if tag.Data != "a" {
			continue
		}
		for _, a := range tag.Attr {
			if a.Key != "href" {
				continue
			}
			name := a.Val
			base := strings.TrimSuffix(name, ".gz")
			if base != prefix {
				suffix, found := strings.CutPrefix(base, prefix+".")
				if !found || !rotationSuffix.MatchString(suffix) {
					continue
				}
				if _, err := time.Parse("20060102-150405", suffix); err != nil {
					continue
				}
			} else if name != prefix {
				continue
			}
			if old := files[base]; old == "" || !strings.HasSuffix(name, ".gz") {
				files[base] = name
			}
			if len(files) > 16 {
				return nil, fmt.Errorf("retained log file count exceeds bound")
			}
		}
	}
	if files[prefix] == "" {
		return nil, fmt.Errorf("current container log is absent from retained inventory")
	}
	out := make([]rotationLogFile, 0, len(files))
	for base, name := range files {
		out = append(out, rotationLogFile{base, name})
	}
	sort.Slice(out, func(i, j int) bool {
		if (out[i].base == prefix) != (out[j].base == prefix) {
			return out[j].base == prefix
		}
		return out[i].base < out[j].base
	})
	return out, nil
}

// Open the current raw file between two inventory reads. A concurrent rotation
// retries without moving the cursor. Once open, its file descriptor retains
// that generation even if a later rotation renames it. Compression renames are
// equivalent; a fetch retries only the same basename's compressed variant.
func (c *kubernetesLogCollector) openRotationLogs(ctx context.Context, target kubernetesLogTarget) (*rotationLogReader, error) {
	directory, prefix, ok := rotationLogDirectory(target)
	if !ok {
		return nil, fmt.Errorf("retained log source identity is unavailable")
	}
	get := func(name string) (io.ReadCloser, error) {
		return c.client.CoreV1().RESTClient().Get().AbsPath(directory + name).Stream(ctx)
	}
	list := func() ([]rotationLogFile, error) {
		body, err := get("")
		if err != nil {
			return nil, err
		}
		defer body.Close()
		return parseRotationLogFiles(body, prefix)
	}
	before, err := list()
	if err != nil {
		return nil, err
	}
	current, err := get(prefix)
	if err != nil {
		return nil, err
	}
	after, err := list()
	bases := func(files []rotationLogFile) []string {
		out := make([]string, len(files))
		for i, f := range files {
			out[i] = f.base
		}
		return out
	}
	if err != nil {
		current.Close()
		return nil, fmt.Errorf("recheck retained log inventory: %w", err)
	}
	if !reflect.DeepEqual(bases(before), bases(after)) {
		current.Close()
		return nil, fmt.Errorf("retained log inventory changed")
	}
	return &rotationLogReader{files: after, current: current, get: get, maxLine: c.pipeline.cfg.KubernetesLogMaxLineBytes, partial: map[string]*rotationCRIRecord{}}, nil
}

type rotationCRIRecord struct {
	at       string
	body     strings.Builder
	complete bool
}

type rotationLogReader struct {
	files        []rotationLogFile
	current      io.ReadCloser
	active       io.ReadCloser
	get          func(string) (io.ReadCloser, error)
	scanner      *bufio.Scanner
	gzip         *gzip.Reader
	maxLine      int
	index        int
	decodedBytes int64
	pendingBytes int
	partial      map[string]*rotationCRIRecord
	records      []*rotationCRIRecord
	output       string
}

func (r *rotationLogReader) Close() error {
	if r.gzip != nil {
		r.gzip.Close()
		r.gzip = nil
	}
	if r.active != nil {
		r.active.Close()
		r.active = nil
	}
	if r.current != nil {
		r.current.Close()
		r.current = nil
	}
	return nil
}

func (r *rotationLogReader) nextFile() error {
	if r.gzip != nil {
		r.gzip.Close()
		r.gzip = nil
	}
	if r.active != nil {
		r.active.Close()
		r.active = nil
	}
	if r.index == len(r.files) {
		return io.EOF
	}
	f := r.files[r.index]
	r.index++
	var body io.ReadCloser
	var err error
	if r.index == len(r.files) {
		body, r.current = r.current, nil
	} else {
		body, err = r.get(f.name)
		if apierrors.IsNotFound(err) {
			if strings.HasSuffix(f.name, ".gz") {
				f.name = f.base
			} else {
				f.name = f.base + ".gz"
			}
			body, err = r.get(f.name)
		}
	}
	if err != nil {
		return err
	}
	if body == nil {
		return fmt.Errorf("retained log stream is unavailable")
	}
	r.active = body
	var reader io.Reader = io.LimitReader(body, rotationReadLimit+1)
	if strings.HasSuffix(f.name, ".gz") {
		z, err := gzip.NewReader(reader)
		if err != nil {
			return err
		}
		reader = z
		r.gzip = z
	}
	r.scanner = bufio.NewScanner(reader)
	r.scanner.Buffer(make([]byte, 0, min(64<<10, r.maxLine+64)), r.maxLine+64)
	r.scanner.Split(scanCompleteCRILine)
	return nil
}

func (r *rotationLogReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for r.output == "" {
		if len(r.records) > 0 && r.records[0].complete {
			v := r.records[0]
			r.records[0] = nil
			r.records = r.records[1:]
			r.pendingBytes -= v.body.Len()
			r.output = v.at + " " + v.body.String() + "\n"
			continue
		}
		if r.scanner == nil {
			if err := r.nextFile(); err != nil {
				if err == io.EOF && len(r.records) != 0 {
					return 0, io.ErrUnexpectedEOF
				}
				return 0, err
			}
		}
		if !r.scanner.Scan() {
			if err := r.scanner.Err(); err != nil {
				return 0, err
			}
			r.scanner = nil
			continue
		}
		line := r.scanner.Text()
		r.decodedBytes += int64(len(line) + 1)
		if r.decodedBytes > rotationReadLimit {
			return 0, fmt.Errorf("retained log decoded bytes exceed bound")
		}
		parts := strings.SplitN(line, " ", 4)
		if len(parts) != 4 || (parts[1] != "stdout" && parts[1] != "stderr") || (parts[2] != "P" && parts[2] != "F") {
			return 0, fmt.Errorf("invalid retained CRI record")
		}
		if _, err := time.Parse(time.RFC3339Nano, parts[0]); err != nil {
			return 0, fmt.Errorf("invalid retained CRI timestamp")
		}
		v := r.partial[parts[1]]
		if v == nil {
			v = &rotationCRIRecord{at: parts[0]}
			r.records = append(r.records, v)
		}
		if v.body.Len()+len(parts[3]) > r.maxLine || r.pendingBytes+len(parts[3]) > 2*r.maxLine || len(r.records) > 4096 {
			return 0, fmt.Errorf("retained CRI record buffer exceeds bound")
		}
		v.body.WriteString(parts[3])
		r.pendingBytes += len(parts[3])
		v.complete = parts[2] == "F"
		if v.complete {
			delete(r.partial, parts[1])
		} else {
			r.partial[parts[1]] = v
		}
	}
	n := copy(p, r.output)
	r.output = r.output[n:]
	return n, nil
}

func scanCompleteCRILine(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF && len(data) > 0 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	return 0, nil, nil
}
