package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/klauspost/compress/gzip"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const local = false

func run() error {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of gcs-unzip <src> <dest>:\n")
		flag.PrintDefaults()
	}

	n := flag.Int("n", 24, "number of goroutines for uploading")
	verbose := flag.Bool("v", false, "show verbose output")
	bufSize := flagBytes("buf", 512*1024, "copy buffer size")
	chunkSize := flagBytes("chunk", 16*1024*1024, "upload chunk size")
	gcInterval := flag.Int("gc", 0, "gc interval")
	diskLimit := flagBytes("disk-limit", 50*1024*1024*1024, "disk limit")
	tmpDir := flag.String("tmp-dir", "", "temporary directory")
	gzipExt := flag.String("gzip-ext", "", "comma-separated list of file extensions to gzip before uploading")
	withMeta := flag.Bool("with-meta", false, "")
	skipTop := flag.Bool("skip-top", false, "")
	oldWindows := flag.Bool("old-windows", false, "")
	gcsMetadata := flag.String("gcs-meta", "", "metadata (comma separated key=value pairs)")

	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		return fmt.Errorf("invalid args")
	}

	src, err := parseGSURL(flag.Arg(0))
	if err != nil {
		return fmt.Errorf("parse src: %w", err)
	}

	dest, err := parseGSURL(flag.Arg(1))
	if err != nil {
		return fmt.Errorf("parse dest: %w", err)
	}

	switch ext := path.Ext(src.Path); strings.ToLower(ext) {
	case ".7z", ".zip":
	default:
		return fmt.Errorf("unsupported format: %s", ext)
	}

	ctx := context.Background()
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage client: %w", err)
	}

	workDir, err := os.MkdirTemp(*tmpDir, "")
	if err != nil {
		return fmt.Errorf("make work dir: %w", err)
	}
	defer func() {
		err := os.RemoveAll(workDir)
		if err != nil {
			log.Printf("failed to remove work dir: %v", err)
		}
	}()

	if *verbose {
		log.Printf("download %s", src.String())
	}
	zipPath, err := download(ctx, gcs, workDir, src)
	if err != nil {
		return fmt.Errorf("download zip: %w", err)
	}
	if *verbose {
		log.Printf("download finished: -> %s", zipPath)
	}

	bucket := gcs.Bucket(dest.Hostname())

	uploadBufPool := sync.Pool{
		New: func() any {
			return make([]byte, *bufSize)
		},
	}
	useGzip := map[string]bool{}
	if *gzipExt != "" {
		for _, ext := range strings.Split(*gzipExt, ",") {
			useGzip["."+strings.ToLower(ext)] = true
		}
	}
	var metadata map[string]string
	if *gcsMetadata != "" {
		metadata = map[string]string{}
		for _, kv := range strings.Split(*gcsMetadata, ",") {
			k, v, _ := strings.Cut(kv, "=")
			metadata[k] = v
		}
	}

	gzipWriterPool := sync.Pool{
		New: func() any {
			return gzip.NewWriter(io.Discard)
		},
	}
	var count atomic.Int64
	uploadsStart := time.Now()

	upload := func(ctx context.Context, f string) error {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		r, err := os.Open(filepath.Join(workDir, f))
		if err != nil {
			return fmt.Errorf("open upload file: %w", err)
		}
		defer r.Close()

		name := path.Join(dest.Path[1:], filepath.ToSlash(f))
		o := bucket.Object(name).Retryer(storage.WithPolicy(storage.RetryAlways))
		ow := o.NewWriter(ctx)
		ow.ChunkSize = int(*chunkSize)
		if len(metadata) > 0 {
			ow.Metadata = metadata
		}
		defer ow.Close()

		var w io.Writer
		var closeWriter func() error
		if useGzip[strings.ToLower(filepath.Ext(f))] {
			if sniff, err := io.ReadAll(io.NewSectionReader(r, 0, 512)); err == nil {
				ow.ContentType = http.DetectContentType(sniff)
			}
			ow.ContentEncoding = "gzip"
			gw := gzipWriterPool.Get().(*gzip.Writer)
			defer gzipWriterPool.Put(gw)
			gw.Reset(ow)

			closeWriter = func() error {
				if err := gw.Close(); err != nil {
					return err
				}
				return ow.Close()
			}
			w = gw
		} else {
			closeWriter = ow.Close
			w = ow
		}

		buf := uploadBufPool.Get().([]byte)
		defer uploadBufPool.Put(buf)

		var start time.Time
		if *verbose {
			start = time.Now()
		}
		uploaded, err := io.CopyBuffer(w, r, buf)
		if err != nil {
			return fmt.Errorf("upload: %w", err)
		}
		if err := closeWriter(); err != nil {
			return fmt.Errorf("close writer: %w", err)
		}
		c := count.Add(1)
		if *gcInterval > 0 && int(c)%*gcInterval == 0 {
			runtime.GC()
		}
		if *verbose {
			log.Printf("%7d: -> %s(%s): %s", c, "gs://"+path.Join(o.BucketName(), o.ObjectName()), bytesString(uint64(uploaded)), time.Now().Sub(start))
		}
		return nil
	}
	if local {
		upload = func(ctx context.Context, f string) error {
			log.Printf("-> %s", f)
			return nil
		}
	}

	zf, err := os.Open(zipPath)
	if err != nil {
		return fmt.Errorf("open zip file: %w", err)
	}
	defer zf.Close()

	archiveName := trimExt(filepath.Base(zf.Name()))

	extractor, err := NewExtractor(zf, *oldWindows)
	if err != nil {
		return fmt.Errorf("extractor: %w", err)
	}

	var largestFile string
	var largestSize uint64
	filesCount := 0

	topDirOnly := true
	for i := 0; i < extractor.Files(); i++ {
		if extractor.IsDir(i) {
			continue
		}
		name := extractor.FileName(i)
		if !*withMeta && isIgnoreMeta(name) {
			continue
		}
		if *skipTop && topDirOnly {
			top, _, _ := strings.Cut(name, string(os.PathSeparator))
			if top != archiveName {
				topDirOnly = false
			}
		}

		filesCount++
		size := extractor.FileSize(i)
		if largestSize < size {
			largestFile = name
			largestSize = size
		}
	}
	if *diskLimit < largestSize {
		return fmt.Errorf("no enough space(%s): %s", largestFile, bytesString(largestSize))
	}

	if *verbose {
		log.Printf("files: %d", filesCount)
	}

	uploadGroup, ctx := errgroup.WithContext(ctx)
	uploadGroup.SetLimit(*n + 1)
	diskSem := semaphore.NewWeighted(int64(*diskLimit))

	type uploadJob struct {
		name string
		size int64
	}
	uploadJobCh := make(chan uploadJob, filesCount)

	uploadGroup.Go(func() error {
		for {
			var size int64
			var name string
			select {
			case <-ctx.Done():
				return nil
			case job, ok := <-uploadJobCh:
				if !ok {
					return nil
				}
				size = job.size
				name = job.name
			}
			uploadGroup.Go(func() error {
				defer diskSem.Release(size)
				defer func() {
					if local {
						return
					}
					err := os.Remove(filepath.Join(workDir, name))
					if err != nil {
						log.Printf("failed to remove temp file: %v", err)
					}
				}()
				return upload(ctx, name)
			})
		}
	})

FILES:
	for i := 0; i < extractor.Files(); i++ {
		select {
		case <-ctx.Done():
			break FILES
		default:
		}
		name := extractor.FileName(i)
		if !*withMeta && isIgnoreMeta(name) {
			continue
		}
		if *skipTop && topDirOnly {
			name = strings.TrimPrefix(name, archiveName)
			if name != "" {
				name = name[1:]
			}
		}
		name = filepath.Join(archiveName, name)
		if extractor.IsDir(i) {
			if err := os.MkdirAll(filepath.Join(workDir, name), 0700); err != nil {
				return fmt.Errorf("mkdir: %w", err)
			}
			continue
		}
		size := int64(extractor.FileSize(i))
		if err := diskSem.Acquire(ctx, size); err != nil {
			return fmt.Errorf("acquire disk sem: %w", err)
		}

		if err := writeTemporary(ctx, extractor, i, name, workDir); err != nil {
			return fmt.Errorf("write temp: %w", err)
		}
		uploadJobCh <- uploadJob{name: name, size: size}
	}
	close(uploadJobCh)

	if err := uploadGroup.Wait(); err != nil {
		return fmt.Errorf("uploads: %w", err)
	}
	log.Printf("total: %s", time.Now().Sub(uploadsStart))
	return nil
}

func main() {
	log.SetPrefix("gcs-unzip: ")
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func flagBytes(name string, value uint64, usage string) *uint64 {
	p := new(uint64)
	*p = value
	flag.Var((*bytesValue)(p), name, usage)
	return p
}

var bytesUnits = []struct {
	suffix string
	value  uint64
}{
	{"g", 1 * 1024 * 1024 * 1024},
	{"m", 1 * 1024 * 1024},
	{"k", 1 * 1024},
	{"gb", 1 * 1024 * 1024 * 1024},
	{"mb", 1 * 1024 * 1024},
	{"kb", 1 * 1024},
	{"b", 1},
	{"", 1},
}

type bytesValue uint64

func (b *bytesValue) String() string {
	for _, u := range bytesUnits {
		if uint64(*b) >= u.value {
			return strconv.FormatUint(uint64(*b)/u.value, 10) + u.suffix
		}
	}
	return "0"
}

func (b *bytesValue) Set(s string) error {
	x := strings.ToLower(s)
	for _, u := range bytesUnits {
		if !strings.HasSuffix(x, u.suffix) {
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSuffix(x, u.suffix), 10, 64)
		if err != nil {
			return fmt.Errorf("parse(%s): %w", s, err)
		}
		*b = bytesValue(v * u.value)
		return nil
	}
	panic("unreachable")
}

func bytesString(x uint64) string {
	bv := bytesValue(x)
	return bv.String()
}

func parseGSURL(s string) (*url.URL, error) {
	u, err := url.ParseRequestURI(s)
	if err != nil {
		return nil, fmt.Errorf("parse uri: %w", err)
	}
	if local {
		return u, nil
	}
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("must start with gs://: %s", u.Scheme)
	}
	return u, nil
}

func download(ctx context.Context, gcs *storage.Client, workDir string, src *url.URL) (string, error) {
	if local {
		return strings.TrimPrefix(src.Path, "/"), nil
	}
	r, err := gcs.Bucket(src.Hostname()).Object(src.Path[1:]).NewReader(ctx)
	if err != nil {
		return "", fmt.Errorf("src reader: %w", err)
	}
	defer r.Close()
	p := filepath.Join(workDir, path.Base(src.Path))
	f, err := os.Create(p)
	if err != nil {
		return "", fmt.Errorf("create tmp file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		return "", fmt.Errorf("copy: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close tmp file: %w", err)
	}
	return p, nil
}

func writeTemporary(ctx context.Context, e Extractor, i int, name, workDir string) error {
	rc, err := e.Open(i)
	if err != nil {
		return fmt.Errorf("open zip entry(%s): %w", name, err)
	}
	defer rc.Close()

	tmpFile := filepath.Join(workDir, name)
	f, err := os.Create(tmpFile)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(tmpFile), 0700); err != nil {
			return fmt.Errorf("mkdir all: %w", err)
		}
		f, err = os.Create(tmpFile)
	}
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

func isIgnoreMeta(name string) bool {
	rest := name
	sep := string(os.PathSeparator)
	for rest != "" {
		n, after, found := strings.Cut(rest, sep)
		if !found {
			return n == ".DS_Store" || n == "Thumbs.db" || n == "__MACOSX"
		}
		rest = after
		if n == "__MACOSX" {
			return true
		}
	}
	return false
}

func trimExt(name string) string {
	return strings.TrimSuffix(name, filepath.Ext(name))
}
