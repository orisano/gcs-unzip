package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
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
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

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

	switch ext := path.Ext(src.Path); ext {
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
		w := o.NewWriter(ctx)
		w.ChunkSize = int(*chunkSize)
		defer w.Close()

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
		if err := w.Close(); err != nil {
			return fmt.Errorf("close writer: %w", err)
		}
		c := count.Add(1)
		if *gcInterval > 0 && int(c)%*gcInterval == 0 {
			runtime.GC()
		}
		if *verbose {
			bv := bytesValue(uploaded)
			log.Printf("%7d: -> %s(%s): %s", c, "gs://"+path.Join(o.BucketName(), o.ObjectName()), bv.String(), time.Now().Sub(start))
		}
		return nil
	}

	zf, err := os.Open(zipPath)
	if err != nil {
		return fmt.Errorf("open zip file: %w", err)
	}
	defer zf.Close()

	extractor, err := NewExtractor(zf)
	if err != nil {
		return fmt.Errorf("extractor: %w", err)
	}

	var largestFile string
	var largestSize uint64
	filesCount := 0
	for i := 0; i < extractor.Files(); i++ {
		if extractor.IsDir(i) {
			continue
		}
		filesCount++
		size := extractor.FileSize(i)
		if largestSize < size {
			largestFile = extractor.FileName(i)
			largestSize = size
		}
	}
	if *diskLimit < largestSize {
		bv := bytesValue(largestSize)
		return fmt.Errorf("no enough space(%s): %s", largestFile, bv.String())
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
					err := os.Remove(filepath.Join(workDir, name))
					if err != nil {
						log.Printf("failed to remove temp file: %v", err)
					}
				}()
				return upload(ctx, name)
			})
		}
	})

	writeBuf := make([]byte, 1*1024*1024)
	for i := 0; i < extractor.Files(); i++ {
		name := extractor.FileName(i)
		if extractor.IsDir(i) {
			if err := os.Mkdir(filepath.Join(workDir, name), 0700); err != nil {
				return fmt.Errorf("mkdir: %w", err)
			}
			continue
		}

		size := int64(extractor.FileSize(i))
		if err := diskSem.Acquire(ctx, size); err != nil {
			return fmt.Errorf("acquire disk sem: %w", err)
		}

		if err := writeTemporary(extractor, name, workDir, writeBuf); err != nil {
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

func parseGSURL(s string) (*url.URL, error) {
	u, err := url.ParseRequestURI(s)
	if err != nil {
		return nil, fmt.Errorf("parse uri: %w", err)
	}

	if u.Scheme != "gs" {
		return nil, fmt.Errorf("must start with gs://: %s", u.Scheme)
	}
	return u, nil
}

func download(ctx context.Context, gcs *storage.Client, workDir string, src *url.URL) (string, error) {
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

func writeTemporary(e Extractor, name, workDir string, buf []byte) error {
	rc, err := e.Open(name)
	if err != nil {
		return fmt.Errorf("open zip entry(%s): %w", name, err)
	}
	defer rc.Close()

	f, err := os.Create(filepath.Join(workDir, name))
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer f.Close()

	if _, err := io.CopyBuffer(f, rc, buf); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}
