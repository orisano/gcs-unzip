package main

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/bodgit/sevenzip"
	"github.com/klauspost/compress/zip"
	"golang.org/x/text/encoding/japanese"
)

type Extractor interface {
	Files() int
	FileName(int) string
	FileSize(int) uint64
	IsDir(int) bool
	Open(string) (io.ReadCloser, error)
}

func NewExtractor(f *os.File) (Extractor, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	switch filepath.Ext(f.Name()) {
	case ".7z":
		zr, err := sevenzip.NewReader(f, fi.Size())
		if err != nil {
			return nil, fmt.Errorf("sevenzip: %w", err)
		}
		return &sevenZipExtractor{zr: zr}, nil
	case ".zip":
		zr, err := zip.NewReader(f, fi.Size())
		if err != nil {
			return nil, fmt.Errorf("zip: %w", err)
		}
		return &zipExtractor{zr: zr, dir: strings.TrimSuffix(filepath.Base(f.Name()), ".zip")}, nil
	default:
		panic("unreachable")
	}
}

type zipExtractor struct {
	zr  *zip.Reader
	dir string
}

func (e *zipExtractor) Files() int {
	return len(e.zr.File)
}

func (e *zipExtractor) FileName(i int) string {
	return filepath.Join(e.dir, filepath.FromSlash(fallbackShiftJIS(e.zr.File[i].Name)))
}

func (e *zipExtractor) FileSize(i int) uint64 {
	return e.zr.File[i].UncompressedSize64
}

func (e *zipExtractor) IsDir(i int) bool {
	return e.zr.File[i].Mode()&fs.ModeDir != 0
}

func (e *zipExtractor) Open(name string) (io.ReadCloser, error) {
	if name, found := strings.CutPrefix(name, e.dir+string(filepath.Separator)); found {
		return e.zr.Open(name)
	} else {
		return nil, fs.ErrNotExist
	}
}

type sevenZipExtractor struct {
	zr *sevenzip.Reader
}

func (e *sevenZipExtractor) Files() int {
	return len(e.zr.File)
}

func (e *sevenZipExtractor) FileName(i int) string {
	return filepath.FromSlash(fallbackShiftJIS(e.zr.File[i].Name))
}

func (e *sevenZipExtractor) FileSize(i int) uint64 {
	return e.zr.File[i].UncompressedSize
}

func (e *sevenZipExtractor) IsDir(i int) bool {
	return e.zr.File[i].Mode()&fs.ModeDir != 0
}

func (e *sevenZipExtractor) Open(name string) (io.ReadCloser, error) {
	return e.zr.Open(name)
}

func fallbackShiftJIS(s string) string {
	if !utf8.ValidString(s) {
		d, err := japanese.ShiftJIS.NewDecoder().String(s)
		if err == nil {
			return d
		}
	}
	return s
}
