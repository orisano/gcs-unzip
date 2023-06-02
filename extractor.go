package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/bodgit/sevenzip"
	"github.com/klauspost/compress/zip"
)

type Extractor interface {
	Files() int
	FileName(int) string
	FileSize(int) uint64
	Open(int) (io.ReadCloser, error)
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
		return &zipExtractor{zr: zr}, nil
	default:
		panic("unreachable")
	}
}

type zipExtractor struct {
	zr *zip.Reader
}

func (e *zipExtractor) Files() int {
	return len(e.zr.File)
}

func (e *zipExtractor) FileName(i int) string {
	return filepath.FromSlash(e.zr.File[i].Name)
}

func (e *zipExtractor) FileSize(i int) uint64 {
	return e.zr.File[i].UncompressedSize64
}

func (e *zipExtractor) Open(i int) (io.ReadCloser, error) {
	return e.zr.Open(e.zr.File[i].Name)
}

type sevenZipExtractor struct {
	zr *sevenzip.Reader
}

func (e *sevenZipExtractor) Files() int {
	return len(e.zr.File)
}

func (e *sevenZipExtractor) FileName(i int) string {
	return filepath.FromSlash(e.zr.File[i].Name)
}

func (e *sevenZipExtractor) FileSize(i int) uint64 {
	return e.zr.File[i].UncompressedSize
}

func (e *sevenZipExtractor) Open(i int) (io.ReadCloser, error) {
	return e.zr.Open(e.zr.File[i].Name)
}
