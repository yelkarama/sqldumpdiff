//go:build windows
// +build windows

package comparer

import (
	"fmt"
	"os"
)

func mmapFile(_ *os.File, _ int) ([]byte, error) {
	return nil, fmt.Errorf("mmap not supported on windows")
}

func munmapFile(_ []byte) error {
	return nil
}
