//go:build !windows
// +build !windows

package comparer

import (
	"os"
	"syscall"
)

func mmapFile(f *os.File, size int) ([]byte, error) {
	return syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmapFile(b []byte) error {
	return syscall.Munmap(b)
}
