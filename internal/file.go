package internal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	ShmPath = "/dev/shm"
)

// CopyFileToShm copy the file at sourcePath to a shared memory location with the given shmName.
func CopyFileToShm(sourcePath, shmName string) error {
	src, err := os.Open(sourcePath)
	if err != nil {
		return err
	}

	// Ensure the shared memory directory exists
	shmFilePath := filepath.Join(ShmPath, shmName)
	if err := os.MkdirAll(filepath.Dir(shmFilePath), 0666); err != nil {
		return err
	}

	// Create and open the destination file in shared memory
	dst, err := os.OpenFile(shmFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer dst.Close()

	// Copy the contents from source to destination
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}

	return nil
}

// ShareFile maps the source file to shared memory with the given shmName
// Note: The maximum file size supported is 4GB
func ShareFile(sourcePath, shmName string) error {
	// Open the source file and get file size
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	fileStat, err := sourceFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}
	fileSize := fileStat.Size()
	// Reject share file larger than 4GB (2^32 bytes)
	if fileSize > 1<<32 {
		return fmt.Errorf("file size exceeds 4GB limit: %d bytes", fileSize)
	}

	sourceData, err := syscall.Mmap(
		int(sourceFile.Fd()),
		0,
		int(fileSize),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("failed to mmap source file: %w", err)
	}
	defer syscall.Munmap(sourceData)

	shmFilePath := filepath.Join(ShmPath, shmName)
	if err = os.MkdirAll(filepath.Dir(shmFilePath), 0666); err != nil {
		return fmt.Errorf("failed to create shm directory: %w", err)
	}
	shmFd, err := syscall.Open(
		shmFilePath,
		syscall.O_CREAT|syscall.O_RDWR,
		0666,
	)
	if err != nil {
		return fmt.Errorf("shm file open failed: %w", err)
	}
	defer syscall.Close(shmFd)

	if err := syscall.Ftruncate(shmFd, fileSize); err != nil {
		return fmt.Errorf("syscall.Ftruncate failed: %w", err)
	}

	shmData, err := syscall.Mmap(
		shmFd,
		0,
		int(fileSize),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("failed to mmap shm: %w", err)
	}
	defer syscall.Munmap(shmData)

	n := copy(shmData, sourceData)
	if n != int(fileSize) {
		return fmt.Errorf("copied size mismatch: expected %d, got %d", fileSize, n)
	}

	return nil
}
