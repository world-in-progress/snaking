package internal

import (
	"io"
	"log"
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
	if err := os.MkdirAll(filepath.Dir(shmFilePath), 0755); err != nil {
		return err
	}

	// Create and open the destination file in shared memory
	dst, err := os.OpenFile(shmFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
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
func ShareFile(sourcePath, shmName string) {
	// Open the source file and get file size
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		log.Fatalf("Failed to open source file: %v", err)
	}
	defer sourceFile.Close()

	fileStat, err := sourceFile.Stat()
	if err != nil {
		log.Fatalf("Failed to stat source file: %v", err)
	}
	fileSize := fileStat.Size()
	// Reject share file larger than 4GB (2^32 bytes)
	if fileSize > 1<<32 {
		log.Fatalf("File size exceeds 4GB limit: %d bytes", fileSize)
	}

	sourceData, err := syscall.Mmap(
		int(sourceFile.Fd()),
		0,
		int(fileSize),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		log.Fatalf("Failed to mmap source file: %v", err)
	}
	defer syscall.Munmap(sourceData)

	shmFilePath := filepath.Join(ShmPath, shmName)
	if err = os.MkdirAll(filepath.Dir(shmFilePath), 0755); err != nil {
		log.Fatalf("Failed to create shm directory: %v", err)
	}
	shmFd, err := syscall.Open(
		shmFilePath,
		syscall.O_CREAT|syscall.O_RDWR,
		0666,
	)
	if err != nil {
		log.Fatalf("ShmOpen failed: %v", err)
	}
	defer syscall.Close(shmFd)

	if err := syscall.Ftruncate(shmFd, fileSize); err != nil {
		log.Fatalf("Ftruncate failed: %v", err)
	}

	shmData, err := syscall.Mmap(
		shmFd,
		0,
		int(fileSize),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		log.Fatalf("Failed to mmap shm: %v", err)
	}
	defer syscall.Munmap(shmData)

	n := copy(shmData, sourceData)
	if n != int(fileSize) {
		log.Fatalf("Copied size mismatch: expected %d, got %d", fileSize, n)
	}
}
