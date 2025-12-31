package shm

import (
	"fmt"
	"os"
	"syscall"
)

type SharedMemory struct {
	Name string
	Size int
	Data []byte
	file *os.File
}

func Attach(name string) (*SharedMemory, error) {
	path := "/dev/shm/" + name

	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open shared memory file %s: %w", path, err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat shared memory file %s: %w", path, err)
	}
	size := int(fi.Size())

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to mmap shared memory file %s: %w", path, err)
	}

	return &SharedMemory{
		Name: name,
		Size: size,
		Data: data,
		file: f,
	}, nil
}

func (sm *SharedMemory) Close() error {
	if sm.Data != nil {
		if err := syscall.Munmap(sm.Data); err != nil {
			return err
		}
		sm.Data = nil
	}
	if sm.file != nil {
		if err := sm.file.Close(); err != nil {
			return err
		}
		sm.file = nil
	}
	return nil
}

func (sm *SharedMemory) Unlink() error {
	path := "/dev/shm/" + sm.Name
	return os.Remove(path)
}
