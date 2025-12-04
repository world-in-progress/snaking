package semaphorepipe

import (
	"fmt"
	"os"
)

type SemaphorePipe struct {
	Sender   *os.File
	Receiver *os.File
}

func New() (*SemaphorePipe, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create semaphore pipe: %w", err)
	}

	pipe := &SemaphorePipe{
		Receiver: r,
		Sender:   w,
	}
	return pipe, nil
}

func (sp *SemaphorePipe) Post() error {
	_, err := sp.Sender.Write([]byte{0x01})
	if err != nil {
		return fmt.Errorf("failed to write to semaphore pipe: %w", err)
	}
	return nil
}

func (sp *SemaphorePipe) Wait() error {
	buf := make([]byte, 1)
	_, err := sp.Receiver.Read(buf)
	if err != nil {
		return fmt.Errorf("failed to read from semaphore pipe: %w", err)
	}
	return nil
}

func (sp *SemaphorePipe) Close() error {
	if err := sp.Receiver.Close(); err != nil {
		return fmt.Errorf("failed to close read end of semaphore pipe: %w", err)
	}
	if err := sp.Sender.Close(); err != nil {
		return fmt.Errorf("failed to close write end of semaphore pipe: %w", err)
	}
	return nil
}
