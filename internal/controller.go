package internal

import (
	"context"
	"log"
	"net"
	"os"
	pb "snaking/internal/proto"
	"sync"

	"google.golang.org/grpc"
)

type StepBarrier struct {
	arrivedCount int
	releaseCh    chan struct{}
}

type SimulationServer struct {
	pb.UnimplementedControllerServer

	totalWorkers int
	dataReadyCh  chan struct{}

	mu       sync.Mutex
	barriers map[int32]*StepBarrier
}

func NewServer(worker int) *SimulationServer {
	return &SimulationServer{
		totalWorkers: worker,
		dataReadyCh:  make(chan struct{}),
		barriers:     make(map[int32]*StepBarrier),
	}
}

func (s *SimulationServer) WaitForStart(ctx context.Context, in *pb.Empty) (*pb.StartConfig, error) {
	select {
	case <-s.dataReadyCh:
		return &pb.StartConfig{
			Ready:          true,
			SharedDataPath: "mock",
			TotalWorkers:   int32(s.totalWorkers),
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *SimulationServer) SyncStep(ctx context.Context, in *pb.StepStatus) (*pb.StepResponse, error) {
	step := in.CurrentStep
	// workerID := in.WorkerId

	s.mu.Lock()
	barrier, exists := s.barriers[step]
	if !exists {
		barrier = &StepBarrier{
			arrivedCount: 0,
			releaseCh:    make(chan struct{}),
		}
		s.barriers[step] = barrier

		// Clean up previous step barrier
		delete(s.barriers, step-1)
	}

	barrier.arrivedCount++
	if barrier.arrivedCount == s.totalWorkers {
		close(barrier.releaseCh)
	}

	waitCh := barrier.releaseCh
	s.mu.Unlock()

	select {
	case <-waitCh:
		return &pb.StepResponse{
			ShouldContinue: true,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *SimulationServer) Run(socketPath string) {
	if _, err := os.Stat(socketPath); err == nil {
		os.Remove(socketPath)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("cannot listen UDS: %v", err)
	}
	os.Chmod(socketPath, 0777) // ensure permissions

	grpcServer := grpc.NewServer()
	pb.RegisterControllerServer(grpcServer, s)

	log.Printf("Simulation server listening on %s", socketPath)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (s *SimulationServer) Ready() {
	close(s.dataReadyCh)
}
