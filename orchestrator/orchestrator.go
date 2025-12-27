package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	pb "snaking/internal/proto"
	w "snaking/orchestrator/worker"
	"sync"
	"syscall"

	"google.golang.org/grpc"
)

type MetaInfo struct {
	Workers []w.WorkerInfo `json:"workers"`
}

type WorkerStatus struct {
	Info   string
	Role   pb.WorkerRole
	Status pb.WorkerStatus
}

type Orchestrator struct {
	pb.UnimplementedControllerServer
	workerMu  sync.Mutex
	stopCh    chan struct{}
	readyCh   chan struct{}
	workerMap map[string]*w.Worker

	streamMu sync.Mutex
}

func New(metaJsonPath string) (*Orchestrator, error) {
	file, err := os.Open(metaJsonPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var metaInfo MetaInfo
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&metaInfo); err != nil {
		return nil, err
	}

	var workerMap = make(map[string]*w.Worker)
	for _, workerInfo := range metaInfo.Workers {
		workerMap[workerInfo.Id] = w.New(&workerInfo)
	}

	o := &Orchestrator{
		workerMap: workerMap,
		stopCh:    nil,
		readyCh:   make(chan struct{}),
	}

	return o, nil
}

// GRPC methods
func (o *Orchestrator) Register(ctx context.Context, in *pb.RegisterInfo) (*pb.RegisteredMessage, error) {
	o.workerMu.Lock()
	workerId := in.WorkerId
	workerRole := in.Role

	// Validate worker id
	if _, exists := o.workerMap[workerId]; !exists {
		o.workerMu.Unlock()
		log.Printf("Unknown worker %s tried to register", workerId)
		return &pb.RegisteredMessage{Success: false}, nil
	}

	// Validate worker connection status
	worker := o.workerMap[workerId]
	if worker.Connecting {
		o.workerMu.Unlock()
		log.Printf("Worker %s already registered", workerId)
		return &pb.RegisteredMessage{Success: false}, nil
	}

	// Validate worker role
	if worker.Role != workerRole {
		o.workerMu.Unlock()
		log.Printf("Worker %s role mismatch: expected %v, got %v", workerId, worker.Role, workerRole)
		return &pb.RegisteredMessage{Success: false}, nil
	}

	o.workerMu.Unlock()
	return &pb.RegisteredMessage{Success: true}, nil
}

// GRPC Methods
func (o *Orchestrator) ControlChannel(stream pb.Controller_ControlChannelServer) error {
	// Handshake to get worker ID
	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}
	workerId := firstMsg.WorkerId

	o.streamMu.Lock()

	// Initialize stop channel if not already
	if o.stopCh == nil {
		o.stopCh = make(chan struct{})
	}

	thisWorker := o.workerMap[workerId]
	thisWorker.Connect(stream)
	log.Printf("Worker %s connected to control channel", workerId)

	// Check ready if all workers are connected
	readyWorkerNum := 0
	for _, worker := range o.workerMap {
		if worker.Connecting {
			readyWorkerNum++
		}
	}
	if readyWorkerNum == len(o.workerMap) {
		close(o.readyCh)
	}
	o.streamMu.Unlock()

	// Cleanup when stream closes
	defer func() {
		o.streamMu.Lock()
		o.workerMap[workerId].Disconnect()
		log.Printf("Worker %s disconnected from control channel", workerId)

		stopWorkerNum := 0
		for _, worker := range o.workerMap {
			if worker.Connecting == false {
				stopWorkerNum++
			}
		}
		if stopWorkerNum == len(o.workerMap) {
			close(o.stopCh)
		}
		o.streamMu.Unlock()
	}()

	// Listen for incoming messages
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		o.streamMu.Lock()
		o.workerMap[msg.WorkerId].HandleStreamMessage(msg)
		o.streamMu.Unlock()
	}
}

func (o *Orchestrator) triggerPreprocessing() {
	// Wait for all workers to be ready
	<-o.readyCh
	log.Printf("All workers are ready. Well, let's go!")

	// Send running signal to all preprocessors
	o.streamMu.Lock()
	defer o.streamMu.Unlock()

	for _, worker := range o.workerMap {
		if worker.Role == pb.WorkerRole_WR_PREPROCESSOR {
			if err := worker.Run(); err != nil {
				log.Printf("Error sending preprocessing command to %s: %v", worker.Id, err)
			}
		}
	}
}

func (o *Orchestrator) BroadcastStop() {
	o.streamMu.Lock()
	for _, worker := range o.workerMap {
		if err := worker.Stop(); err != nil {
			log.Printf("Error sending stop command to %s: %v", worker.Id, err)
		}
	}
	o.streamMu.Unlock()

	// Wait for all workers to stop
	if o.stopCh != nil {
		<-o.stopCh
		log.Printf("All workers have stopped.")
	}
}

func (o *Orchestrator) Start(socketPath string) error {
	if _, err := os.Stat(socketPath); err == nil {
		os.Remove(socketPath)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("cannot listen UDS: %v", err)
	}
	os.Chmod(socketPath, 0777) // ensure permissions

	grpcServer := grpc.NewServer()
	pb.RegisterControllerServer(grpcServer, o)
	go o.triggerPreprocessing()

	errCh := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			errCh <- err
		}
	}()

	log.Printf("Orchestrator listening on %s", socketPath)

	// Wait for termination signal or server error
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigCh:
		log.Println("\nReceived shutdown signal...")
		o.BroadcastStop()
		grpcServer.GracefulStop()
		log.Println("Orchestrator shut down gracefully.")
		return nil
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	}
}
