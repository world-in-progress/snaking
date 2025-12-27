package orchestrator

// import (
// 	"context"
// 	"fmt"
// 	"log"
// 	"net"
// 	"os"
// 	pb "snaking/internal/proto"
// 	"strings"
// 	"sync"
// 	"time"

// 	"google.golang.org/grpc"
// )

// const (
// 	solverPrefix            = "solver"
// 	preprocessorPrefix      = "preprocessor"
// 	monitorHeartbeatTimeout = 10 * time.Second
// )

// type WorkerStatusEnum int

// const (
// 	STOPPED WorkerStatusEnum = iota
// 	PENDING
// 	READY
// 	RUNNING
// )

// type StepBarrier struct {
// 	arrivedCount int
// 	releaseCh    chan struct{}
// }

// type MetaInfo struct {
// 	AssetPath  string
// 	WorkerList []string
// }

// type WorkerStatus struct {
// 	lastUpdate int64
// 	errorMsg   string
// 	status     WorkerStatusEnum
// }

// type Orchestrator struct {
// 	pb.UnimplementedControllerServer

// 	mu           sync.Mutex
// 	readyCh      chan struct{}
// 	preprocessCh chan struct{}

// 	allHealthy       bool
// 	solverList       map[string]*WorkerStatus
// 	preprocessorList map[string]*WorkerStatus

// 	metaInfo *MetaInfo

// 	barriers map[int32]*StepBarrier

// 	cmdStreams map[string]pb.Controller_ControlChannelServer
// 	streamMu   sync.Mutex
// }

// func New(metaInfo *MetaInfo) (*Orchestrator, error) {

// 	o := &Orchestrator{
// 		allHealthy:       true,
// 		metaInfo:         metaInfo,
// 		readyCh:          make(chan struct{}),
// 		preprocessCh:     make(chan struct{}),
// 		barriers:         make(map[int32]*StepBarrier),
// 		solverList:       make(map[string]*WorkerStatus),
// 		preprocessorList: make(map[string]*WorkerStatus),
// 		workerStreams:    make(map[string]pb.Controller_ControlChannelServer),
// 	}

// 	return o, nil
// }

// func (o *Orchestrator) getWorkerStatusNoLock(workerId string) *WorkerStatus {
// 	if status, exists := o.solverList[workerId]; exists {
// 		return status
// 	} else if status, exists := o.preprocessorList[workerId]; exists {
// 		return status
// 	}
// 	return nil
// }

// // GRPC Methods
// func (o *Orchestrator) Register(ctx context.Context, in *pb.RegisterInfo) (*pb.RegisteredMessage, error) {
// 	// o.mu.Lock()

// 	// workerId := in.WorkerId
// 	// newStatus := &WorkerStatus{
// 	// 	lastUpdate: time.Now().Unix(),
// 	// 	status:     READY,
// 	// 	errorMsg:   "",
// 	// }

// 	// // Register worker
// 	// if strings.HasPrefix(workerId, solverPrefix) {
// 	// 	o.solverList[workerId] = newStatus
// 	// } else if strings.HasPrefix(workerId, preprocessorPrefix) {
// 	// 	o.preprocessorList[workerId] = newStatus
// 	// } else {
// 	// 	log.Printf("Unknown worker prefix for worker ID: %s", workerId)
// 	// }

// 	// // If all workers are registered, signal readiness
// 	// allReadyCh := o.readyCh
// 	// if len(o.metaInfo.WorkerList) == len(o.solverList)+len(o.preprocessorList) {
// 	// 	close(allReadyCh)
// 	// }

// 	// o.mu.Unlock()
// 	return &pb.RegisteredMessage{Success: true, SharedDataPath: o.metaInfo.AssetPath}, nil
// }

// // GRPC Methods
// func (o *Orchestrator) FinishPreprocess(ctx context.Context, in *pb.PreprocessFinished) (*pb.Empty, error) {
// 	workerId := in.WorkerId

// 	o.mu.Lock()
// 	defer o.mu.Unlock()

// 	// Mark preprocessor as finished
// 	if status, exists := o.preprocessorList[workerId]; exists {
// 		status.status = STOPPED
// 	} else {
// 		log.Printf("Received preprocess finish from unknown worker ID: %s", workerId)
// 	}

// 	// Check if all preprocessors are done
// 	allDone := true
// 	for _, status := range o.preprocessorList {
// 		if status.status != STOPPED {
// 			allDone = false
// 			break
// 		}
// 	}

// 	if allDone {
// 		close(o.preprocessCh)
// 	}

// 	return &pb.Empty{}, nil
// }

// // GRPC Methods
// func (o *Orchestrator) PostError(ctx context.Context, in *pb.ErrorMessage) (*pb.Empty, error) {
// 	workerId := in.WorkerId
// 	errorMsg := in.Message

// 	o.mu.Lock()
// 	defer o.mu.Unlock()
// 	log.Printf("Received error from worker (%s): %s\n", workerId, errorMsg)

// 	// Update worker status to STOPPED and record the error message
// 	status := o.getWorkerStatusNoLock(workerId)
// 	if status == nil {
// 		log.Printf("Received error from unknown worker ID: %s", workerId)
// 		return &pb.Empty{}, nil
// 	}

// 	status.status = STOPPED
// 	status.errorMsg = errorMsg
// 	return &pb.Empty{}, nil
// }

// // GRPC Methods
// func (o *Orchestrator) HeartBeat(ctx context.Context, in *pb.HeartbeatInfo) (*pb.HeartbeatResponse, error) {
// 	workerId := in.WorkerId
// 	workerStatus := in.Status
// 	fmt.Printf("Get heartbeat from worker (%s) with status %d\n", workerId, workerStatus)

// 	o.mu.Lock()
// 	defer o.mu.Unlock()

// 	// Update worker status based on the heartbeat
// 	status := o.getWorkerStatusNoLock(workerId)
// 	if status == nil {
// 		log.Printf("Received heartbeat from unknown worker ID: %s", workerId)
// 		return &pb.HeartbeatResponse{Status: int32(STOPPED)}, nil
// 	}

// 	status.lastUpdate = time.Now().Unix()
// 	status.status = WorkerStatusEnum(workerStatus)

// 	if !o.allHealthy {
// 		return &pb.HeartbeatResponse{Status: int32(STOPPED)}, nil
// 	}
// 	return &pb.HeartbeatResponse{Status: int32(status.status)}, nil
// }

// // GRPC Methods
// func (o *Orchestrator) WaitForStart(ctx context.Context, in *pb.Empty) (*pb.StartConfig, error) {
// 	// Wait for readiness signal
// 	select {
// 	case <-o.readyCh:
// 		// Once ready, wait for all preprocessors to finish
// 		<-o.preprocessCh
// 		if o.metaInfo == nil {
// 			return &pb.StartConfig{
// 				Ready:          false,
// 				SharedDataPath: "",
// 			}, nil
// 		}
// 		return &pb.StartConfig{
// 			Ready:          true,
// 			SharedDataPath: o.metaInfo.AssetPath,
// 		}, nil
// 	case <-ctx.Done():
// 		return nil, ctx.Err()
// 	}
// }

// // GRPC Methods
// func (o *Orchestrator) SyncStep(ctx context.Context, in *pb.StepStatus) (*pb.StepResponse, error) {
// 	step := in.CurrentStep
// 	// workerID := in.WorkerId

// 	o.mu.Lock()
// 	barrier, exists := o.barriers[step]
// 	if !exists {
// 		barrier = &StepBarrier{
// 			arrivedCount: 0,
// 			releaseCh:    make(chan struct{}),
// 		}
// 		o.barriers[step] = barrier

// 		// Clean up previous step barrier
// 		delete(o.barriers, step-1)
// 	}

// 	barrier.arrivedCount++
// 	if barrier.arrivedCount == len(o.solverList) {
// 		close(barrier.releaseCh)
// 	}

// 	waitCh := barrier.releaseCh
// 	o.mu.Unlock()

// 	select {
// 	case <-waitCh:
// 		return &pb.StepResponse{
// 			ShouldContinue: true,
// 		}, nil
// 	case <-ctx.Done():
// 		return nil, ctx.Err()
// 	}
// }

// // GRPC Methods
// func (o *Orchestrator) ControlChannel(stream pb.Controller_ControlChannelServer) error {
// 	// 1. Handshake: Read the first message to get Worker ID
// 	firstMsg, err := stream.Recv()
// 	if err != nil {
// 		return err
// 	}
// 	workerId := firstMsg.WorkerId

// 	o.streamMu.Lock()
// 	// Register the worker
// 	o.workerStreams[workerId] = stream
// 	log.Printf("Worker %s connected to control channel", workerId)

// 	// Initialize worker status
// 	newStatus := &WorkerStatus{
// 		lastUpdate: time.Now().Unix(),
// 		status:     READY,
// 		errorMsg:   "",
// 	}
// 	if strings.HasPrefix(workerId, solverPrefix) {
// 		o.solverList[workerId] = newStatus
// 	} else if strings.HasPrefix(workerId, preprocessorPrefix) {
// 		o.preprocessorList[workerId] = newStatus
// 	} else {
// 		log.Printf("Unknown worker prefix for worker ID: %s", workerId)
// 	}

// 	// If all workers are registered, signal readiness
// 	if len(o.metaInfo.WorkerList) == len(o.solverList)+len(o.preprocessorList) {
// 		close(o.readyCh)
// 	}
// 	o.streamMu.Unlock()

// 	// Cleanup when stream closes
// 	defer func() {
// 		o.streamMu.Lock()
// 		delete(o.workerStreams, workerId)
// 		o.streamMu.Unlock()
// 		log.Printf("Worker %s disconnected from control channel", workerId)
// 	}()

// 	// 3. Keep reading loop
// 	for {
// 		status, err := stream.Recv()
// 		if err != nil {
// 			return err
// 		}
// 		// Handle worker status updates if needed
// 		log.Printf("Received status from %s: %v", workerId, status)
// 	}
// }

// func (o *Orchestrator) BroadcastCommand(cmdType pb.OrchestratorCommand_CommandType, payload string) {
// 	o.streamMu.Lock()
// 	defer o.streamMu.Unlock()

// 	cmd := &pb.OrchestratorCommand{
// 		Command: cmdType,
// 		Payload: payload,
// 	}

// 	for id, stream := range o.workerStreams {
// 		if err := stream.Send(cmd); err != nil {
// 			log.Printf("Failed to send command to %s: %v", id, err)
// 			// Optionally remove the broken stream
// 		}
// 	}
// }

// func (o *Orchestrator) monitorHeartbeats() {
// 	ticker := time.NewTicker(5 * time.Second)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		o.mu.Lock()
// 		now := time.Now().Unix()

// 		for id, worker := range o.solverList {
// 			if now-worker.lastUpdate > int64(monitorHeartbeatTimeout.Seconds()) {

// 				o.allHealthy = false
// 				worker.errorMsg = "Heartbeat timeout"
// 				log.Printf("Worker %s marked as dead due to heartbeat timeout", id)
// 			}
// 		}

// 		o.mu.Unlock()
// 	}
// }

// func (o *Orchestrator) Run(socketPath string) error {
// 	if _, err := os.Stat(socketPath); err == nil {
// 		os.Remove(socketPath)
// 	}

// 	listener, err := net.Listen("unix", socketPath)
// 	if err != nil {
// 		log.Fatalf("cannot listen UDS: %v", err)
// 	}
// 	os.Chmod(socketPath, 0777) // ensure permissions

// 	grpcServer := grpc.NewServer()
// 	pb.RegisterControllerServer(grpcServer, o)

// 	// Start a goroutine to monitor worker heartbeats after several seconds to allow workers to start
// 	go o.monitorHeartbeats()

// 	// Start serving
// 	log.Printf("Simulation orchestrator listening on %s", socketPath)
// 	if err := grpcServer.Serve(listener); err != nil {
// 		log.Fatalf("failed to serve: %v", err)
// 	}

// 	return nil
// }
