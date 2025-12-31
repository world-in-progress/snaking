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
	"snaking/orchestrator/dag"
	w "snaking/orchestrator/worker"
	"sync"
	"syscall"

	"google.golang.org/grpc"
)

const (
	RoleSolver        string = "solver"
	RolePreprocessor  string = "preprocessor"
	RolePostprocessor string = "postprocessor"
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
	workerMap map[string]*w.Worker

	preDag *dag.Scheduler

	activeEventCh chan<- *pb.WorkerStreamMessage

	workerMu sync.Mutex
	stopCh   chan struct{}
	readyCh  chan struct{}
	stopSig  chan os.Signal

	streamMu sync.Mutex
}

type ArgFromWorkerOutput struct {
	Name        string `json:"name"`
	WorkerId    string `json:"worker-id"`
	OutputField string `json:"output-field"`
}

func getDependentWorkerIds(args []json.RawMessage) ([]string, error) {
	var deps []string
	for _, arg := range args {
		var argInfo map[string]any
		if err := json.Unmarshal(arg, &argInfo); err != nil {
			return nil, fmt.Errorf("failed to unmarshal worker arg: %w", err)
		}

		if argInfo["type"] == "from-worker-output" {
			var outputArg ArgFromWorkerOutput
			if err := json.Unmarshal(arg, &outputArg); err != nil {
				return nil, fmt.Errorf("failed to unmarshal ArgFromWorkerOutput: %w", err)
			}
			deps = append(deps, outputArg.WorkerId)
		}
	}
	return deps, nil
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

	var preDagDeps []dag.Dependency
	var workerMap = make(map[string]*w.Worker)
	for _, workerInfo := range metaInfo.Workers {
		workerMap[workerInfo.Id] = w.New(&workerInfo)

		// Build dependencies for preprocessors
		if workerInfo.Role == RolePreprocessor {
			deps, err := getDependentWorkerIds(workerInfo.Args)
			if err != nil {
				return nil, fmt.Errorf("failed to get dependencies for worker %s: %w", workerInfo.Id, err)
			}
			preDagDeps = append(preDagDeps, dag.Dependency{
				Id:        workerInfo.Id,
				DependsOn: deps,
			})
		}
	}

	o := &Orchestrator{
		workerMap: workerMap,
		preDag:    dag.NewScheduler(preDagDeps),
		stopCh:    nil,
		stopSig:   make(chan os.Signal, 1),
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
		return &pb.RegisteredMessage{ArgDescriptions: ""}, nil
	}

	// Validate worker connection status
	worker := o.workerMap[workerId]
	if worker.Connecting {
		o.workerMu.Unlock()
		log.Printf("Worker %s already registered", workerId)
		return &pb.RegisteredMessage{ArgDescriptions: ""}, nil
	}

	// Validate worker role
	if worker.Role != workerRole {
		o.workerMu.Unlock()
		log.Printf("Worker %s role mismatch: expected %v, got %v", workerId, worker.Role, workerRole)
		return &pb.RegisteredMessage{ArgDescriptions: ""}, nil
	}

	o.workerMu.Unlock()
	return &pb.RegisteredMessage{ArgDescriptions: worker.GetArgDescriptions()}, nil
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
		if o.activeEventCh != nil {
			select {
			case o.activeEventCh <- msg:
			default:
				log.Printf("Dropping event from worker %s due to full channel", msg.WorkerId)
			}
		}
		o.streamMu.Unlock()
	}
}

func (o *Orchestrator) runDag(ctx context.Context, dag *dag.Scheduler) {
	for {
		select {
		case startEvent := <-dag.StartCh:
			worker, exists := o.workerMap[startEvent.WorkerId]
			if !exists {
				log.Printf("Worker %s not found for starting task", startEvent.WorkerId)
				continue
			}

			if err := worker.Run(startEvent.Payloads); err != nil {
				log.Printf("Error starting task on worker %s: %v", startEvent.WorkerId, err)
			}
		case <-dag.DoneCh:
			log.Printf("DAG execution completed.")
			return
		case <-ctx.Done():
			log.Printf("Dag stopped: %v", ctx.Err())
			return
		}
	}
}

func (o *Orchestrator) Run(ctx context.Context) error {
	o.streamMu.Lock()
	o.activeEventCh = o.preDag.EventCh
	o.streamMu.Unlock()

	go o.runDag(ctx, o.preDag)

	log.Printf("Starting preprocessing DAG...")
	if err := o.preDag.Run(ctx); err != nil {
		return fmt.Errorf("preprocessing failed: %w", err)
	}
	return nil
}

func (o *Orchestrator) triggerPreprocessing() {
	// Wait for all workers to be ready
	<-o.readyCh
	log.Printf("All workers are ready. Well, let's go!")

	ctx := context.Background()
	if err := o.Run(ctx); err != nil {
		log.Printf("Error running preprocessing DAG: %v", err)
		o.stopSig <- os.Interrupt
		return
	}

	// Send running signal to all preprocessors
	// o.streamMu.Lock()
	// defer o.streamMu.Unlock()

	// if err := o.PreDag.Run(); err != nil {
	// 	log.Fatalf("Error running preprocessing DAG: %v", err)
	// }
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
	case <-o.stopSig:
		log.Println("Received stop signal from orchestrator...")
		o.BroadcastStop()
		grpcServer.GracefulStop()
		log.Println("Orchestrator shut down gracefully.")
		return nil
	case err := <-errCh:
		log.Println("Received error from GRPC server...")
		return fmt.Errorf("server error: %w", err)
	}
}
