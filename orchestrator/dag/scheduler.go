package dag

import (
	"context"
	"fmt"
	"log"
	pb "snaking/internal/proto"
)

type EventType int

type Dependency struct {
	Id        string   `json:"id"`
	DependsOn []string `json:"depends-on"`
}

const (
	EventTaskCompleted EventType = iota
	EventTaskFailed
)

type RunningEvent struct {
	WorkerId string
	LunchStr string
}

type StartEvent struct {
	WorkerId string
	Payloads map[string]string
}
type Scheduler struct {
	adjacencyMap  map[string][]string
	dependencyMap map[string][]string

	inDegrees       map[string]int
	readyQueue      []StartEvent
	CompletionInfos map[string]string // workerId -> completion payload

	StartCh chan StartEvent
	DoneCh  chan struct{}
	EventCh chan *pb.WorkerStreamMessage
}

func NewScheduler(deps []Dependency) *Scheduler {
	inDegrees := make(map[string]int)
	adjacencyMap := make(map[string][]string)
	dependencyMap := make(map[string][]string)

	// Make dependency map from deps
	for _, d := range deps {
		dependencyMap[d.Id] = d.DependsOn
	}

	// Build in-degrees and adjacency map
	readyQueue := make([]StartEvent, 0)
	for _, d := range deps {
		// Initialize in-degrees
		inDegrees[d.Id] = len(d.DependsOn)

		// Build adjacency map
		adjacencyMap[d.Id] = make([]string, 0)
		for _, depId := range d.DependsOn {
			adjacencyMap[depId] = append(adjacencyMap[depId], d.Id)
		}

		// If in-degree is zero, add to ready queue
		if inDegrees[d.Id] == 0 {
			readyQueue = append(readyQueue, StartEvent{WorkerId: d.Id, Payloads: make(map[string]string)})
		}
	}

	return &Scheduler{
		adjacencyMap:  adjacencyMap,
		dependencyMap: dependencyMap,

		inDegrees:       inDegrees,
		readyQueue:      readyQueue,
		CompletionInfos: make(map[string]string),
		StartCh:         make(chan StartEvent),
		EventCh:         make(chan *pb.WorkerStreamMessage),
	}
}

func (s *Scheduler) flushReadyQueue() {
	for _, event := range s.readyQueue {
		go func(event StartEvent) {
			s.StartCh <- event
		}(event)
	}
	// Reset ready queue
	s.readyQueue = make([]StartEvent, 0)
}

func (s *Scheduler) allFinished() bool {
	return len(s.CompletionInfos) == len(s.inDegrees)
}

func (s *Scheduler) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.flushReadyQueue()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Dag stopped: %v", ctx.Err())
			return nil
		case msg := <-s.EventCh:
			workerId := msg.WorkerId
			switch msg.Type {
			case pb.WorkerStreamMessageType_WSM_REPORTSTATUS:
				workerMsg, _ := msg.Content.(*pb.WorkerStreamMessage_Status)
				switch workerMsg.Status {
				case pb.WorkerStatus_WS_COMPLETED:
					{
						// Mark worker as finished
						s.CompletionInfos[workerId] = msg.Payload

						// Kahn's algorithm step: reduce in-degrees of next nodes
						nexts := s.adjacencyMap[workerId]
						for _, nextId := range nexts {
							s.inDegrees[nextId]--
							if s.inDegrees[nextId] == 0 {
								// Build start event with payloads from dependencies
								startEvent := StartEvent{
									WorkerId: nextId,
									Payloads: make(map[string]string),
								}
								for _, depId := range s.dependencyMap[nextId] {
									startEvent.Payloads[depId] = s.CompletionInfos[depId]
								}

								// Add to ready queue
								s.readyQueue = append(s.readyQueue, startEvent)
							}
						}

						if s.allFinished() {
							log.Printf("All tasks completed successfully")
							return nil
						}

						s.flushReadyQueue()
					}
				case pb.WorkerStatus_WS_FAILED:
					{
						// TODO (Dsssyc): handle task failure
						cancel()
						log.Printf("Task failed, cancelling DAG")
						return fmt.Errorf("task %s failed with message: %s", workerId, msg.Payload)
					}
				}
			case pb.WorkerStreamMessageType_WSM_REPORTSTEP:
				{
					// TODO (Dsssyc): handle step report
				}
			}
		}
	}
}
