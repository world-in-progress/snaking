package dag

import (
	"context"
	"fmt"
	"log"
)

type EventType int

const (
	EventTaskCompleted EventType = iota
	EventTaskFailed
)

type TaskEvent struct {
	WorkerId string
	Type     EventType
}

type Scheduler struct {
	AdjacencyMap map[string][]string

	inDegrees  map[string]int
	readyQueue []string

	startCh chan string
	eventCh chan TaskEvent
}

func NewScheduler(deps []Dependency) *Scheduler {
	inDegrees := make(map[string]int)
	adjacencyMap := make(map[string][]string)

	// First pass: discover all executors
	readyQueue := make([]string, 0)
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
			readyQueue = append(readyQueue, d.Id)
		}
	}

	return &Scheduler{
		AdjacencyMap: adjacencyMap,

		inDegrees:  inDegrees,
		readyQueue: readyQueue,
		startCh:    make(chan string),
		eventCh:    make(chan TaskEvent),
	}
}

func (s *Scheduler) flushReadyQueue() {
	for _, id := range s.readyQueue {
		go func(workerId string) {
			s.startCh <- workerId
		}(id)
	}
	// Reset ready queue
	s.readyQueue = make([]string, 0)
}

func (s *Scheduler) allFinished() bool {
	// TODO (Dsssyc): optimize this check, especially for how to check completion efficiently
	for _, degree := range s.inDegrees {
		if degree > 0 {
			return false
		}
	}
	return true
}

func (s *Scheduler) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.flushReadyQueue()

	for {
		select {
		case event := <-s.eventCh:
			switch event.Type {
			case EventTaskCompleted:
				{
					// Kahn's algorithm step: reduce in-degrees of next nodes
					nexts := s.AdjacencyMap[event.WorkerId]
					for _, nextId := range nexts {
						s.inDegrees[nextId]--
						if s.inDegrees[nextId] == 0 {
							s.readyQueue = append(s.readyQueue, nextId)
						}
					}

					if s.allFinished() {
						return nil
					}

					s.flushReadyQueue()
				}
			case EventTaskFailed:
				{
					// TODO (Dsssyc): handle task failure
					cancel()
					log.Printf("Task failed, cancelling DAG")
					return fmt.Errorf("task %s failed", event.WorkerId)
				}
			}
		case <-ctx.Done():
			log.Printf("Dag stopped: %v", ctx.Err())
			return nil
		}
	}
}
