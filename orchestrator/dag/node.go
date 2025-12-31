package dag

// import (
// 	pb "snaking/internal/proto"
// 	w "snaking/orchestrator/worker"
// )

// type Node struct {
// 	Worker                *w.Worker
// 	CurrentDependency     []*w.Worker
// 	ExistingDependencyNum int
// }

// func NewNode(worker *w.Worker, deps []*w.Worker) *Node {
// 	return &Node{
// 		Worker:                worker,
// 		CurrentDependency:     deps,
// 		ExistingDependencyNum: len(deps),
// 	}
// }

// func (n *Node) Finished() bool {
// 	return n.Worker.Status == pb.WorkerStatus_WS_COMPLETED
// }

// func (n *Node) Ready() bool {
// 	return n.ExistingDependencyNum == 0
// }

// func (n *Node) Update(completedWorkerId string) {
// 	if n.ExistingDependencyNum == -1 {
// 		// already triggered
// 		return
// 	}

// 	// Check if the completedWorkerId is in CurrentDependency
// 	// If found, move it to the last position and reduce ExistingDependencyNum
// 	for i := 0; i < n.ExistingDependencyNum; i++ {
// 		if n.CurrentDependency[i].Id == completedWorkerId {
// 			// Swap the found dependency with the one at ExistingDependencyNum - 1
// 			n.CurrentDependency[i], n.CurrentDependency[n.ExistingDependencyNum-1] = n.CurrentDependency[n.ExistingDependencyNum-1], n.CurrentDependency[i]
// 			n.ExistingDependencyNum--
// 			break
// 		}
// 	}

// 	// If all dependencies are resolved, trigger the worker
// 	if n.ExistingDependencyNum == 0 {
// 		n.Worker.Run()
// 		n.ExistingDependencyNum = -1 // mark as triggered
// 	}
// }
