package dag

import (
	w "snaking/orchestrator/worker"
)

type Node struct {
	Worker                *w.Worker
	CurrentDependency     []*w.Worker
	ExistingDependencyNum int
}

func NewWorkerTrigger(worker *w.Worker, deps []*w.Worker) *Node {
	return &Node{
		Worker:                worker,
		CurrentDependency:     deps,
		ExistingDependencyNum: len(deps),
	}
}

func (wt *Node) Update(currentRunningWorkerIds []string) {
	if wt.ExistingDependencyNum == 0 {
		return
	}

	for _, workerId := range currentRunningWorkerIds {
		// Check if the workerId is in CurrentDependency
		// If found, move it to the last position and reduce ExistingDependencyNum
		for i := 0; i < wt.ExistingDependencyNum; i++ {
			if wt.CurrentDependency[i].Id == workerId {
				// Swap the found dependency with the one at ExistingDependencyNum - 1
				wt.CurrentDependency[i], wt.CurrentDependency[wt.ExistingDependencyNum-1] = wt.CurrentDependency[wt.ExistingDependencyNum-1], wt.CurrentDependency[i]
				wt.ExistingDependencyNum--
				break
			}
		}
	}
}

func (wt *Node) Tick() {

}
