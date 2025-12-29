package dag

import (
	pb "snaking/internal/proto"
	w "snaking/orchestrator/worker"
)

type Dependency struct {
	Id        string   `json:"id"`
	DependsOn []string `json:"depends-on"`
}

type Dag struct {
	Dependencies []Dependency
	nodes        []*Node
	idleNodeNum  int
	completed    bool
}

func New(dep []Dependency, workerMap map[string]*w.Worker) *Dag {
	nodes := make([]*Node, len(dep))
	for i, d := range dep {
		worker, _ := workerMap[d.Id]
		var currentDeps []*w.Worker
		for _, depId := range d.DependsOn {
			depWorker, _ := workerMap[depId]
			currentDeps = append(currentDeps, depWorker)
		}
		nodes[i] = NewNode(worker, currentDeps)
	}
	d := &Dag{
		Dependencies: dep,
		nodes:        nodes,
		idleNodeNum:  len(nodes),
		completed:    false,
	}

	// Set Dag as trigger for each worker
	for _, node := range nodes {
		node.Worker.SetTrigger(d)
	}

	return d
}

func (d *Dag) notifyTaskCompletion(completedWorkerId string) {
	// Find and remove the completed node from idle nodes
	miss := true
	for i := 0; i < d.idleNodeNum; i++ {
		node := d.nodes[i]
		if node.Worker.Id == completedWorkerId {
			// Swap the completed node with the last idle node
			d.nodes[i], d.nodes[d.idleNodeNum-1] = d.nodes[d.idleNodeNum-1], d.nodes[i]
			d.idleNodeNum--
			miss = false
			break
		}
	}
	if miss {
		return
	}

	// Mark DAG as completed if no idle nodes remain
	if d.idleNodeNum == 0 {
		d.completed = true
		return
	}

	// Update dependencies for remaining idle nodes
	for i := 0; i < d.idleNodeNum; i++ {
		node := d.nodes[i]
		node.Update(completedWorkerId)
	}
}

func (d *Dag) NotifyTaskStatus(id string, status pb.WorkerStatus) {
	if status != pb.WorkerStatus_WS_COMPLETED {
		return
	}
	d.notifyTaskCompletion(id)
}

func (d *Dag) Run() error {
	// Trigger all ready nodes (in first round, ready nodes are those without dependencies)
	for i := 0; i < d.idleNodeNum; i++ {
		node := d.nodes[i]
		if node.Ready() {
			if err := node.Worker.Run(); err != nil {
				return err
			}
		}
	}
	return nil
}
