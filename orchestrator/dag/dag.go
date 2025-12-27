package dag

import (
	w "snaking/orchestrator/worker"
)

type Dependency struct {
	Id        string   `json:"id"`
	DependsOn []string `json:"depends-on"`
}

type Dag struct {
	Dependencies []Dependency
	nodes        map[string]Node
}

func New(dep []Dependency, workerMap map[string]*w.Worker) *Dag {
	triggers := make(map[string]Node)
	for _, d := range dep {
		worker, _ := workerMap[d.Id]
		var currentDeps []*w.Worker
		for _, depId := range d.DependsOn {
			depWorker, _ := workerMap[depId]
			currentDeps = append(currentDeps, depWorker)
		}
		triggers[d.Id] = Node{
			Worker:            worker,
			CurrentDependency: currentDeps,
		}
	}
	return &Dag{
		Dependencies: dep,
		nodes:        triggers,
	}
}

func (wt *Dag) Tick() {

}
