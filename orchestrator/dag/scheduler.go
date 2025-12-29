package dag

type Executor interface {
	GetID() string
	Execute()
}

type BackwardRelation struct {
	ExecutorId string
	Dependents []string
}

type Scheduler struct {
	// TODO(Dsssyc): A bug over here
	Relations map[string]BackwardRelation

	inDegrees    map[string]int32
	executors    map[string]Executor
	standbyQueue []string

	eventCh chan struct{}
	stopCh  chan struct{}
}

// func NewScheduler(deps []Dependency, executors map[string]Executor) *Scheduler {
func NewScheduler(deps []Dependency) *Scheduler {
	inDegrees := make(map[string]int32)
	brs := make(map[string]BackwardRelation)

	// First pass: discover all executors
	for _, d := range deps {
		brs[d.Id] = BackwardRelation{
			ExecutorId: d.Id,
			Dependents: make([]string, 0),
		}
		inDegrees[d.Id] = 0
	}

	// Second pass: build backward relations
	for _, d := range deps {
		for _, depId := range d.DependsOn {
			dps := brs[depId].Dependents
			dps = append(dps, d.Id)
		}
	}

	// Third pass: calculate in-degrees
	for _, d := range deps {
		br := brs[d.Id]
		inDegrees[d.Id] = int32(len(br.Dependents))
	}

	return &Scheduler{
		// executors:    executors,
		inDegrees:    inDegrees,
		Relations:    brs,
		standbyQueue: []string{},

		eventCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
}
