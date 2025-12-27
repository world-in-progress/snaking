package worker

import (
	pb "snaking/internal/proto"
)

func getRoleEnum(role string) pb.WorkerRole {
	switch role {
	case "preprocessor":
		return pb.WorkerRole_WR_PREPROCESSOR
	case "solver":
		return pb.WorkerRole_WR_SOLVER
	case "postprocessor":
		return pb.WorkerRole_WR_POSTPROCESSOR
	default:
		return pb.WorkerRole_WR_UNKNOWN
	}
}

type WorkerInfo struct {
	Id   string `json:"id"`
	Role string `json:"role"`
}

type Worker struct {
	Id         string
	Role       pb.WorkerRole
	Status     pb.WorkerStatus
	Connecting bool

	stream pb.Controller_ControlChannelServer
}

func New(info *WorkerInfo) *Worker {
	return &Worker{
		Id:         info.Id,
		Role:       getRoleEnum(info.Role),
		Status:     pb.WorkerStatus_WS_IDLE,
		Connecting: false,
	}
}

func (w *Worker) changeRemoteStatus(newStatus pb.WorkerStatus) error {
	if w.Status == newStatus {
		return nil
	}

	cmd := &pb.OrchestratorStreamMessage{
		Type:   pb.OrchestratorStreamMessageType_OSM_COMMAND,
		Status: newStatus,
	}
	if err := w.stream.Send(cmd); err != nil {
		return err
	}
	w.Status = newStatus
	return nil
}

func (w *Worker) Connect(stream pb.Controller_ControlChannelServer) {
	w.stream = stream
	w.Connecting = true
}

func (w *Worker) Disconnect() {
	w.stream = nil
	w.Connecting = false
}

func (w *Worker) Run() error {
	return w.changeRemoteStatus(pb.WorkerStatus_WS_RUNNING)
}

func (w *Worker) Stop() error {
	return w.changeRemoteStatus(pb.WorkerStatus_WS_STOP)
}

func (w *Worker) HandleMessage(msg *pb.WorkerStreamMessage) {
	switch msg.Type {
	case pb.WorkerStreamMessageType_WSM_REPORTSTATUS:
		if statusMsg, ok := msg.Payload.(*pb.WorkerStreamMessage_Status); ok {
			switch statusMsg.Status {
			case pb.WorkerStatus_WS_COMPLETED:
				w.Status = pb.WorkerStatus_WS_COMPLETED
			}

		}
	}
}
