package worker

import (
	"encoding/json"
	"fmt"
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

type TaskTrigger interface {
	NotifyTaskStatus(id string, status pb.WorkerStatus)
}

type ArgFromWorkerOutput struct {
	Name        string `json:"name"`
	WorkerId    string `json:"worker-id"`
	OutputField string `json:"output-field"`
}

type WorkerInfo struct {
	Id   string            `json:"id"`
	Role string            `json:"role"`
	Args []json.RawMessage `json:"args"`
}

type Worker struct {
	Id         string
	Role       pb.WorkerRole
	Status     pb.WorkerStatus
	Connecting bool
	Args       []json.RawMessage

	trigger TaskTrigger
	stream  pb.Controller_ControlChannelServer
}

func New(info *WorkerInfo) *Worker {
	return &Worker{
		Id:         info.Id,
		Role:       getRoleEnum(info.Role),
		Status:     pb.WorkerStatus_WS_IDLE,
		Connecting: false,
		Args:       info.Args,
		trigger:    nil,
		stream:     nil,
	}
}

func (w *Worker) SetTrigger(trigger TaskTrigger) {
	w.trigger = trigger
}

func (w *Worker) GetArgDescriptions() string {
	descriptions := struct {
		Args []json.RawMessage `json:"args"`
	}{
		Args: w.Args,
	}
	bytes, err := json.Marshal(descriptions)
	if err != nil {
		return ""
	}
	return string(bytes)
}

func (w *Worker) sendCommand(command pb.WorkerCommand, payload string) error {
	if w.stream == nil {
		return fmt.Errorf("worker not connected")
	}

	cmd := &pb.OrchestratorStreamMessage{
		Type: pb.OrchestratorStreamMessageType_OSM_COMMAND,
		Content: &pb.OrchestratorStreamMessage_Cmd{
			Cmd: command,
		},
		Payload: payload,
	}

	if err := w.stream.Send(cmd); err != nil {
		return err
	}
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

func (w *Worker) Run(dependencies map[string]string) error {
	bytes, err := json.Marshal(dependencies)
	if err != nil {
		return fmt.Errorf("failed to marshal dependencies: %w", err)
	}
	return w.sendCommand(pb.WorkerCommand_WC_START, string(bytes))
}

func (w *Worker) Stop() error {
	// Avoid sending stop command if not connected
	if w.stream == nil {
		w.Status = pb.WorkerStatus_WS_STOP
		return nil
	}
	return w.sendCommand(pb.WorkerCommand_WC_STOP, "")
}
