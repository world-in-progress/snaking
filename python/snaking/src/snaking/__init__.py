import os
import grpc
import time
import atexit
import logging
import threading
from enum import IntEnum
from pathlib import Path
from typing import Generator
from contextlib import contextmanager

from .proto import snaking_pb2 as pb
from .proto import snaking_pb2_grpc as pb_grpc

logger = logging.getLogger(__name__)

# TODO(Dsssyc): SERVER_ADDRESS should be configurable
SERVER_ADDRESS = 'unix:///tmp/controller.sock'

REGISTER_TIMEOUT = 30.0      # seconds
HEARTBEAT_TIMEOUT = 1.0     # seconds

class WorkerStatus(IntEnum):
    STOPPED = 0
    PENDING = 1
    READY = 2
    RUNNING = 3

def _heartbeat_loop(snaking_instance: 'Snaking', interval: float = 3.0):
    snaking_instance._status = WorkerStatus.RUNNING.value
    while snaking_instance.keep_on:
        try:
            status = snaking_instance._heartbeat()
            print(status)
            if status != snaking_instance.status:
                snaking_instance.status = WorkerStatus.STOPPED
                break
            if status == WorkerStatus.STOPPED:
                break
            
            time.sleep(interval)
        except Exception:
            snaking_instance.status = WorkerStatus.STOPPED

class Snaking:
    def __init__(self):
        self._final_step_int: int = 0
        self._current_step_int: int = 0
        self._coupling_step_int: int = 0
        
        self._lock: threading.Lock = threading.RLock()
        
        self._id: str = os.getenv('WORKER_ID')
        self._shared_path: Path | None = None
        self._status: int = WorkerStatus.READY.value
        self._channel: grpc.Channel = grpc.insecure_channel(SERVER_ADDRESS)
        self._stub: pb_grpc.ControllerStub = pb_grpc.ControllerStub(self._channel)
    
    @property
    def shared_path(self) -> Path:
        if self._shared_path is None:
            raise RuntimeError('Shared path is not available before registration.')
        return self._shared_path
    
    @property
    def status(self) -> WorkerStatus:
        with self._lock:
            return WorkerStatus(self._status)
    
    @status.setter
    def status(self, value: WorkerStatus):
        with self._lock:
            self._status = value.value
    
    @property
    def current_step(self) -> int:
        with self._lock:
            return self._current_step_int
    
    @current_step.setter
    def current_step(self, value: int):
        with self._lock:
            self._current_step_int = value
    
    @property
    def coupling_step(self) -> int:
        with self._lock:
            return self._coupling_step_int
    
    @coupling_step.setter
    def coupling_step(self, value: int):
        with self._lock:
            self._coupling_step_int = value
    
    @property
    def final_step(self) -> int:
        with self._lock:
            return self._final_step_int
    
    @final_step.setter
    def final_step(self, value: int):
        with self._lock:
            self._final_step_int = value
    
    @property
    def keep_on(self) -> bool:
        with self._lock:
            if self.current_step >= self.final_step:
                self.status = WorkerStatus.STOPPED
                return False
            
            if self.coupling_step > 0 and self.current_step % self.coupling_step == 0:
                logger.debug(f'Worker {self._id} waiting for sync at step {self.current_step}...')
                self._wait_for_sync()
            return self.status not in {WorkerStatus.STOPPED}
    
    @contextmanager
    def proprocessing(self):
        try:
            self._register()
            yield   
        except TimeoutError:
            # TODO(Dsssyc): Handle registration timeout reasonably
            pass
        except Exception as e:
            self.post_error(f'Error during preprocessing in worker {self._id}: {str(e)}')
        finally:
            self._finish_preprocess()
    
    @contextmanager
    def simulating(self, coupling_step: int):
        try:
            if self._shared_path is not None:
                raise RuntimeError('Worker already in simulating context.')
            
            self._register()
            self._wait_for_ready() # hold on?
            self.coupling_step = coupling_step
            yield
        except Exception as e:
            self.post_error(f'Error during simulation in worker {self._id} at step: {str(e)}')
        finally:
            # TODO(Dsssyc): Implement proper pending/stopping logic
            pass
        
    def post_error(self, error_message: str = ''):
        self.status = WorkerStatus.STOPPED
        req = pb.ErrorMessage(worker_id=self._id, message=error_message)
        self._stub.PostError(req)
        print('Posted error to controller:', error_message)
    
    def _heartbeat(self) -> WorkerStatus:
        req = pb.HeartbeatInfo(worker_id=self._id, status=self._status)
        
        current = time.time()
        timeout_valid = HEARTBEAT_TIMEOUT is not None and HEARTBEAT_TIMEOUT > 0
        while True:
            if timeout_valid and time.time() - current > HEARTBEAT_TIMEOUT:
                raise TimeoutError(f'Heartbeat timed out after {HEARTBEAT_TIMEOUT} seconds')
            try:
                res = self._stub.HeartBeat(req)
                return WorkerStatus(res.status)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    logger.debug('Cannot send heartbeat to orchestrator, retrying...')
                    time.sleep(0.5)
                    continue
    
    def _finish_preprocess(self):
        req = pb.PreprocessFinished(worker_id=self._id)
        self._stub.FinishPreprocess(req)

    def _register(self):
        start_time = time.time()
        while True:
            if REGISTER_TIMEOUT is not None and time.time() - start_time > REGISTER_TIMEOUT:
                raise TimeoutError(f"Registration timed out after {REGISTER_TIMEOUT} seconds")

            try:
                req = pb.RegisterInfo(worker_id=self._id)
                res = self._stub.Register(req)
                if res.success:
                    # Start heartbeat thread
                    heartbeat_thread = threading.Thread(target=_heartbeat_loop, args=(self,), daemon=True)
                    heartbeat_thread.start()
                    self._shared_path = Path(res.shared_data_path)
                    return
                else:
                    # TODO (Dsssyc): Handle registration failure reasonably
                    pass
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    logger.debug("Controller not available, retrying...")
                    time.sleep(0.5)

    def _wait_for_ready(self) -> bool:
        ready_flag = False
        
        while True:
            try:
                response = self._stub.WaitForStart(pb.Empty())
                ready_flag = response.ready
                break
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("Controller not available, retrying...")
                    time.sleep(0.5)
                else:
                    print(f"gRPC error: {e}")
                    break
        
        return ready_flag

    def _wait_for_sync(self) -> bool:
        req = pb.StepStatus(worker_id=self._id, current_step=self._current_step_int)
        sync_res = self._stub.SyncStep(req)
        return sync_res.should_continue

    def _cleanup(self):
        if self._channel is not None:
            self._channel.close()
            self._channel = None
    
snaking = Snaking()

# Helpers ##################################################

atexit.register(snaking._cleanup)
    