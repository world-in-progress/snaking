import os
import grpc
import time
import random
import atexit
import logging
import threading
from enum import Enum
from pathlib import Path
from typing import Generator
from contextlib import contextmanager

from .proto import snaking_pb2 as pb
from .proto import snaking_pb2_grpc as pb_grpc

logger = logging.getLogger(__name__)

# TODO(Dsssyc): SERVER_ADDRESS should be configurable
SERVER_ADDRESS = 'unix:///tmp/controller.sock'

REGISTER_TIMEOUT = 2.0      # seconds
HEARTBEAT_TIMEOUT = 1.0     # seconds

class WorkerStatus(Enum):
    READY = 0
    RUNNING = 1
    STOPPING = 2
    STOPPED = 3
    PENDING = 4

def _heartbeat_loop(snaking_instance: 'Snaking', interval: float = 3.0):
    snaking_instance._status = WorkerStatus.RUNNING
    while snaking_instance.alive:
        try:
            status = snaking_instance._heartbeat()
            print(status)
            if status != snaking_instance._status:
                snaking_instance._status = WorkerStatus.STOPPED
                break
            if status == WorkerStatus.STOPPED:
                break
            
            time.sleep(interval)
        except Exception:
            snaking_instance._status = WorkerStatus.STOPPED

class Snaking:
    def __init__(self):
        self._id: str = os.getenv('WORKER_ID')
        self._status: WorkerStatus = WorkerStatus.READY
        self._channel: grpc.Channel = grpc.insecure_channel(SERVER_ADDRESS)
        self._stub: pb_grpc.ControllerStub = pb_grpc.ControllerStub(self._channel)
    
    @property
    def alive(self) -> bool:
        return self._status not in {WorkerStatus.STOPPING, WorkerStatus.STOPPED}
    
    @contextmanager
    def proprocessing(self) -> Generator[Path, None, None]:
        try:
            shared_path = self._register()
            yield shared_path
        except TimeoutError:
            # TODO(Dsssyc): Handle registration timeout reasonably
            pass
        except Exception as e:
            self.post_error(f'Error during preprocessing in worker {self._id}: {str(e)}')
        finally:
            self._finish_preprocess()
    
    @contextmanager
    def simulating(self):
        try:
            shared_path = self._register()
            self._wait_for_ready()
            yield shared_path
        finally:
            # TODO(Dsssyc): Implement proper pending/stopping logic
            pass
        
    def post_error(self, error_message: str = ''):
        self._status = WorkerStatus.STOPPED
        req = pb.ErrorMessage(worker_id=self._id, message=error_message)
        self._stub.PostError(req)
        print('Posted error to controller:', error_message)
    
    def _heartbeat(self) -> WorkerStatus:
        req = pb.HeartbeatInfo(worker_id=self._id, status=self._status.value)
        
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

    def _register(self) -> Path:
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
                    return Path(res.shared_data_path)
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

    def wait_for_sync(self, simulation_time: int) -> bool:
        req = pb.StepStatus(worker_id=self._id, current_step=simulation_time)
        sync_res = self._stub.SyncStep(req)
        return sync_res.should_continue

    def _cleanup(self):
        if self._channel is not None:
            self._channel.close()
            self._channel = None
    
snaking = Snaking()

# Helpers ##################################################

atexit.register(snaking._cleanup)
    