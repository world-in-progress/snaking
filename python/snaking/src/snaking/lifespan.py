import os
import grpc
import time
import random
import atexit
import logging
from .proto import snaking_pb2 as pb
from .proto import snaking_pb2_grpc as pb_grpc

logger = logging.getLogger(__name__)

# TODO(Dsssyc): SERVER_ADDRESS, WORKER_ID should be configurable
SERVER_ADDRESS = 'unix:///tmp/controller.sock'
WORKER_ID = os.getenv("WORKER_ID", f"worker-{random.randint(1000,9999)}")

def heartbeat(error_message: str = '') -> bool:
    stub = _get_stub()
    
    req = pb.HeartbeatInfo(worker_id=WORKER_ID, error_message=error_message)
    res = stub.HeartBeat(req)
    return res.success

def register():
    stub = _get_stub()
    
    while True:
        try:
            req = pb.RegisterInfo(worker_id=WORKER_ID)
            res = stub.Register(req)
            if res.success:
                break
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                logger.debug("Controller not available, retrying...")
                time.sleep(0.5)
            else:
                logger.error(f"gRPC error during registration: {e}")
                break

def wait_for_ready() -> bool:
    stub = _get_stub()
    ready_flag = False
    
    while True:
        try:
            response = stub.WaitForStart(pb.Empty())
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

def wait_for_sync(simulation_time: int) -> bool:
    stub = _get_stub()
    
    req = pb.StepStatus(worker_id=WORKER_ID, current_step=simulation_time)
    sync_res = stub.SyncStep(req)
    return sync_res.should_continue

# Helpers ##################################################

_channel = None
_stub = None

def _get_stub():
    global _channel, _stub
    if _channel is None:
        _channel = grpc.insecure_channel(SERVER_ADDRESS)
        _stub = pb_grpc.ControllerStub(_channel)
    return _stub

def _cleanup():
    global _channel
    if _channel is not None:
        _channel.close()
        _channel = None

atexit.register(_cleanup)