import os
import grpc
import time
import random
from .proto import snaking_pb2 as pb
from .proto import snaking_pb2_grpc as pb_grpc

# TODO(Dsssyc): SERVER_ADDRESS, WORKER_ID should be configurable
SERVER_ADDRESS = 'unix:///tmp/controller.sock'
WORKER_ID = os.getenv("WORKER_ID", f"worker-{random.randint(1000,9999)}")

def is_ready() -> bool:
    channel = grpc.insecure_channel(SERVER_ADDRESS)
    stub = pb_grpc.ControllerStub(channel)
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
    
    channel.close()
    return ready_flag

def wait_for_sync(simulation_time: int) -> bool:
    channel = grpc.insecure_channel(SERVER_ADDRESS)
    stub = pb_grpc.ControllerStub(channel)
    
    req = pb.StepStatus(worker_id=WORKER_ID, current_step=simulation_time)
    sync_res = stub.SyncStep(req)
    channel.close()
    return sync_res.should_continue