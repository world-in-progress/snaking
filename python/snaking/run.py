import sys
import time
import grpc
import struct
import os
import random

# from src.proto import snaking_pb2 as pb
# from src.proto import snaking_pb2_grpc as pb_grpc

# SERVER_ADDRESS = 'unix:///tmp/controller.sock'

# channel = grpc.insecure_channel(SERVER_ADDRESS)
# stub = pb_grpc.ControllerStub(channel)

# while True:
#     try:
#         response = stub.WaitForStart(pb.Empty())
#         if response.ready:
#             print("Received start signal from controller.")
#             break
#     except grpc.RpcError as e:
#         if e.code() == grpc.StatusCode.UNAVAILABLE:
#             print("Controller not available, retrying...")
#             time.sleep(1)
#         else:
#             print(f"gRPC error: {e}")
#             sys.exit(1)

from src.snaking import is_ready, wait_for_sync

is_ready()