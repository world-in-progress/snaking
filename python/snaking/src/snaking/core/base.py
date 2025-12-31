import os
import grpc
import time
import atexit
import logging
import threading
import queue
import uuid
from enum import IntEnum
from pathlib import Path
from typing import Generator
from contextlib import contextmanager

from ..proto import snaking_pb2 as pb
from ..proto import snaking_pb2_grpc as pb_grpc

logger = logging.getLogger(__name__)
WS = pb.WorkerStatus
WC = pb.WorkerCommand
WSMT = pb.WorkerStreamMessageType
OSMT = pb.OrchestratorStreamMessageType

# TODO(Dsssyc): SERVER_ADDRESS should be configurable
SERVER_ADDRESS = 'unix:///tmp/controller.sock'

REGISTER_TIMEOUT = 30.0     # seconds
HEARTBEAT_TIMEOUT = 1.0     # seconds

def _control_stream_loop(s: 'BasicHost'):
    try:
        s._control_stream = s._stub.ControlChannel(s.stream_controller())
        
        for msg in s._control_stream:
            s.message_handler(msg)
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.CANCELLED:
            pass
        else:
            logger.error(f'Control stream error: {e}')
            s._stop = True
    except Exception as e:
        logger.error(f'Unexpected error in control stream: {e}')
        s._stop = True
    finally:
        logger.info('Control stream loop exited.')
                
class BasicHost:
    def __init__(self, id: str, role: pb.WorkerRole):
        self._id = id
        self._role = role
        self._lock = threading.Lock()
        self._channel: grpc.Channel = grpc.insecure_channel(SERVER_ADDRESS)
        self._stub: pb_grpc.ControllerStub = pb_grpc.ControllerStub(self._channel)
        self._control_stream = None
        self._received_payload: str = ''
        self._args = None
        
        self._stop = False
        self._proceed = threading.Event()
        self._send_queue: queue.Queue[pb.WorkerStreamMessage] = queue.Queue()
    
    def register(self):
        start_time = time.time()
        while True:
            if REGISTER_TIMEOUT is not None and time.time() - start_time > REGISTER_TIMEOUT:
                raise TimeoutError(f'Registration timed out after {REGISTER_TIMEOUT} seconds')
            try:
                req = pb.RegisterInfo(worker_id=self._id, role=self._role)
                res: pb.RegisteredMessage = self._stub.Register(req, timeout=REGISTER_TIMEOUT)
                self._args = res.arg_descriptions
                logger.info(f'Worker ({self._id}) registered successfully.')
                threading.Thread(target=_control_stream_loop, args=(self,), daemon=True).start()
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    logger.debug('Orchestrator not available, retrying...')
                    time.sleep(1.0)
                    continue
            except Exception as e:
                logger.error(f'Unexpected error during registration: {e}')
                raise e
            break
    
    def report_error(self, error_msg: str):
        self._send_queue.put(pb.WorkerStreamMessage(
            worker_id=self._id,
            type=WSMT.WSM_REPORTSTATUS,
            status=WS.WS_FAILED,
            payload=error_msg
        ))
        logger.error(f'Worker ({self._id}) reported error: {error_msg}')
        if self._proceed.is_set():
            self._proceed.clear()
        self._proceed.wait()
    
    def complete(self, out_payload: str = ''):
        if self._stop:
            return
        self._send_queue.put(pb.WorkerStreamMessage(
            worker_id=self._id,
            type=WSMT.WSM_REPORTSTATUS,
            status=WS.WS_COMPLETED,
            payload=out_payload
        ))
        if self._proceed.is_set():
            self._proceed.clear()
        
        logger.info(f'Worker ({self._id}) marked as completed, parking.')
        self._proceed.wait()
    
    def stop(self):
        self._stop = True
        if self._control_stream:
            self._control_stream.cancel()
        self._proceed.set()
    
    @property
    def keep_on(self) -> bool:
        if self._stop:
            if self._control_stream:
                self._control_stream.cancel()
            self._proceed.set()
            return False
        else:
            if not self._proceed.is_set():
                logger.debug('Worker is waiting for proceeding...')
                self._proceed.wait()
            return True
    
    def stream_controller(self):
        yield pb.WorkerStreamMessage(
            worker_id=self._id,
            type=WSMT.WSM_REPORTSTATUS,
            status=WS.WS_IDLE
        )
        
        while not self._stop:
            try:
                msg = self._send_queue.get()
                yield msg
            except Exception as e:
                self._stop = True
                logger.error(f'Error in control stream: {e}')
                break
    
    def message_handler(self, msg: pb.OrchestratorStreamMessage):
        """Handle messages from the orchestrator."""
        type = msg.type
        if type == OSMT.OSM_COMMAND:
            cmd = msg.cmd
            if cmd == WC.WC_START:
                if not self._proceed.is_set():
                    logger.debug('Received RUNNING command, proceeding.')
                    if msg.payload:
                        self._received_payload = msg.payload
                    self._proceed.set()
                else:
                    logger.warning('Received RUNNING command while already running, skipping.')
            if cmd == WC.WC_STOP:
                self._stop = True
                # Make sure to unblock any waiting operations
                if not self._proceed.is_set():
                    self._proceed.set()
                logger.info(f'Received STOP command, stopping worker ({self._id}).')