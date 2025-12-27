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
        
        self._stop = False
        self._proceed = threading.Event()
        self._send_queue: queue.Queue[pb.WorkerStreamMessage] = queue.Queue()
    
    def keep_on(self):
        if not self._proceed.is_set() and not self._stop:
            logger.debug('Worker is set to keep on, proceeding.')
            self._proceed.wait()
    
    def register(self):
        start_time = time.time()
        while True:
            if REGISTER_TIMEOUT is not None and time.time() - start_time > REGISTER_TIMEOUT:
                raise TimeoutError(f'Registration timed out after {REGISTER_TIMEOUT} seconds')
            try:
                req = pb.RegisterInfo(worker_id=self._id, role=self._role)
                res = self._stub.Register(req, timeout=REGISTER_TIMEOUT)
                if res.success:
                    logger.info(f'Worker ({self._id}) registered successfully.')
                    threading.Thread(target=_control_stream_loop, args=(self,), daemon=True).start()
                else:
                    raise RuntimeError(f'Registration failed: {res.message}')
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    logger.debug('Orchestrator not available, retrying...')
                    time.sleep(1.0)
                    continue
            except Exception as e:
                logger.error(f'Unexpected error during registration: {e}')
                raise e
            break
    
    def complete(self):
        self._send_queue.put(pb.WorkerStreamMessage(
            worker_id=self._id,
            type=WSMT.WSM_REPORTSTATUS,
            status=WS.WS_COMPLETED
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
            status = msg.status
            if status == WS.WS_RUNNING:
                if not self._proceed.is_set():
                    logger.debug('Received RUNNING command, proceeding.')
                    self._proceed.set()
                else:
                    logger.warning('Received RUNNING command while already running, skipping.')
            if status == WS.WS_STOP:
                self.stop()
                logger.info(f'Received STOP command, stopping worker ({self._id}).')
                    
                    
            
                    
    
    # def send_request(self, msg_type: pb.WorkerStreamMessageType, payload: str = '') -> threading.Event:
    #     """Send a message to the orchestrator via the control stream."""
    #     m_id = uuid.uuid4().hex
    #     m_event = threading.Event()
    #     self._request_cache[m_id] = (m_event, None)
    #     msg = pb.WorkerStreamMessage(worker_id=self._id, message_id=m_id, type=msg_type, payload=payload)
        
    #     self._req_queue.put(msg)
    #     return m_event
    
    # def connect(self):
    #     # Start control stream thread
    #     threading.Thread(target=_control_stream_loop, args=(self,), daemon=True).start()
        
    #     start_time = time.time()
    #     while True:
    #         if REGISTER_TIMEOUT is not None and time.time() - start_time > REGISTER_TIMEOUT:
    #             raise TimeoutError(f'Connection timed out after {REGISTER_TIMEOUT} seconds')
            
    #         try:
    #             self._request_cache = {} # reset request cache
    #             self._req_queue = queue.Queue() # reset request queue
    #             self._control_stream = self._stub.ControlChannel(self._stream_controller())
                
    #             # Register worker self
    #             if not self.send_request(WSMT.WSM_REGISTER).wait(timeout=REGISTER_TIMEOUT):
    #                 raise TimeoutError('Registration acknowledgment not received in time')
    #             error = self._request_cache.get(self._id, (None, None))[1]
    #             if error:
    #                 raise error
    #         except grpc.RpcError as e:
    #             if e.code() == grpc.StatusCode.UNAVAILABLE:
    #                 logger.debug('Orchestrator not available, retrying...')
    #                 time.sleep(1.0)
    #                 continue
    #             else:
    #                 raise e
    #         except Exception as e:
    #             logger.error(f'Unexpected error during connect: {e}')
    #             raise e
    
    # def _stream_controller(self, heartbeat_interval: float = 3.0):
    #     m_id: str
    #     last_heartbeat = time.time()
    #     while not self._stop:
    #         m_id = ''
    #         try:
    #             # Check for messages in the queue (non-blocking)
    #             try:
    #                 # Wait for a message or timeout to send heartbeat
    #                 timeout = max(0, heartbeat_interval - (time.time() - last_heartbeat))
    #                 msg = self._req_queue.get(timeout=timeout)
    #                 m_id = msg.message_id
    #                 yield msg
    #                 self._req_queue.task_done()
    #             except queue.Empty:
    #                 # Timeout reached, time to send heartbeat
    #                 pass

    #             # Send heartbeat if interval passed
    #             if time.time() - last_heartbeat >= heartbeat_interval:
    #                 yield pb.WorkerStreamMessage(worker_id=self._id, type=WSMT.WSM_HEARTBEAT)
    #                 last_heartbeat = time.time()
                    
    #         except Exception as e:
    #             if m_id and m_id in self._request_cache:
    #                 self._request_cache[m_id][1] = e
    #                 self._request_cache[m_id][0].set()