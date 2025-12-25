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

from .proto import snaking_pb2 as pb
from .proto import snaking_pb2_grpc as pb_grpc

logger = logging.getLogger(__name__)
WSMT = pb.WorkerStreamMessageType
OSMT = pb.OrchestratorStreamMessageType

# TODO(Dsssyc): SERVER_ADDRESS should be configurable
SERVER_ADDRESS = 'unix:///tmp/controller.sock'

REGISTER_TIMEOUT = 30.0     # seconds
HEARTBEAT_TIMEOUT = 1.0     # seconds

def _control_stream_loop(s: 'Snaking'):
    m_id: str = ''
    try:
        for msg in s._control_stream:
            m_id = msg.message_id
            s.message_handler(msg.message_id, msg.type, msg.payload)
    except Exception as e:
        if m_id and m_id in s._request_cache:
            s._request_cache[m_id][1] = e
            s._request_cache[m_id][0].set()
        else:
            for idx in range(len(s._request_cache)):
                s._request_cache[list(s._request_cache.keys())[idx]][1] = e
                s._request_cache[list(s._request_cache.keys())[idx]][0].set()
                
class Snaking:
    def __init__(self, id: str):
        self._id = id
        self._lock = threading.Lock()
        self._channel: grpc.Channel = grpc.insecure_channel(SERVER_ADDRESS)
        self._stub: pb_grpc.ControllerStub = pb_grpc.ControllerStub(self._channel)
        self._control_stream = self._stub.ControlChannel(self._stream_controller())
        
        self._stop = False
        self._req_queue: queue.Queue[pb.WorkerStreamMessage] = queue.Queue()
        self._request_cache: dict[str, tuple[threading.Event, Exception | None]] = {}
        self._registered = threading.Event()
    
    def connect(self):
        # Start control stream thread
        threading.Thread(target=_control_stream_loop, args=(self,), daemon=True).start()
        
        start_time = time.time()
        while True:
            if REGISTER_TIMEOUT is not None and time.time() - start_time > REGISTER_TIMEOUT:
                raise TimeoutError(f'Connection timed out after {REGISTER_TIMEOUT} seconds')
            
            try:
                self._request_cache = {} # reset request cache
                self._req_queue = queue.Queue() # reset request queue
                self._control_stream = self._stub.ControlChannel(self._stream_controller())
                
                # Register worker self
                if not self.send_request(WSMT.WSM_REGISTER).wait(timeout=REGISTER_TIMEOUT):
                    raise TimeoutError('Registration acknowledgment not received in time')
                error = self._request_cache.get(self._id, (None, None))[1]
                if error:
                    raise error
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    logger.debug('Orchestrator not available, retrying...')
                    time.sleep(1.0)
                    continue
                else:
                    raise e
            except Exception as e:
                logger.error(f'Unexpected error during connect: {e}')
                raise e
    
    def send_request(self, msg_type: pb.WorkerStreamMessageType, payload: str = '') -> threading.Event:
        """Send a message to the orchestrator via the control stream."""
        m_id = uuid.uuid4().hex
        m_event = threading.Event()
        self._request_cache[m_id] = (m_event, None)
        msg = pb.WorkerStreamMessage(worker_id=self._id, message_id=m_id, type=msg_type, payload=payload)
        
        self._req_queue.put(msg)
        return m_event
    
    def _stream_controller(self, heartbeat_interval: float = 3.0):
        m_id: str
        last_heartbeat = time.time()
        while not self._stop:
            m_id = ''
            try:
                # Check for messages in the queue (non-blocking)
                try:
                    # Wait for a message or timeout to send heartbeat
                    timeout = max(0, heartbeat_interval - (time.time() - last_heartbeat))
                    msg = self._req_queue.get(timeout=timeout)
                    m_id = msg.message_id
                    yield msg
                    self._req_queue.task_done()
                except queue.Empty:
                    # Timeout reached, time to send heartbeat
                    pass

                # Send heartbeat if interval passed
                if time.time() - last_heartbeat >= heartbeat_interval:
                    yield pb.WorkerStreamMessage(worker_id=self._id, type=WSMT.WSM_HEARTBEAT)
                    last_heartbeat = time.time()
                    
            except Exception as e:
                if m_id and m_id in self._request_cache:
                    self._request_cache[m_id][1] = e
                    self._request_cache[m_id][0].set()
    
    def message_handler(self, m_id: str, type: pb.OrchestratorStreamMessageType, payload: str = ''):
        """Handle messages from the orchestrator."""
        if type == OSMT.OSM_COMMAND:
            logger.info(f'Received command: {payload}')
            if m_id in self._request_cache:
                self._request_cache[m_id][0].set()
        elif type == OSMT.OSM_ACKNOWLEDGE:
            logger.info(f'Worker ({self._id}) registered successfully.')
            if m_id in self._request_cache:
                self._request_cache[m_id][0].set()
        else:
            logger.warning(f'Unknown message type received: {type}')