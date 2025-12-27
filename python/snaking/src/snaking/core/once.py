from .base import BasicHost
from ..proto import snaking_pb2 as pb
from ..proto import snaking_pb2_grpc as pb_grpc

class OnceHost(BasicHost):
    def __init__(self, id: str, role: pb.WorkerRole):
        super().__init__(id, role)
        self._once: callable = None
    
    def set_once(self, func: callable):
        self._once = func
    
    def start(self):
        self.register()
        self.keep_on()
        
        while not self._stop:
            if self._once is not None:
                self._once()
            self.complete()