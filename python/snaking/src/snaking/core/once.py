from .io import parse_input
from .base import BasicHost
from pydantic import BaseModel
from ..proto import snaking_pb2 as pb

class OnceHost(BasicHost):
    def __init__(self, id: str, role: pb.WorkerRole):
        super().__init__(id, role)
        self._in: BaseModel = None
        self._out: BaseModel = None
        self._in_cls: BaseModel = None
        self._out_cls: BaseModel = None
        self._once: callable = None
    
    def set_once(self, func: callable):
        self._once = func
    
    def set_in(self, cls: BaseModel):
        self._in_cls = cls
    
    def set_out(self, cls: BaseModel):
        self._out_cls = cls
    
    def start(self):
        try:
            if self._once is None:
                raise ValueError('Once function not set for OnceHost.')
            
            self.register()
            while self.keep_on:
                if self._in_cls is not None:
                    if self._received_payload:
                        self._in = parse_input(self._in_cls, self._args, self._received_payload)
                        self._received_payload = ''
                    else:
                        raise ValueError('No input payload received for OnceHost.')
                self._out = self._once(self._in)
                
                out_payload = ''
                if self._out_cls is not None:
                    out_payload = self._out.model_dump_json()
                self.complete(out_payload)
                
        except Exception as e:
            self.report_error(str(e))