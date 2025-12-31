from pydantic import BaseModel

from .core import Role
from .core import OnceHost

class Snaking:
    def __init__(self, id: str, role: Role):
        pb_role = Role.role_to_pb(role)
        if role == Role.PREPROCESSOR:
            self._host = OnceHost(id, pb_role)
        else:
            pass
    
    def set_in(self, cls: BaseModel):
        if isinstance(self._host, OnceHost):
            self._host.set_in(cls)
        else:
            raise TypeError('set_in is only available for OnceHost instances.')
    
    def set_out(self, cls: BaseModel):
        if isinstance(self._host, OnceHost):
            self._host.set_out(cls)
        else:
            raise TypeError('set_out is only available for OnceHost instances.')
    
    def set_once(self, func: callable):
        if isinstance(self._host, OnceHost):
            self._host.set_once(func)
        else:
            raise TypeError('set_once is only available for OnceHost instances.')
    
    def run(self):
        self._host.start()