from .core import Role
from .core import OnceHost

class Snaking:
    def __init__(self, id: str, role: Role):
        pb_role = Role.role_to_pb(role)
        if role == Role.PREPROCESSOR:
            self._host = OnceHost(id, pb_role)
        else:
            pass
    
    def set_once(self, func: callable):
        if isinstance(self._host, OnceHost):
            self._host.set_once(func)
        else:
            raise TypeError('set_once is only available for OnceHost instances.')
    
    def run(self):
        self._host.start()