from enum import IntEnum
from .once import OnceHost
from ..proto import snaking_pb2 as pb

class Role(IntEnum):
    SOLVER = 0
    PREPROCESSOR = 1
    POSTPROCESSOR = 2

    @staticmethod
    def role_to_pb(role: 'Role') -> pb.WorkerRole:
        if role == Role.SOLVER:
            return pb.WR_SOLVER
        elif role == Role.PREPROCESSOR:
            return pb.WR_PREPROCESSOR
        elif role == Role.POSTPROCESSOR:
            return pb.WR_POSTPROCESSOR
        else:
            return pb.WR_UNKNOWN