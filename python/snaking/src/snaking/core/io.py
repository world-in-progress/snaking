import json
from enum import Enum
from typing import TypeVar
from pydantic import BaseModel

# Used to point out a generic shared memory name type for FDB 
FdbShmName = TypeVar('FdbShmName', bound=str)

# Used to point out a generic file path type for FDB
FdbFilePath = TypeVar('FdbFilePath', bound=str)

class ArgType(Enum):
    CONST = 'const'
    WORKER_OUTPUT = 'from-worker-output'

def parse_input(input_model: BaseModel, arg_descriptions: str, payload_str: str):
    """
    Parse input arguments based on their descriptions and payloads.
    
    payloads: A JSON string mapping worker IDs to their output payload JSON string.
    """
    arg_ds = json.loads(arg_descriptions)['args']
    payloads: dict[str, any] = {}
    if payload_str:
        for worker_id, payload_str in json.loads(payload_str).items():
            payloads[worker_id] = json.loads(payload_str)
        
    in_dict: dict[str, any] = {}
    for arg in arg_ds:
        arg_name = arg['name']
        arg_type = ArgType(arg['type'])
        
        if arg_type == ArgType.CONST:
            in_dict[arg_name] = arg['value']
        elif arg_type == ArgType.WORKER_OUTPUT:
            from_worker_id = arg['worker-id']
            if from_worker_id not in payloads:
                raise ValueError(f'Payload from worker {from_worker_id} not found for argument {arg_name}')
            from_value_name = arg['output-field']
            in_dict[arg_name] = payloads[from_worker_id][from_value_name]
    
    return input_model.model_validate(in_dict)
    