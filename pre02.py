import time
import logging
import fastdb4py as fdb
from pydantic import BaseModel
from python.snaking.src.snaking import Snaking, Role, FdbShmName

logging.basicConfig(level=logging.DEBUG)

class Point(fdb.Feature):
    x: fdb.F64
    y: fdb.F64
    z: fdb.F64

class InputModel(BaseModel):
    shm_name: FdbShmName

def main(input_data: InputModel):
    time.sleep(3)  # simulate some preprocessing work
    db = fdb.ORM.load(input_data.shm_name)
    ps = db[Point]['points']
    for i in range(99):
        p = ps[i]
        logging.info(f"Point {i}: x={p.x}, y={p.y}, z={p.z}")
    
    db.unlink()
    
if __name__ == '__main__':
    snaking = Snaking('preprocessor-002', Role.PREPROCESSOR)
    snaking.set_once(main)
    snaking.set_in(InputModel)
    snaking.run()
