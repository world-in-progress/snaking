import time
import logging
import fastdb4py as fdb
from pydantic import BaseModel
from python.snaking.src.snaking import Snaking, Role, FdbFilePath, FdbShmName

logging.basicConfig(level=logging.DEBUG)

class Point(fdb.Feature):
    x: fdb.F64
    y: fdb.F64
    z: fdb.F64

class InputModel(BaseModel):
    db_path: FdbFilePath
    
class OutputModel(BaseModel):
    shm_name: FdbShmName

def main(input: InputModel) -> OutputModel:
    time.sleep(3)  # simulate some preprocessing work
    db_path = input.db_path
    db = fdb.ORM.truncate([
        fdb.TableDefn(Point, 99, 'points')
    ])
    
    ps = db[Point]['points']
    
    for i in range(99):
        p = ps[i]
        p.x = float(i)
        p.y = float(i)
        p.z = float(i)
    db.save(db_path)

    logging.info(f"Database saved at {db_path}")
    db = fdb.ORM.load(db_path, from_file=True)
    ps = db[Point]['points']
    
    for i in range(99):
        p = ps[i]
        logging.info(f"Point {i}: x={p.x}, y={p.y}, z={p.z}")
    
    db.share('test-points', close_after=True)
    db.close()
    return OutputModel(shm_name='test-points')
    
if __name__ == '__main__':
    snaking = Snaking('preprocessor-001', Role.PREPROCESSOR)
    snaking.set_in(InputModel)
    snaking.set_out(OutputModel)
    snaking.set_once(main)
    snaking.run()
