import os
# os.environ['WORKER_ID'] = 'preprocessor-001'
os.environ['WORKER_ID'] = 'solver-001'
import time
import logging
import fastdb4py as fdb
from python.snaking.src.snaking import snaking

logging.basicConfig(level=logging.DEBUG)

class Point(fdb.Feature):
    x: fdb.F64
    y: fdb.F64
    z: fdb.F64

if __name__ == '__main__':
    with snaking.simulating() as shared_path:
        time.sleep(15)
        db_path = shared_path / 'points.fdb'
        fdb.ORM.truncate([
            fdb.TableDefn(Point, 100, 'points')
        ]).save(str(db_path))
        raise RuntimeError("Simulated preprocessing error")
