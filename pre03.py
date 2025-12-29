import time
import logging
import fastdb4py as fdb
from python.snaking.src.snaking import Snaking, Role

logging.basicConfig(level=logging.DEBUG)

class Point(fdb.Feature):
    x: fdb.F64
    y: fdb.F64
    z: fdb.F64

def main():
    time.sleep(3)  # simulate some preprocessing work
    db_path = './points.fdb'
    db = fdb.ORM.load(db_path, from_file=True)
    ps = db[Point]['points']
    for i in range(99):
        p = ps[i]
        logging.info(f"Point {i}: x={p.x}, y={p.y}, z={p.z}")
    
if __name__ == '__main__':
    snaking = Snaking('preprocessor-003', Role.PREPROCESSOR)
    snaking.set_once(main)
    snaking.run()
