import os
os.environ['WORKER_ID'] = 'preprocessor-001'
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
    with snaking.preprocessing():
        time.sleep(5)  # Simulate some preprocessing work
        db_path = snaking.shared_path / 'points.fdb'
        db = fdb.ORM.truncate([
            fdb.TableDefn(Point, 100, 'points')
        ])
        
        ps = db[Point]['points']
        
        for i in range(100):
            p = ps[i]
            p.x = float(i)
            p.y = float(i) * 2.0
            p.z = float(i) * 3.0
        db.save(str(db_path))
    
        logging.info(f"Database saved at {db_path}")
        db = fdb.ORM.load(str(db_path), from_file=True)
        ps = db[Point]['points']
        
        for i in range(100):
            p = ps[i]
            logging.info(f"Point {i}: x={p.x}, y={p.y}, z={p.z}")
