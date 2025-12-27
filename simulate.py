# import os
# os.environ['WORKER_ID'] = 'solver-001'
# import logging
# import fastdb4py as fdb
# from python.snaking.src.snaking import snaking

# logging.basicConfig(level=logging.DEBUG)

# class Point(fdb.Feature):
#     x: fdb.F64
#     y: fdb.F64
#     z: fdb.F64
    
# if __name__ == '__main__':
#     snaking.final_step = 10
    
#     with snaking.simulating(2):
#         while snaking.keep_on:
#             db_path = str(snaking.shared_path / 'points.fdb')
#             db = fdb.ORM.load(db_path, from_file=True)
            
#             ps = db[Point]['points']
#             for p in ps:
#                 p.x += 1.0
#                 p.y += 1.0
#                 p.z += 1.0
            
#             print(f'Point updated to ({ps[0].x}, {ps[0].y}, {ps[0].z})')
#             db.save(db_path)
            
#             snaking.current_step += 1