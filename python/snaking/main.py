import os
import argparse
import fastdb4py as fdb

class Point(fdb.Feature):
    x: fdb.F32
    y: fdb.F32

def wait(read_fd: int):
    try:
        os.read(read_fd, 1)
    except BlockingIOError:
        print('Error: File descriptor is not ready for reading.')
    except Exception as e:
        print(f'Error while reading from fd: {e}')

def create(path: str):
    db = fdb.ORM.create()
    for i in range(10):
        db.push(Point(x=float(i), y=float(i * i)))
    db.save(path)

def read(path: str):
    for p in fdb.ORM.load(path, from_file=True)[Point][Point]:
        print(f'Point(x={p.x}, y={p.y})')

def shm_read(path: str):
    print('Reading from shared memory database:')
    db = fdb.ORM.load(path)
    for p in db[Point][Point]:
        print(f'Point(x={p.x}, y={p.y})')
    db.unlink()

def shm_wait(path: str, read_fd: int):
    print(f'Waiting for data on shared memory database: {path}')
    wait(read_fd)
    shm_read(path)

def clean(path: str):
    os.remove(path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Python FDB Test Utils')
    parser.add_argument('--create', action='store_true', help='Create a test database')
    parser.add_argument('--read', action='store_true', help='Read from the test database')
    parser.add_argument('--shm-read', action='store_true', help='Read from the shared memory database')
    parser.add_argument('--shm-wait', action='store_true', help='Wait for data on the shared memory database')
    parser.add_argument('--clean', action='store_true', help='Clean up test databases')
    parser.add_argument('--path', type=str, help='Path to the database file or shared memory name', required=True)
    parser.add_argument('--fd', type=int, help='File descriptor to wait on for shared memory read')
    args = parser.parse_args()
    
    path = args.path
    
    if args.create:
        create(path)
    if args.read:
        read(path)
    if args.shm_read:
        shm_read(path)
    if args.shm_wait:
        if args.fd is None:
            raise ValueError('File descriptor (--fd) must be provided for shm-wait')
        shm_wait(path, args.fd)
    if args.clean:
        clean(path)