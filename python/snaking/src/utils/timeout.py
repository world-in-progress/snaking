import time
from typing import Generator
from functools import wraps
from contextlib import contextmanager


def timeout(seconds: float | None):
    "A decorator to timeout a function after a given number of seconds."
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if seconds is None:
                return func(*args, **kwargs)
            
            current_time = time.time()
            while True:
                if time.time() - current_time > seconds:
                    raise TimeoutError(f'Function {func.__name__} timed out after {seconds} seconds')
                try:
                    return func(*args, **kwargs)
                except Exception:
                    time.sleep(0.1)
        return wrapper
    return decorator