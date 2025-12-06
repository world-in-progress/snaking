import sys
import time
import grpc
import struct
import os
import random
import fastdb4py as fdb

from src.snaking import wait_for_ready, wait_for_sync

if __name__ == '__main__':
    wait_for_ready()
    