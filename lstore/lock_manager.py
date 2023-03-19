from collections import defaultdict
import threading


class LockManager:

    def __init__(self):
        self.locks = defaultdict(ReadWriteLock)
        
    def acquire_read(self, rid):
        return self.locks[rid].acquire_read()
    
    def release_read(self, rid):
        return self.locks[rid].release_read()
    
    def acquire_write(self, rid):
        return self.locks[rid].acquire_write()
    
    def release_write(self, rid):
        return self.locks[rid].release_write()



class ReadWriteLock:

    def __init__(self):
        # avoid race cond.
        self._wr_can_use = threading.Lock()

        self._readers = 0
        self._writers = False


    def acquire_read(self):
        self._wr_can_use.acquire()

        if self._writers:
            self._wr_can_use.release()
            print("Read Trans. Dropped")
            return False
        
        self._readers += 1
        self._wr_can_use.release()

        return True
    
    def release_read(self):
        self._wr_can_use.acquire()

        try:
            self._readers -= 1
        finally:
            self._wr_can_use.release()

    def acquire_write(self):
        self._wr_can_use.acquire()

        if self._readers or self._writers:
            self._wr_can_use.release()
            print("Write Trans. Dropped")
            return False
        
        self._writers = True
        self._wr_can_use.release()
        return True
    
    def release_write(self):
        self._wr_can_use.acquire()

        try:
            self._writers = False
        finally:
            self._wr_can_use.release()

