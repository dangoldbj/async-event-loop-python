from queue import deque
import time
import heapq

class Scheduler:
    def __init__(self):
        self.sequence = 0
        self.ready = deque()
        self.sleeping = []

    def call_soon(self, func):
        self.ready.append(func)

    def call_later(self, delay, func):
        deadline = time.time() + delay
        heapq.heappush(self.sleeping, (deadline, self.sequence, func))
        self.sequence += 1
    
    def run(self):
        while self.ready or self.sleeping:
            if not self.ready:
                deadline, _, func = heapq.heappop(self.sleeping)
                delta = deadline - time.time()
                if delta > 0:
                    time.sleep(delta)
                self.ready.append(func)
        
            while self.ready:
                func = self.ready.popleft()
                func()

sched = Scheduler()

def countdown(n):
    if n > 0:
        print('Down', n)
        sched.call_later(4, lambda: countdown(n - 1))

def countup(stop, x=0):
    def _run(x):
        if x < stop:
            print('Up', x)
            sched.call_later(1, lambda: _run(x + 1))
    _run(0)

# sched.call_soon(lambda: countdown(5))
# sched.call_soon(lambda: countup(20))

class QueueClosed(Exception):
    pass

class AsyncQueue:
    def __init__(self):
        self.items = deque()
        self.waiting = deque()
        self._closed = False

    def close(self):
        self._closed = True
        if self.waiting and not self.items:
            for func in self.waiting:
                sched.call_soon(func)
    
    def put(self, item):
        if self._closed:
            raise QueueClosed()

        self.items.append(item)
        if self.waiting:
            func = self.waiting.popleft()
            sched.call_soon(func)

    def get(self, callback):
        if self.items:
            callback(Result(value=self.items.popleft()))
        else:
            if self._closed:
                callback(Result(exc=QueueClosed()))
            else:
                self.waiting.append(lambda: self.get(callback))

class Result:
    def __init__(self, value=None, exc=None):
        self.exc = exc
        self.value = value
    
    def result(self):
        if self.exc:
            raise self.exc
        
        return self.value

def producer(q, count):
    def _run(n):
        if n < count:
            print('Producing', n)
            q.put(n)
            sched.call_later(1, lambda: _run(n + 1))
        else:
            print('Producer Done')
            q.close()
    _run(0)
    
def consumer(q, label='a'):
    def _consume(result):
        try:
            item = result.result()
            print('Consuming', label, item)
            sched.call_soon(lambda: consumer(q))
        except QueueClosed:
            print('Consumer Done', label)
    q.get(_consume)

q = AsyncQueue()
sched.call_soon(lambda: producer(q, 10))
sched.call_soon(lambda: consumer(q))
sched.run()