import time
from queue import deque
import heapq

class Scheduler:
    def __init__(self):
        self.sequence = 0
        self.ready = deque()
        self.sleeping = []
        self.current = None

    async def sleep(self, delay):
        # current coro wants to sleep
        deadline = time.time() + delay
        self.sequence += 1
        heapq.heappush(self.sleeping, (deadline, self.sequence, self.current))
        self.current = None
        await switch()

    def new_task(self, coro):
        self.ready.append(coro)

    def run(self):
        while self.ready or self.sleeping:
            if not self.ready:
                deadline, _, coro = heapq.heappop(self.sleeping)
                delta = deadline - time.time()
                if delta > 0:
                    time.sleep(delta)
                self.ready.append(coro)
            self.current = self.ready.popleft()
            try:
                self.current.send(None)
                if self.current:
                    self.ready.append(self.current)
            except StopIteration:
                pass

class Awaitable:
    def __await__(self):
        yield

def switch():
    return Awaitable()

async def countdown(n):
    while n > 0:
        print('Down', n)
        await sched.sleep(4)
        n -= 1

async def countup(stop):
    x = 0
    while x < stop:
        print('Up', x)
        await sched.sleep(1)
        x += 1

sched = Scheduler()

# sched.new_task(countdown(5))
# sched.new_task(countup(5))

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
            sched.ready.append(self.waiting.popleft())

    async def put(self, item):
        if self._closed:
            raise QueueClosed()
        self.items.append(item)
        if self.waiting:
            sched.ready.append(self.waiting.popleft())

    async def get(self):
        while not self.items:
            if self._closed:
                raise QueueClosed()
            self.waiting.append(sched.current)
            sched.current = None
            await switch()
        return self.items.popleft()

async def producer(q, count):
    for n in range(count):
        print('Producing', n)
        await q.put(n)
        await sched.sleep(1)
    
    print('Producer done')
    q.close()

async def consumer(q):
    try:
        while True:
            item = await q.get()
            print('Consume', item)
    except QueueClosed:
        print('Consumer done')

q = AsyncQueue()
sched.new_task(producer(q, 10))
sched.new_task(consumer(q))
sched.run()