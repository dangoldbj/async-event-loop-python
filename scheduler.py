from queue import deque
import time
import heapq
from select import select

class Scheduler:
    def __init__(self):
        self.sequence = 0
        self.ready = deque()
        self.sleeping = []
        self.current = None #used by coroutines
        self._read_waiting = {}
        self._write_waiting = {}

    def call_soon(self, func):
        self.ready.append(func)

    def call_later(self, delay, func):
        deadline = time.time() + delay
        heapq.heappush(self.sleeping, (deadline, self.sequence, func))
        self.sequence += 1

    def read_wait(self, fd, func):
        self._read_waiting[fd] = func

    def write_wait(self, fd, func):
        self._write_waiting[fd] = func
    
    def run(self):
        while (self.ready or self.sleeping or self._read_waiting or self._write_waiting):
            if not self.ready:
                if self.sleeping:
                    # deadline, _, func = heapq.heappop(self.sleeping)
                    deadline, _, _ = self.sleeping[0]
                    timeout = deadline - time.time()
                    if timeout < 0:
                        timeout = 0
                else:
                    timeout = None
                # print(f'{timeout=}, {self._read_waiting=} {self._write_waiting=}')
                can_read, can_write, _ = select(self._read_waiting, self._write_waiting, [], timeout)
                # print("UNBLOCKED")
                
                for fd in can_read:
                    self.ready.append(self._read_waiting.pop(fd))
                for fd in can_write:
                    self.ready.append(self._write_waiting.pop(fd))
                
                now = time.time()
                # print(f'{now=}, {self.sleeping=}')
                while self.sleeping:
                    if now > self.sleeping[0][0]:
                        # print('add to ready')
                        self.ready.append(heapq.heappop(self.sleeping)[2])
                    else:
                        break
            # print(f'{self.ready=},  {self.sleeping=}')
            while self.ready:
                func = self.ready.popleft()
                func()

    def new_task(self, coro):
        self.ready.append(Task(coro))

    async def sleep(self, delay):
        self.call_later(delay, self.current)
        self.current = None
        await switch()

    async def recv(self, sock, maxbytes):
        self.read_wait(sock, self.current)
        self.current = None
        await switch()
        return sock.recv(maxbytes)

    async def send(self, sock, data):
        self.write_wait(sock, self.current)
        self.current = None
        await switch()
        return sock.send(data)

    async def accept(self, sock):
        self.read_wait(sock, self.current)
        self.current = None
        await switch()
        return sock.accept()

sched = Scheduler()

class Awaitable:
    def __await__(self):
        yield

def switch():
    return Awaitable()

class Task:
    def __init__(self, coro):
        self.coro = coro

    def __call__(self):
        try:
            sched.current = self
            self.coro.send(None)
            if sched.current:
                sched.ready.append(self)
        except StopIteration:
            pass

## Pub-Sub Using Coroutines
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
# sched.new_task(producer(q, 10))
# sched.new_task(consumer(q))

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


from socket import *
async def tcp_server(addr):
    # print("TCP SERVER INITIATE")
    sock = socket(AF_INET, SOCK_STREAM)
    sock.bind(addr)
    sock.listen(1)
    while True:
        # print("HERE")
        client, addr = await sched.accept(sock)
        # print("CLIENT ACCEPTED")
        sched.new_task(echo_handler(client))

async def echo_handler(sock):
    while True:
        data = await sched.recv(sock, 10000)
        if not data:
            break
        await sched.send(sock, b'Got:' +  data)
    print('Connection closed')
    sock.close()

sched.new_task(tcp_server(('127.0.0.1', 40000)))
sched.run()