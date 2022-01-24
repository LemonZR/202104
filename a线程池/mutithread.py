import threading
from threading import Thread, Lock, RLock
import os, time

x = 0

l = RLock()

li = (i for i in range(100000000))


def countc():
    i = 0
    while i < 5:
        i = i + 1
        time.sleep(10)
        print('threading id :', threading.currentThread().ident)
        print('parent process:', os.getppid())
        print('process id:', os.getpid())


def add_x():
    global x

    for i in range(1000000):
        # print('%s 获取x:值为%d' % (threading.currentThread().ident, x))
        # l.acquire()
        # time.sleep(1)
        x += 1
        # l.release()


def iter_print():
    global li
    global x
    for i in li:
        l.acquire()
        # print('%s 获取x:值为%d' % (threading.currentThread().ident, x))
        x += i
        l.release()
        # time.sleep(1)


if __name__ == '__main__':
    t1 = Thread(target=iter_print)
    t2 = Thread(target=iter_print)

    t1.start()
    t2.start()
    t1.join()
    t2.join()
    print(x)
