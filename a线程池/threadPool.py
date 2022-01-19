from concurrent.futures import ThreadPoolExecutor
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED


# 定义一个准备作为线程任务的函数
def action(max):
    my_sum = 0
    for i in range(max):
        print(threading.current_thread().name + '  ' + str(i))
        my_sum += i
    return my_sum


# # 创建一个包含2条线程的线程池
# pool = ThreadPoolExecutor(max_workers=2)
# # 向线程池提交一个task, 50会作为action()函数的参数
# future1 = pool.submit(action, 50)
# # 向线程池再提交一个task, 100会作为action()函数的参数
# future2 = pool.submit(action, 100)
# # 判断future1代表的任务是否结束
# print(future1.done())
# time.sleep(3)
# # 判断future2代表的任务是否结束
# print(future2.done())
# # 查看future1代表的任务返回的结果
# print(future1.result())
# # 查看future2代表的任务返回的结果
# print(future2.result())
# # 关闭线程池
# pool.shutdown()


# 2222222222222222222
# https://www.jianshu.com/p/b9b3d66aa0be
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import time


# 参数times用来模拟网络请求的时间

def get_html(times,a):
    time.sleep(times)
    print("get page {}s finished".format(times))
    return times


executor = ThreadPoolExecutor(max_workers=2)
urls = [3, 2, 4]  # 并不是真的url
all_task = [executor.submit(get_html, url,'a') for url in urls]
print(type(all_task))
print(wait(all_task, return_when=ALL_COMPLETED))
print("main")
# 执行结果
# get page 2s finished
# get page 3s finished
# get page 4s finished
# main
