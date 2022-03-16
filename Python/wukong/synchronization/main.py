import time
import _thread
from counting_semaphore import CountingSemaphore
from barrier import Barrier

#semaphore = CountingSemaphore(initial_permits = 1, id = 1, semaphore_name = "semaphore")

# print("1")
# semaphore.P()
# print("2")
# semaphore.V()
# print("3")
# semaphore.P()
# print("4")
# semaphore.V()
# print("5")
# semaphore.V()
# print("6")
# semaphore.P()
# print("7")
# semaphore.P()
# print("8")

# semaphore = CountingSemaphore(initial_permits = 0, id = 1, semaphore_name = "semaphore")

# def task(counting_semaphore : CountingSemaphore):
#     print("Starting a task...")
#     counting_semaphore.P()
#     print("Done")

# print("Starting thread...")

# try:
#     _thread.start_new_thread(task, (semaphore,))
# except Exception as ex:
#     print("[ERROR] Failed to start thread.")
#     print(ex)

# print("Sleeping...")

# time.sleep(1)

# print("Done sleeping, calling V()")

# semaphore.V()

# print("Successfully called V()")

barrier = Barrier(monitor_name = "barrier")
barrier.n = 3

def task(b : Barrier):
    print("Calling wait_b()")
    b.wait_b()
    print("Successfully called wait_b()")

try:
    print("Starting first thread")
    _thread.start_new_thread(task, (barrier,))
except Exception as ex:
    print("[ERROR] Failed to start first thread.")
    print(ex)

try:
    print("Starting second thread")
    _thread.start_new_thread(task, (barrier,))
except Exception as ex:
    print("[ERROR] Failed to start second thread.")
    print(ex)

print("Sleeping")
time.sleep(1)
print("Woke up")

barrier.wait_b()