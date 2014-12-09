import threading

# A single point of contact to terminate all of the threads.
GLOBAL_EXIT_EVENT = threading.Event()
