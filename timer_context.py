import time


class Timer:
    """
    Timer context manager to measure performance time
    """
    def __init__(self):
        pass

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        stop = time.time()
        performance_time = stop - self.start
        print(f"Performance time: {performance_time} second(s)")
