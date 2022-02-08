from datetime import datetime

class performance_timer:
    def __init__(self):
        self.start = datetime.now()
        self.elapsed_time_s = 0

    def restart(self):
        self.start = datetime.now()
        self.start_inter = datetime.now()

    def stop(self):
        self.end = datetime.now()
        delta = (self.end - self.start)
        self.elapsed_time_s=delta.total_seconds()
        return self.elapsed_time_s