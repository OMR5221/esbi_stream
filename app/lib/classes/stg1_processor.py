# Class that controls creation of plants multiprocessing:
from .stg1_worker import STG1Worker

# from pathos.multiprocessing import freeze_support, ProcessPool as Pool
import multiprocessing as mp
from functools import partial
import time
import pathlib


class STG1Processor():

    def __init__(self, env, input_plant_ids, start_date, end_date, minute_interval):
        self.env = env
        self.plant_ids = input_plant_ids
        self.start_date = start_date
        self.end_date = end_date
        self.min_interval = minute_interval

    def process(self):
        # Create the log_dir:
        curr_dir = pathlib.Path.cwd()
        log_dir = curr_dir.joinpath('logs')
        log_dir.mkdir(exist_ok=True)

        num_pools = int(len(self.plant_ids))
        p = Pool(num_pools)

        worker = STG1Worker(self.start_date, self.end_date, self.min_interval, log_dir)

        print("STARTING Multiprocess per Plant:")

        p.map(STG1Worker.plant_thread_gen, [worker] * num_pools, self.plant_ids)

        print("FINISHED Multiprocessing per Plant:")

if __name__ == '__main__':
    freeze_support()
