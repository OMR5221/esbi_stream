# Import local libraries:
# from app.lib.classes.stg1_processor import STG1Processor
from app.lib.classes.stg1_processor import STG1Worker
from app.lib.helpers.logger import Logger
from app.lib.helpers.db_config import db_configs

# Import 3rd party libraries:
import argparse
# import multiprocessing
# from multiprocessing import freeze_support
# from functools import partial
from datetime import date, datetime, timedelta
import time
from sqlalchemy import (create_engine, MetaData, Table, Column, ForeignKey, select, func, Integer, String, DateTime)
import pandas as pd
import keyring


# from pathos.multiprocessing import freeze_support, ProcessPool as Pool
import multiprocessing as mp
from functools import partial
import time
import pathlib


class STG1Processor:

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
        # pools = mp.Pool(processes=num_pools)

        worker = STG1Worker(self.start_date, self.end_date, self.min_interval, log_dir)

        print("STARTING Multiprocess per Plant:")
        #mp.map(STG1Worker.plant_thread_gen, [worker] * num_pools, self.plant_ids)
        #mp.close()
        #mp.join()

        with mp.Pool(num_pools) as pool:
            pool.map(worker.plant_thread_gen, self.plant_ids)

        print("FINISHED Multiprocessing per Plant:")


def valid_date(in_date_str):
    try:
        return datetime.strptime(in_date_str, '%Y-%m-%d')
    except ValueError:
        err_msg = "Not a valid Date: {InputDate}".format(in_date_str)
        raise argparse.ArguementTypeError(err_msg)


def get_plants(env, input_plant_ids):

    db = db_configs[env].DB
    service_id = db_configs[env].service_id
    user_name = db_configs[env].logins["es_dw"]["USERNAME"]
    sid = db_configs[env].SID

    es_dw_engine = create_engine('{DB}://{USERNAME}:{PASSWORD}@{SID}'
                    .format(    DB=db,
                                USERNAME=user_name,
                                PASSWORD=keyring.get_password(service_id, user_name),
                                SID=sid
                           )
                 )

    es_dw_conn = es_dw_engine.connect()

    plant_query = """
                    SELECT DISTINCT
                            td.plant_id
                    FROM ES_TAGS_DIM td
                    JOIN ES_PLANT_DIM pd
                        ON td.PLANT_ID = pd.PLANT_ID
                        -- AND pd.status = 'A'
                    """

    if len(input_plant_ids) > 0:

        if len(input_plant_ids) > 1:
            plant_query = plant_query + """
                                        WHERE td.PLANT_ID IN {PlantIDs}
                                        """.format(PlantIDs=input_plant_ids)
        else:
            plant_query = plant_query + """
                                        WHERE td.PLANT_ID = {PlantID}
                                        """.format(PlantID=input_plant_ids[0])

    # Get plant info by sqlalchemy-core:
    plant_resp = es_dw_conn.execute(plant_query)

    return [int(float(pid_rec[0])) for pid_rec in plant_resp.fetchall()]



if __name__ == '__main__':
    mp.freeze_support()

    # Default run is all Active plants for the prior daya in dev:
    start_time = time.time()

    parser = argparse.ArgumentParser(description='Run ESBI ETL process: ')

    # Server to run the process on:
    parser.add_argument('-e', '--env', required=False, type=str, default='dev',
                        choices=['dev','qa','prod'],
                        help='Server descriptor to run process on: [dev, qa, prod]')

    # Operation to run: write to file (file), summary (test), write to db (db), write to s3 (s3)?
    parser.add_argument('-o', '--out', required=False, type=str, default='print',
                        choices=['print','file','db', 's3'],
                        help='How to output the run: [print, file, db, s3]')


    # What plants to process:
    parser.add_argument('-pids', '--plant_ids', type=int, nargs='+', default=[], help='A list of \
                        plant ids to be processed: 34044 1227 1228')
    # day to start process from:
    parser.add_argument('-sd', '--start_date', required=False, type=valid_date,
                        default=(datetime.now() - timedelta(1)).strftime('%Y-%m-%d'), help='Start \
                        Date: YYYY-mm-dd')
    # XOR(end_date, interval)
    end_group = parser.add_mutually_exclusive_group()
    # day to end process on: Prevent incorrect date formats from being entered
    end_group.add_argument('-ed', '--end_date', type=valid_date,
                           default=datetime.now().strftime('%Y-%m-%d'), help='End Date: YYYY-mm-dd')
    # number of days to pull:
    end_group.add_argument('-nd', '--num_days', type=int, default=0)

    # number of minutes worth of data to pull per batch call:
    parser.add_argument('-mi', '--min_int', required=False, type=int, default=60, help='Number of \
                        minutes of data to request at a time: 60, 120, 180')

    # parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS, help='python run.py -pids 34044 1227 -sd 2019-01-01 -ed 2019-01-10')

    args = parser.parse_args()

    # get env to run in:
    env = str(args.env)

    # get output operation
    output = str(args.out)

    # Only tuple not list works in sqlalchemy-core in clause:
    input_plant_ids = get_plants(env, tuple(args.plant_ids))

    start_date = args.start_date
    end_date = args.end_date
    minute_interval = args.min_int

    # Check if correct format entered:
    # start_date = datetime.strptime(start_date_str, '%Y%m%d')

    if args.num_days == 0:
        # end_date = datetime.strptime(end_date_str, '%Y%m%d')
        interval = abs(int((end_date - start_date).days))
    else:
        interval = args.num_days
        end_date = start_date + timedelta(interval)

    print('RUN ENV: ' + str(env))
    print('INPUT PLANTS: ' + str(input_plant_ids))
    #print('START DATE: ' + str(start_date_str))
    #print('END DATE: ' + str(end_date_str))
    print('NUM DAYS: ' + str(interval))
    print('MINUTE INTERVAL: ' + str(minute_interval))

    # Create STG1Processor to begin:
    stg1 = STG1Processor(env, input_plant_ids, start_date, end_date, minute_interval)
    stg1.process()

    duration = time.time() - start_time
    print(f"Completed in {duration} seconds")
