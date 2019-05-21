from lib.api.interpolated_results import GetInterpolatedValues

from sqlalchemy import (create_engine, MetaData, Table, Column, ForeignKey, select, func, Integer, String, DateTime)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import (sessionmaker, Session, scoped_session)
from datetime import date, datetime, timedelta
from sqlalchemy.ext.declarative import declarative_base
from pandas import DataFrame, merge
import pandas as pd
import requests
# Parse response for the needed values to load into STG1 table:
from bs4 import BeautifulSoup
import os
import json
import requests
import sys
import getopt
import argparse
import multiprocessing
from functools import partial
import time
import threading
import concurrent.futures
import pathlib
import csv


def get_plants(input_plant_ids):

    es_dw_engine = create_engine('oracle://SCHEMA:password@DB')
    es_dw_conn = es_dw_engine.connect()

    plant_query = """
                    SELECT DISTINCT
                            td.plant_id
                    FROM ES_TAGS_DIM td
                    JOIN ES_PLANT_DIM pd
                        ON td.PLANT_ID = pd.PLANT_ID
                        AND pd.status = 'A'
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


def get_tags(plant_id):

    es_dw_engine = create_engine('oracle://SCHEMA:password@DB')
    es_dw_conn = es_dw_engine.connect()

    tag_query = """
                    SELECT DISTINCT
                        td.tag_name
                    FROM ES_TAGS_DIM td
                    WHERE td.plant_id = {PlantID}
                """.format(PlantID=plant_id)

    # Get plant info by sqlalchemy-core:
    tag_resp = es_dw_conn.execute(tag_query)

    return [str(tag_name) for tag_name in tag_resp.fetchall()]

def calc_interval(start_time_str, end_time_str):

    print("STARTING INTERVAL CALC")
    start_datetime = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')

    time_diff = (end_datetime - start_datetime)

    day_diff = int(time_diff.days)

    if day_diff > 0:
            days_to_mins = (day_diff * 24) * 60
    else:
            days_to_mins = 0

    secs_to_mins = (time_diff.seconds / 60) + 1

    num_min_str = str(int(days_to_mins + secs_to_mins))

    print(num_min_str)

    return num_min_str


def get_control_params(plant_id):

    print("RUNNING PLANT_ID: " + str(plant_id))

    es_dw_engine = create_engine('oracle://SCHEMA:password@DB')
    es_dw_conn = es_dw_engine.connect()

    # Get tags and plant info:
    try:
        es_tags_dim_res = es_dw_conn.execute("""
                                             select td.plant_id, td.tag_name, td.server_name,
                                             td.pi_method, pd.plant_code
                                             from ES_TAGS_DIM td
                                             JOIN ES_PLANT_DIM pd
                                                ON td.PLANT_ID = pd.PLANT_ID AND pd.status = 'A'
                                             WHERE td.PLANT_ID={PlantID}""".format(PlantID=plant_id))

    except:
        print("Plant not found")
        raise

    es_tags_df = pd.DataFrame(es_tags_dim_res.fetchall())
    es_tags_df.columns = [k.upper() for k in es_tags_dim_res.keys()]

    return es_tags_df


def gen_timeframe():

    # Date config params:
    INTERVAL = 1  # days
    MINUTES = 5  # 60 * 24 = # of minutes in a day

    minutes_in_timerange = INTERVAL * MINUTES
    today = datetime.utcnow().date()
    start_dt = datetime.strptime('2018-12-01', '%Y-%m-%d')
    # end_dt = datetime(today.year, today.month, today.day)
    # start_dt = end_dt - timedelta(days=INTERVAL)

    timerange_list = {i: start_dt + timedelta(minutes=i)
                      for i in range(minutes_in_timerange)}

    dateranges = []

    for key, value in timerange_list.items():
        row = {}
        row['DateId'] = key
        row['DateStr'] = value.strftime('%Y-%m-%d %H:%M:%S')
        row['DateVal'] = value
        dateranges.append(row)

    return pd.DataFrame(dateranges)


# Merge Plant, Tags data with Timeframes
def merge_control_time(param_df, time_df):

    return param_df.assign(foo=1).merge(time_df.assign(foo=1)).drop('foo', 1)


def gen_requests():

    control_params_df = get_control_params
    timespan_df = gen_timeframe()
    call_data_df = merge_control_time(control_params_df, timespan_df)

    call_objs = []

    # Create the GetArchive Objects with the post data:
    for index, rec in call_data_df.iterrows():
        call_obj = GetArchiveValue(rec['SERVER_NAME'], rec['TAG_NAME'], rec['DateStr'])
        call_objs.append(call_obj)

    return call_objs

def createLogger(logger_name, log_dir, log_filename):

    import logging

    logger = logging.getLogger(str(logger_name))
    logger.setLevel(logging.DEBUG)

    log_filename = '{}.log'.format(str(log_filename))
    log_file_handler = logging.FileHandler(log_dir.joinpath(log_filename))
    log_file_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_file_handler.setFormatter(formatter)
    logger.addHandler(log_file_handler)

    return logger

def plant_thread_gen(start_date, end_date, log_dir, min_interval, plant_id):


    # Create new log directory for the plant:
    plant_dir = log_dir.joinpath('{PlantID}'.format(PlantID=plant_id))
    plant_dir.mkdir(exist_ok=True)


    #logger.info("RUNNING PLANT_ID: " + str(plant_id))
    #logger.info("START DATE: " + str(start_date))
    #logger.info("END DATE: " + str(end_date))

    num_days = (end_date - start_date).days + 1
    # logger.info("RUNNING FOR " + str(num_days) + " DAYS")

    # Define configuration settings that can be used to control the queries that call the process:
    INTERVAL = 1  # days
    MINUTES_PER_HOUR = 60
    MINUTES_PER_DAY = 1440  # 60 * 24 = # of minutes in a day
    IPR_MIN_INTERVAL = min_interval  # number of minutes per api pull
    IPR_MIN_RANGE = int(MINUTES_PER_DAY / IPR_MIN_INTERVAL)

    es_dw_engine = create_engine('oracle://SCHEMA:password@DB')
    es_dw_conn = es_dw_engine.connect()

    es_ods_engine = create_engine('oracle://SCHEMA:password@DB')

    # produce our own MetaData object
    es_dw_metadata = MetaData()
    es_dw_metadata.reflect(es_dw_engine, only=['es_tags_dim', 'es_plant_dim'])

    # we can then produce a set of mappings from this MetaData.
    es_dw_base = automap_base(metadata=es_dw_metadata)

    # calling prepare() just sets up mapped classes and relationships.
    es_dw_base.prepare()

    # Prepare the ODS STAGE 1 Table:
    es_ods_metadata = MetaData()

    scada_stg1_table = Table('es_scada_stg1', es_ods_metadata,
                             Column("plant_id", Integer, primary_key=True),
                             Column("plant_code", String, primary_key=True),
                             Column("tagname", String, primary_key=True),
                             autoload=True, autoload_with=es_ods_engine)

    #es_ods_metadata.reflect(es_ods_engine, only=['es_scada_stg1'])

    # we can then produce a set of mappings from this MetaData.
    es_ods_base = automap_base(metadata=es_ods_metadata)

    # calling prepare() just sets up mapped classes and relationships.
    es_ods_base.prepare()

    # mapped classes are now created with names by default
    # matching that of the table name.
    tags_dim = es_dw_base.classes.es_tags_dim
    plant_dim = es_dw_base.classes.es_plant_dim

    es_dw_session = Session(bind=es_dw_engine)

    es_ods_session_factory = sessionmaker(bind=es_ods_engine)
    es_ods_session = scoped_session(es_ods_session_factory)

    plant_tag_query = """
                        SELECT
                            td.plant_id,
                            td.tag_type,
                            td.tag_name,
                            td.server_name,
                            td.pi_method,
                            pd.plant_code
                    FROM ES_TAGS_DIM td
                    JOIN ES_PLANT_DIM pd
                        ON td.PLANT_ID = pd.PLANT_ID
                        AND pd.status = 'A'
                    """
    plant_tag_query = plant_tag_query + """
                        WHERE td.PLANT_ID = {PlantID}
                        """.format(PlantID=plant_id)

    # Get tags and plant info:
    try:
        # Get plant info by sqlalchemy-core:
        es_tags_dim_res = es_dw_conn.execute(plant_tag_query)

    except:
        print('No data to process for the site')
        raise

    es_tags_df = pd.DataFrame(es_tags_dim_res.fetchall())
    es_tags_df.columns = [k.upper() for k in es_tags_dim_res.keys()]

    # 3. Expression: Add Time Fields
    # Add End_Time, Incremental_Time, End_Date_Interpolated
    # Using some $$START_TIME Parameter possibly set in configuration file?

    daterange_dict = {i: (start_date + timedelta(minutes=i*MINUTES_PER_DAY)).strftime('%Y-%m-%d') for i in range(num_days)}

    timerange_dict = {i: ((start_date + timedelta(minutes=i)).time()).strftime('%H:%M:%S') for i in
                      range(MINUTES_PER_DAY)}

    ipr_timerange_dict = {
        i: {
                'START_TIME': ((start_date + timedelta(minutes=i *
                                            IPR_MIN_INTERVAL)).time()).strftime('%H:%M:%S'),
                'END_TIME': ((start_date + timedelta(minutes=((i + 1) *
                                            IPR_MIN_INTERVAL)-1)).time()).strftime('%H:%M:%S'),
                'MINUTE_INTERVAL': str(IPR_MIN_INTERVAL)
        } for i in range(IPR_MIN_RANGE)
    }

    daterange_df = pd.DataFrame.from_dict(daterange_dict, orient='index', columns=['DATE'])
    # timerange_df = pd.DataFrame.from_dict(timerange_dict, orient='index', columns=['TIME'])
    ipr_timerange_df = pd.DataFrame.from_dict(ipr_timerange_dict, orient='index',
                                              columns=['START_TIME', 'END_TIME', 'MINUTE_INTERVAL'])

    # merge date and time:
    # datetime_df = daterange_df.assign(foo=1).merge(timerange_df.assign(foo=1)).drop('foo', 1)
    # datetime_df['DATE_TIME'] = datetime_df['DATE'] + " " + datetime_df['TIME']  # GetArchiveRecord

    datetime_df = daterange_df.assign(foo=1).merge(ipr_timerange_df.assign(foo=1)).drop('foo', 1)
    datetime_df['START_DATETIME'] = datetime_df['DATE'] + " " + datetime_df['START_TIME']
    datetime_df['END_DATETIME'] = datetime_df['DATE'] + " " + datetime_df['END_TIME']

    # Will cause a Memory Error on larger inputs:
    tags_df = es_tags_df.loc[(es_tags_df['PLANT_ID'] == int(plant_id)), ['TAG_NAME']]

    # Call api with list of plants
    api_caller = partial(api_process, datetime_df, es_tags_df, plant_dir)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(api_caller, tags_df.itertuples())


def api_process(datetime_df, es_dw_df, plant_dir, tag):

    # Call api processing for input tag:
    # Create directory per TAG:
    tag_dir = plant_dir.joinpath('{TagName}'.format(TagName=tag.TAG_NAME))
    tag_dir.mkdir(exist_ok=True)

    # 5. HTTP Extract: Make respective calls to the API

    # Filter for the tag to be processed:
    tag_df = es_dw_df.loc[(es_dw_df['TAG_NAME'] == tag.TAG_NAME)]

    # Merge tag and datetime run dfs:
    tag_dt_df = tag_df.assign(foo=1).merge(datetime_df.assign(foo=1)).drop('foo', 1)

    # Change to get directly from schema reflect?
    scada_stg1_header = ("PLANT_ID", "PLANT_CODE", "TAG_TYPE", "TAG_NAME", "VALUE", "TIMESTAMPLOCAL",
                         "TIMESTAMPUTC", "CREATE_DATE", "UPDATE_DATE", "CREATED_BY", "UPDATED_BY")

    for rec in tag_dt_df.itertuples():

        # Get start - end time values for logging and data output:
        starttime_dt = datetime.strptime(rec.START_DATETIME, '%Y-%m-%d %H:%M:%S')
        starttime_str = starttime_dt.strftime("%Y%m%d_%H%M")

        endtime_dt = datetime.strptime(rec.END_DATETIME, '%Y-%m-%d %H:%M:%S')
        endtime_str = endtime_dt.strftime("%Y%m%d_%H%M")

        logger = createLogger(str('{TagName}-{MinDateTime}-{MaxDateTime}'.format(TagName=rec.TAG_NAME,
                                                                        MinDateTime=starttime_str,
                                                                        MaxDateTime=endtime_str)),
                     tag_dir,
                     str('{MinDateTime}-{MaxDateTime}'.format(MinDateTime=starttime_str,
                                                                       MaxDateTime=endtime_str)))

        logger.info("START - Pulling data for TAG: " + str(tag.TAG_NAME))

        log_line =  "SERVER: " + str(rec.SERVER_NAME) + ", TAGNAME: " + str(rec.TAG_NAME) + ", START: " + str(rec.START_DATETIME) + " to END: " + str(rec.END_DATETIME)

        u = GetInterpolatedValues(rec.SERVER_NAME, rec.TAG_NAME, rec.START_DATETIME,
                                  rec.END_DATETIME, str(rec.MINUTE_INTERVAL))

        try:
            pi_results = u.post()

            logger.info("SUCCESS - PULLED DATA FOR " + log_line)

        except:
            logger.error("ERROR - FAILED TO PULL DATA FOR " + log_line )

        create_date = datetime.now()


        # data is a list of tuples
        scada_stg1_records = [(rec.PLANT_ID,
                               rec.PLANT_CODE,
                               rec.TAG_TYPE,
                               rec.TAG_NAME,
                               str(pi_val.value.text),
                               str(pi_val['time']),
                               None,
                               create_date,
                               create_date,
                               'OXR0MQY',
                               'OXR0MQY') for pi_val in pi_results.findAll('values')]

        # Count should be about the same as # minutes in interval
        data_pull_size = len(scada_stg1_records)

        """
        if data_pull_size >= (int(rec.MINUTE_INTERVAL) * 0.9):
            logger.info("INFO - Pulled more than 90% of data")
        else:
            logger.warning("INFO - Pulled less than 90% of data")
        """

        # INSERT INTO TABLE es_scada_stg1 #
        # es_ods_trans = es_ods_conn.begin()
        try:
            # Write records to db:
            # scada_stg1_ins_resp = es_ods_conn.execute(scada_stg1_table.insert(), scada_stg1_records)

            # Write the list of tuples to a csv file:
            curr_dir = pathlib.Path.cwd()
            data_dir = curr_dir.joinpath('data')
            data_dir.mkdir(exist_ok=True)

            data_filename = '{PlantID}_{PlantCode}_{TagName}_{StartTime}-{EndTime}.csv'.format(
                                        PlantID=rec.PLANT_ID,
                                        PlantCode=rec.PLANT_CODE,
                                        TagName=rec.TAG_NAME,
                                        StartTime=starttime_str,
                                        EndTime=endtime_str)

            output_file = data_dir.joinpath(data_filename)

            # with output_file.open('wb') as wf:
                # Pickling: pickle.dump(scada_stg1_records, wf)

            # Csv Output files:
            with open(output_file, 'w') as wf:
                csvwriter = csv.writer(wf)
                csvwriter.writerow(scada_stg1_header)
                csvwriter.writerows(scada_stg1_records)

            scada_stg1_records = []

        except:
            logger.error('ERROR - FAILED TO WRITE DATA FOR ' + log_line)
            # es_ods_trans.rollback()
            raise

    # BEGIN STG2 Pivot of data?



def main(argv):

    start_time = time.time()

    parser = argparse.ArgumentParser(description='Run ESBI ETL process: ')
    # What plants to process:
    parser.add_argument('-pids', '--plant_ids', type=int, nargs='+', default=[])
    # day to start process from:
    parser.add_argument('-sd', '--start_date', required=True, type=str, default=(datetime.now() -
                                                                                 timedelta(1)).strftime('%Y%m%d'))
    # XOR(end_date, interval)
    end_group = parser.add_mutually_exclusive_group()
    # day to end process on:
    end_group.add_argument('-ed', '--end_date', type=str, default=datetime.now().strftime('%Y%m%d'))
    # number of days to pull:
    end_group.add_argument('-nd', '--num_days', type=int, default=0)

    # number of minutes worth of data to pull per batch call:
    parser.add_argument('-mi', '--min_int', required=False, type=int, default=60)

    args = parser.parse_args()

    # Only tuple not list works in sqlalchemy-core in clause:
    input_plant_ids = get_plants(tuple(args.plant_ids))

    start_date_str = args.start_date
    end_date_str = args.end_date
    minute_interval = args.min_int

    start_date = datetime.strptime(start_date_str, '%Y%m%d')

    if args.num_days == 0:
        end_date = datetime.strptime(end_date_str, '%Y%m%d')
        interval = abs(int((end_date - start_date).days))
    else:
        interval = args.num_days
        end_date = start_date + timedelta(interval)

    print('INPUT PLANTS: ' + str(input_plant_ids))
    print('START DATE: ' + str(start_date_str))
    print('END DATE: ' + str(end_date_str))
    print('NUM DAYS: ' + str(interval))
    print('MINUTE INTERVAL: ' + str(minute_interval))

    # Create the log_dir:
    curr_dir = pathlib.Path.cwd()
    log_dir = curr_dir.joinpath('logs')
    log_dir.mkdir(exist_ok=True)

    printer_lock = multiprocessing.Lock()

    # Call api with list of plants
    api_caller = partial(plant_thread_gen, start_date, end_date, log_dir, minute_interval)

    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        # Call API pull per plant id:
        pool.map(api_caller, input_plant_ids)

    duration = time.time() - start_time
    print(f"Completed in {duration} seconds")


if __name__ == "__main__":
    main(sys.argv[1:])
