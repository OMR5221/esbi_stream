from ..api.interpolated_results import GetInterpolatedValues
from ..helpers.logger import Logger

from sqlalchemy import (create_engine, MetaData, Table, Column, ForeignKey, select, func, Integer, String, DateTime)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import (sessionmaker, Session, scoped_session)
import cx_Oracle
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
from functools import partial
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import pathlib
import csv


class STG1Worker(object):

    def __init__(self, start_date, end_date, min_interval, base_dir):

        print("Created WORKER")
        self.id = 1
        self.start_date = start_date
        self.end_date = end_date
        self.min_interval = min_interval
        self.completed = False
        self.run_duration = 0
        self.log_dir = base_dir

    def run_successful(self):
        return self.completed

    def log_start_time():
        pass

    def log_end_time():
        pass

    def log_duration():
        pass

    def logger_details():
        pass

    def processed_plants():
        pass
        # es_ods_trans = es_ods_conn.begin()

    def processed_invs():
        pass

    def processed_racks():
        pass

    # Get plant specific data and generate threds per tags:
    def plant_thread_gen(self, plant_id):

        print("STARTING PLANT_THREAD_GEN for {Plant_ID}".format(Plant_ID= plant_id))
        MINUTES_PER_DAY = 1440  # 60 * 24 = # of minutes in a dayV
        INTERVAL = 1  # minute level data
        MINUTES_PER_HOUR = 60

        # Create new log directory for the plant:
        plant_dir = self.log_dir.joinpath('{PlantID}'.format(PlantID=plant_id))
        plant_dir.mkdir(exist_ok=True)

        num_days = (self.end_date - self.start_date).days + 1

        # Define configuration settings that can be used to control the queries that call the process:
        IPR_MIN_INTERVAL = self.min_interval  # number of minutes per api pull
        IPR_MIN_RANGE = int(MINUTES_PER_DAY / IPR_MIN_INTERVAL)

        es_dw_engine = create_engine('oracle://SCHEMA:password@DB')
        es_dw_conn = es_dw_engine.connect()

        es_ods_engine = create_engine('oracle://SCHEMA:password@DB')
        es_ods_conn = es_ods_engine.connect()

        plant_tag_query =   """
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

        daterange_dict = {i: (self.start_date + timedelta(minutes=i*MINUTES_PER_DAY)).strftime('%Y-%m-%d') for i in range(num_days)}

        timerange_dict = {i: ((self.start_date + timedelta(minutes=i)).time()).strftime('%H:%M:%S') for i in
                          range(MINUTES_PER_DAY)}

        ipr_timerange_dict = {
            i: {
                    'START_TIME': ((self.start_date + timedelta(minutes=i *
                                                IPR_MIN_INTERVAL)).time()).strftime('%H:%M:%S'),
                    'END_TIME': ((self.start_date + timedelta(minutes=((i + 1) *
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

        print("CREATING THREADS:")

        # Call api with list of plants
        api_caller = partial(self.api_process, datetime_df, es_tags_df, plant_dir)


        es_ods_conn = cx_Oracle.connect('SCHEMA_NAME/password@DB', threaded=True)
        es_ods_cursor = es_ods_conn.cursor()

        es_ods_cursor.prepare("""INSERT INTO ES_ODS_OWNER.ES_SCADA_STG1(PLANT_ID, PLANT_CODE,
                              TAGNAME, VALUE, TIMESTAMPLOCAL, TIMESTAMPUTC, CREATE_DATE,
                              UPDATE_DATE, CREATED_BY, UPDATED_BY) VALUES
                              (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)""")

        tags = [tag for tag in tags_df.itertuples()]

        with ThreadPoolExecutor(max_workers=5) as executor:

            futures = [executor.submit(self.api_process, datetime_df, es_tags_df, plant_dir, tag)
                       for tag in tags]

            for future in as_completed(futures):
                results = future.result()
                for data in results:
                    es_ods_cursor.executemany(None, data)
                    es_ods_conn.commit()

            """
            # Start the load operations and mark each future with its URL
            futures = {executor.submit(self.api_process, datetime_df, es_tags_df, plant_dir, tag):
                                tag for tag in tags_df.itertuples()}

            for future in concurrent.futures.as_completed(futures):
                api_call = futures[future]
                try:
                    api_data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (api_call, exc))
                else:
                    print('%r page is %d bytes' % (api_call, len(api_data)))
            """
            # executor.map(api_caller, tags_df.itertuples())
            # executor.close()
            # executor.join()
            # executor.shutdown(wait=True)

        es_ods_cursor.close()
        es_ods_conn.close()


    def api_process(self, datetime_df, es_dw_df, plant_dir, tag):

        all_recs = []

        es_ods_conn = cx_Oracle.connect('SCHEMA_NAME/password@DB', threaded=True)
        # es_ods_conn.autocommit = True

        es_ods_cursor = es_ods_conn.cursor()

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

            logger = Logger.createLogger(str('{TagName}-{MinDateTime}-{MaxDateTime}'.format(TagName=rec.TAG_NAME,
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
                logger.error("ERROR - FAILED TO PULL DATA FOR " + log_line)

            create_date = datetime.date(datetime.now())

            # data is a list of tuples
            # dict(ZIP(stg1_headers, rec))
            scada_stg1_records = [(int(rec.PLANT_ID),
                                   str(rec.PLANT_CODE),
                                   str(rec.TAG_NAME),
                                   str(pi_val.value.text),
                                   datetime.strptime(str(pi_val['time']), '%Y-%m-%dT%H:%M:%S'),
                                   None,
                                   create_date,
                                   create_date,
                                   'OXR0MQY',
                                   'OXR0MQY') for pi_val in pi_results.findAll('values')]

            """
            scada_stg1_records = [(int(rec.PLANT_ID),
                                   str(rec.PLANT_CODE),
                                   rec.TAG_NAME,
                                   str(pi_val.value.text),
                                   datetime.strptime(str(pi_val['time'])),
                                   None,
                                   create_date,
                                   create_date,
                                   'OXR0MQY',
                                   'OXR0MQY') for pi_val in pi_results.findAll('values')]

            """

            # Count should be about the same as # minutes in interval
            data_pull_size = len(scada_stg1_records)

            # Check amount of data being processed:
            if data_pull_size >= (int(rec.MINUTE_INTERVAL) * 0.9):
                logger.info("INFO - Pulled more than 90% of data")
            else:
                logger.warning("INFO - Pulled less than 90% of data")

            # INSERT INTO TABLE es_scada_stg1:
            es_ods_cursor.prepare("""INSERT INTO ES_ODS_OWNER.ES_SCADA_STG1(PLANT_ID, PLANT_CODE, TAGNAME, VALUE, TIMESTAMPLOCAL, TIMESTAMPUTC,CREATE_DATE,UPDATE_DATE, CREATED_BY, UPDATED_BY) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)""")

            try:
                # Write records to db:
                # es_ods_cursor.executemany(None, scada_stg1_records)

                # es_ods_conn.commit()

                # es_ods_cursor.close()
                # es_ods_conn.close()

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

                all_recs.append(scada_stg1_records)

                scada_stg1_records = []

            except:
                logger.error('ERROR - FAILED TO WRITE DATA FOR ' + log_line)
                # es_ods_trans.rollback()
                raise

        return all_recs
