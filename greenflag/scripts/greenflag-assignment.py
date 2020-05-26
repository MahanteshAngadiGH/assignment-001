#!/usr/bin/env python
# coding: utf-8

from config import config
import common_utils as c_utils
import db_utils as db_utils

from datetime import datetime

import pandas as pd
import csv

import logging

import glob

import pyarrow as pa
import pyarrow.parquet as pq

import sys

import os
from os import path
from pathlib import Path


# function data_process
def  data_process(logger):
    try:
        status = convert_csv_to_parquet(logger)
        msg = "Status (convert_csv_to_parquet) ... : {}".format(status)
        print(msg)
        logger.info(msg)

        if status["Status"] == "Success":
            parquet_df = process_parquet(logger)
            status = results(parquet_df, logger)
            status = load_to_database(logger, parquet_df)


    except Exception as e:
        status = {"Status": "Failed"}

        msg = "The data process execution failed with error : {}".format(e.args[0])
        print(msg)
        logger.error(msg)
        raise

    finally:
        return status


# function convert_csv_to_parquet
def  convert_csv_to_parquet(logger):
    try:
        msg = "\n"
        print(msg)
        logger.info(msg)

        raw_file_path = config["raw_file_path"]

        msg = "Raw Data Files Path ... : {}".format(raw_file_path)
        print(msg)
        logger.info(msg)

        item_exists = str(path.exists(raw_file_path))

        if item_exists == "False":
            msg = "The given/configured Path {} does not exist ... : {}".format(raw_file_path, item_exists)
            print(msg)
            logger.info(msg)

            status = {"Status": "Failed"}
            return status
        else:
            raw_files_pattern = config["raw_files_pattern"]

            msg = "Raw Data File Names Pattern ... : {}".format(raw_files_pattern)
            print(msg)
            logger.info(msg)
            
            files_to_process = glob.glob(raw_file_path + raw_files_pattern)

            msg = "Files to Process ... : {}".format(files_to_process)
            print(msg)
            logger.info(msg)
    
            if not files_to_process:
                msg = "There are no source files to process in the given path ... : {}".format(raw_file_path)
                print(msg)
                logger.info(msg)

                status = {"Status": "Failed"}
                return status

            else:

                parquet_file_path = config["parquet_file_path"]

                msg = "Parquet Data Files Path ... : {}".format(parquet_file_path)
                print(msg)
                logger.info(msg)

                item_exists = str(path.exists(parquet_file_path))

                if item_exists == "False":
                    msg = "The given/configured Stage (for Parquet files) Path {} does not exist ... : {}".format(parquet_file_path, item_exists)
                    print(msg)
                    logger.info(msg)

                    status = {"Status": "Failed"}
                    return status
                else:

                    # write csv files to parquet format
                    li = []
                    idx = 1
                    for filename in files_to_process:
                        msg = "\nProcessing the File Number - {}".format(idx)
                        print(msg)
                        logger.info(msg)
            
                        msg = "File Name ... : {}".format(filename)
                        print(msg)
                        logger.info(msg)
            
                        # Create a Dataframe on data in a file
                        df =  pd.read_csv(filename, index_col=None, header=0)
            
                        # Convert Dataframe to Apache Arrow Table
                        table = pa.Table.from_pandas(df)
            
                        # Get/Derive the Parquet file
                        parquet_file = filename.lower().replace("raw", "parquet").replace("csv", "parquet")
    
                        msg = "Parquet File Name ... : {}".format(parquet_file)
                        print(msg)
                        logger.info(msg)
            
                        # Write Dataframe to Parquet file with GZIP
                        msg = "Write to Parquet File Name ... : {}".format(parquet_file)
                        print(msg)
                        logger.info(msg)

                        pq.write_table(table, parquet_file, compression="GZIP")
            
                        idx = idx + 1
            
                    status = {"Status": "Success"}
                
                    return status

    except Exception as e:
        status = {"Status": "Failed"}

        msg = "The csv to parquet conversion failed with error : {}".format(e.args[0])
        print(msg)
        logger.error(msg)
        raise


# function process_parquet
def  process_parquet(logger):
    try:
        msg = "\n"
        print(msg)
        logger.info(msg)

        parquet_file_path = config["parquet_file_path"]

        msg = "Parquet Data Files Path ... : {}".format(parquet_file_path)
        print(msg)
        logger.info(msg)
        
        parquet_files_pattern = config["parquet_files_pattern"]

        msg = "Parquet Data File Names Pattern ... : {}".format(parquet_files_pattern)
        print(msg)
        logger.info(msg)
        
        parquet_files_to_process = glob.glob(parquet_file_path + parquet_files_pattern)

        msg = "File Name ... : {}".format(parquet_files_to_process)
        print(msg)
        logger.info(msg)
        
        li = []
        
        for filename in parquet_files_to_process:
            p_df = pd.read_parquet(filename)
            li.append(p_df)
        
        parquet_df = pd.concat(li, axis=0, ignore_index=True, sort=False)

        return parquet_df

    except Exception as e:
        status = {"Status": "Failed"}

        msg = "The parquet file processing failed with error : {}".format(e.args[0])
        print(msg)
        logger.error(msg)

        raise


# function results
def results(parquet_df, logger):
    msg = "\n\nPriniting/Logging the results ... : "
    print(msg)
    logger.info(msg)

    try:
        # get the Maximum Temperature value
        max_temp = parquet_df.loc[parquet_df['ScreenTemperature'].idxmax()].loc['ScreenTemperature']

        msg = "The maximum temperature is ... : {}".format(max_temp)
        print(msg)
        logger.info(msg)

        
        # get the row with Maximum Temperature value
        max_temp_date = parquet_df.loc[parquet_df['ScreenTemperature'].idxmax()].loc['ObservationDate']

        msg = "The day with maximum temperature is ... : {}".format(max_temp_date)
        print(msg)
        logger.info(msg)
        
        # get the row with Maximum Temperature value
        max_temp_region = parquet_df.loc[parquet_df['ScreenTemperature'].idxmax()].loc['Region']

        msg = "The region with maximum temperature is ... : {}".format(max_temp_region)
        print(msg)
        logger.info(msg)

        status = {'Status': 'Success'}

        return status

    except Exception as e:
        status = {'Status': 'Failed'}

        msg = "The results display/logging failed with error:{}".format(e.args[0])
        print(msg)
        logger.error(msg)

        raise


def load_to_database(logger, parquet_df):
    msg = "\n"
    print(msg)
    logger.error(msg)

    try:
        database = config["db_file"]

        msg = "Creating a database for Weather data ... : {}".format(database)
        print(msg)
        logger.info(msg)

        # create a database connection
        conn = db_utils.create_connection(database)

        msg = "SQL Lite Database Connection Object ... : {}".format(conn)
        print(msg)
        logger.info(msg)

        # create tables
        if conn is None:
            msg = "Error! cannot create the database connection."
            print(msg)
            logger.info(msg)
        else:
            msg = "Loading the data into the weather table from a dataframe."
            print(msg)
            logger.info(msg)

            parquet_df.to_sql("weather", conn, if_exists="replace")

    except Exception as e:
        status = {'Status': 'Failed'}

        msg = "The results display/logging failed with error : {}".format(e.args[0])
        print(msg)
        logger.error(msg)

        raise

    finally:
        if conn:
            conn.close()

            msg = "Finally close the database connection"
            print(msg)
            logger.info(msg)


# The main() function
def main():
    # Calling data process activity
    job_name = config["greenflag-data-process-job"]
    logger = c_utils.get_logger(job_name)

    msg = ">>  ==========  Data Processing Job - {} started ... : {}".format(job_name, datetime.now())
    print(msg)
    logger.info(msg)

    status = data_process(logger)

    msg = "\nThe Final Status of the job - {}  execution is ... : {}".format(job_name, status)
    print(msg)
    logger.info(msg)

    msg = ">>  ==========  Data Processing Job - {} finished ... : {}".format(job_name, datetime.now())
    print(msg)
    logger.info(msg)


if __name__ == '__main__':
    main()
