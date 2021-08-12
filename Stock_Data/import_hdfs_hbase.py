"""
@Author: Ranjith G C
@Date: 2021-08-12
@Last Modified by: Ranjitth G C
@Last Modified time: 2021-08-12
@Title : Program Aim is to import real time stock data into hbase table using happybase.
"""

import happybase as hb
from happybase import connection
from loggers import logger
import csv
import requests
import os
import time

from dotenv import load_dotenv
load_dotenv('.env')
key = os.getenv('API_KEY')
# Start thrift server first: hbase-daemon.sh start thrift

class Hbase:
    def __init__(self):
        """
        Description: 
            created constructor for creating connection to hbase.
        """
        self.create_connection()

    def create_connection(self):
        """
        Description:
            This function is used for creating connection with hbase.
        """
        try:
            conn = hb.Connection()
            conn.open()
            self.conn = conn
        except Exception as e:
            logger.error(e)

    def create_table(self):
        """
        Description:
            This function is used for creating hbase table
        """
        try:
            connection = self.conn
            connection.create_table('stock',{'cf1': dict(max_versions=1),'cf2': dict(max_versions=1),'cf3': dict(max_versions=1),'cf4': dict(max_versions=1),'cf5': dict(max_versions=1)})
            logger.info("table created successfully")
       
        except Exception as e: 
            logger.error(e)
            connection.close()

    def put_csv_data_into_hbase(self):
        """
        Description:
            This function is used for putting csv data into hbase table
        """
        try:
            connection = self.conn
            table = connection.table('stock')
            CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=15min&slice=year1month1&apikey= key'

            with requests.Session() as s:
                download = s.get(CSV_URL)
                decoded_content = download.content.decode('utf-8')
                cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                next(cr)

                my_list = list(cr)
                for row in my_list:
                    table.put(row[0],
                    {'cf1:Open': row[1],
                    'cf2:High': row[2],
                    'cf3:Low': row[3],
                    'cf4:Close': row[4],
                    'cf5:Volume': row[5]})      
        except Exception as e: 
            logger.error(e)
            connection.close()

    def display_table(self):
        """
        Description:
            This function is used for displaying data from hbase table.
        """
        try:
            connection = self.conn
            table = connection.table('stock')
            for key,data in table.scan():
                id = key.decode('utf-8')
                for value1,value2 in data.items():
                    cf1 = value1.decode('utf-8')
                    cf2 = value2.decode('utf-8')
                    print(id,cf1,cf2) 

        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    hbase = Hbase()
    hbase.create_table()
    hbase.put_csv_data_into_hbase()
    hbase.display_table()