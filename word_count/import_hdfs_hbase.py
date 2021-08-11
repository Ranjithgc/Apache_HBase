"""
@Author: Ranjith G C
@Date: 2021-08-11
@Last Modified by: Ranjitth G C
@Last Modified time: 2021-08-12
@Title : Program Aim is to import csv file into hbase table using happybase.
"""

import happybase as hb
from happybase import connection
from loggers import logger
import csv
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
            connection.create_table('word_count',{'cf1': dict(max_versions=1),'cf2': dict(max_versions=1)})
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
            table = connection.table('word_count')
            input = csv.DictReader(open("/home/ubunta/Desktop/HBase/word_count/output"))
            for row in input:
                table.put(row['id'],
            {'cf1:word': row['word'],
            'cf2:Count': row['count']})       
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
            table = connection.table('word_count')
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