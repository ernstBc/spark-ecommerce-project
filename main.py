# ETL Process 
# Extract Data from Kaggle and put it in local machine or postgres
from src.components.extract import Extract
from src.components.transform import Transform
from src.components.load import Load
from pyspark.sql import SparkSession
import argparse


parser = argparse.ArgumentParser(description='A ETL process')


parser.add_argument('--sparkjars',help="PostgreSQL JDBC driver JAR filepath")
parser.add_argument('--mode', default='local',help="The mode to save the data [local,postgres]")
parser.add_argument('--port', type=int, default=8000, help='The port number for the server.')
parser.add_argument('--host', default='localhost', help='The host address for the server.')

if __name__ =='__main__':
    args = parser.parse_args()

    spark = SparkSession.builder.appName('oblist').getOrCreate()


    e = Extract()
    t = Transform(spark)
    l = Load(spark)


    # init process
    
    # extract / download data
    e.extract()
    # clean, join and trasnform data 
    transformed_data = t.transform()
    # save dataframe in parquet local
    l.load(transformed_data, 'ostia')

    spark.stop()