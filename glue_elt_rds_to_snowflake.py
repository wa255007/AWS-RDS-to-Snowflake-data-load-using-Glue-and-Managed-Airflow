import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
from pyspark.sql import functions as F
import pyspark
from pyspark import SparkConf, SparkContext
from urllib.parse import urlparse
from io import StringIO, BytesIO
import boto3
import json
import logging
from datetime import datetime
import pytz
import os
from typing import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME", "table_name","config_s3_file_path"
         ],
)

table_name=args["table_name"]
job_run_id = args['JOB_RUN_ID']
s3_config_file_path=args["config_s3_file_path"]
out = urlparse (s3_config_file_path)
print (out)
bucket=out.netloc
key =out.path.lstrip('/')
datasource_config = s3_client.get_object(
    Bucket=bucket,
    Key=key)
config = json.loads(datasource_config['Body'].read().decode('utf-8'))

print(
    f"[GLUE LOG] - arguments received are : {args}"
)

confa = (
    pyspark.SparkConf()
 )

sc = SparkContext(conf=confa)
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
print(f"[GLUE LOG] - args recieved are  {args}")
spark = SparkSession.builder.appName("RDS_SNOWFLAKE_LOADER").getOrCreate()
df = spark.read.format("jdbc").\
options(
         url= f"jdbc:postgresql://{config['rds_url']}/{config['rds_database']}",
         dbtable=table_name,
         user= config['rds_username'],
         password=config['rds_password'],
         driver='org.postgresql.Driver').\
load()
df.show()
df.printSchema()
print("successfully connected TYRING TO WRITE IT BACK")
sfOptions = {
  "sfURL" : config['snow_account'],
  "sfUser" : config['snow_user'],
  "sfPassword" : config['snow_password'],
  "sfDatabase" : config['snow_db'],
  "sfSchema" : config['snow_schema'],
  "sfWarehouse" : config['warehouse']
}
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", f"glue_{table_name}").mode("overwrite").save()
print("successfully connected and written all records", df.count())
job.commit()
