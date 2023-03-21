import datetime
import logging
import os
import configparser
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, udf, count, mean, round, array_distinct, flatten, col, row_number
from pyspark.sql.types import StructType, StringType, BooleanType, LongType, DoubleType, IntegerType
from pyspark.sql.window import Window

import boto3
import botocore

from botocore.errorfactory import ClientError

s3 = boto3.resource('s3')
client = boto3.client('s3')
config = configparser.ConfigParser()

sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Instantiate logger
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))

# Take Current Date as input
mst_time = datetime.now() - timedelta(hours=6)
logger.info('Current Time:{}'.format(mst_time))

today_cond = mst_time.strftime('%Y-%m-%d')
today = datetime.strptime(today_cond, '%Y-%m-%d').date()
date_N_days_ago = today - timedelta(days=6)
logger.info('N Days Ago Date:{}'.format(date_N_days_ago))

#Function: Get all the incremental Paths using paginator
def get_paths(path):
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=path)
    paths = []
    for page in pages:
        for o in page['Contents']:
            # print(obj['Size'])
            if ((o["LastModified"].date() >= date_N_days_ago) & (o["LastModified"].date() < today) ):
                file='s3a://{}/{}'.format(bucket,o["Key"])
                paths.append(file)
    return paths

# Function: Schema Creation
def read_schema(schema_arg):
    d_types={
    "StringType()":StringType(),
    "LongType()":LongType(),
    "DoubleType()":DoubleType(),    
    "BooleanType()":BooleanType()    
    }  
    split_values = schema_arg.split(",")
    sch = StructType()
    for word in split_values:
        x = word.split(":")
        sch.add(x[0],d_types[x[1]], True)
    return sch

#Function: Get Length of the list of Row
slen = udf(lambda s: len(s), IntegerType())

#Get the Variables from config.ini file
BUCKET = 'taas-test-artifacts'
FILE_TO_READ = 'Auto_Severity_Data/Config/config.ini'

obj = client.get_object(Bucket=BUCKET, Key=FILE_TO_READ) 
config.read_string(obj['Body'].read().decode())

#Variables
bucket = config.get('variables','bucket')

#Paths
exe_path=config.get('paths','exe_path')
ext_path=config.get('paths','ext_path')
out_path=config.get('paths','out_path')

#Schema
ext_schemaConf = config.get('schema','ext_schema')
exe_schemaConf = config.get('schema','exe_schema')

ext_schema=read_schema(ext_schemaConf)
exe_schema=read_schema(exe_schemaConf)

# Setup file paths
s3_file_path_source_exe = get_paths(exe_path)
s3_file_path_source_ext = get_paths(ext_path)

out_path="Auto_Severity_Data/Weekly_Test_Cases_Summary_Data/"

s3_file_path_target = 's3a://{}/{}'.format(bucket,out_path)

if 's3a://{}/{}'.format(bucket,exe_path) in s3_file_path_source_exe: s3_file_path_source_exe.remove('s3a://{}/{}'.format(bucket,exe_path))
if 's3a://{}/{}'.format(bucket,ext_path) in s3_file_path_source_ext: s3_file_path_source_ext.remove('s3a://{}/{}'.format(bucket,ext_path))

if len(s3_file_path_source_exe)>0:
     source_data_exe_df = spark.read.schema(exe_schema).json(path=s3_file_path_source_exe, multiLine=True).persist()
else:
    exit()
    
if len(s3_file_path_source_ext)>0:
    source_data_ext_df = spark.read.schema(ext_schema).json(path=s3_file_path_source_ext, multiLine=True).persist()
else:
    exit()

# Rename columns
source_data_ext_df = source_data_ext_df.withColumnRenamed('Landslide_validation_criteria_0.str', 'criteria_00') \
    .withColumnRenamed('Landslide_validation_status_0.str', 'criteria_status_00') \
    .withColumnRenamed('Landslide_validation_criteria_1.str', 'criteria_01') \
    .withColumnRenamed('Landslide_validation_status_1.str', 'criteria_status_01') \
    .withColumnRenamed('Landslide_validation_criteria_2.str', 'criteria_02') \
    .withColumnRenamed('Landslide_validation_status_2.str', 'criteria_status_02') \
    .withColumnRenamed('Landslide_validation_criteria_3.str', 'criteria_03') \
    .withColumnRenamed('Landslide_validation_status_3.str', 'criteria_status_03') \
    .withColumnRenamed('Landslide_validation_criteria_4.str', 'criteria_04') \
    .withColumnRenamed('Landslide_validation_status_4.str', 'criteria_status_04') \
    .withColumnRenamed('Landslide_validation_criteria_5.str', 'criteria_05') \
    .withColumnRenamed('Landslide_validation_status_5.str', 'criteria_status_05') \
    .withColumnRenamed('Landslide_validation_criteria_6.str', 'criteria_06') \
    .withColumnRenamed('Landslide_validation_status_6.str', 'criteria_status_06') \
    .withColumnRenamed('Landslide_validation_criteria_7.str', 'criteria_07') \
    .withColumnRenamed('Landslide_validation_status_7.str', 'criteria_status_07') \
    .withColumnRenamed('Landslide_validation_criteria_8.str', 'criteria_08') \
    .withColumnRenamed('Landslide_validation_status_8.str', 'criteria_status_08') \
    .withColumnRenamed('Landslide_validation_criteria_9.str', 'criteria_09') \
    .withColumnRenamed('Landslide_validation_status_9.str', 'criteria_status_09') \
    .withColumnRenamed('Landslide_validation_criteria_10.str', 'criteria_10') \
    .withColumnRenamed('Landslide_validation_status_10.str', 'criteria_status_10') \
    .withColumnRenamed('Landslide_validation_criteria_11.str', 'criteria_11') \
    .withColumnRenamed('Landslide_validation_status_11.str', 'criteria_status_11') \
    .withColumnRenamed('Landslide_validation_criteria_12.str', 'criteria_12') \
    .withColumnRenamed('Landslide_validation_status_12.str', 'criteria_status_12') \
    .withColumnRenamed('Landslide_validation_criteria_13.str', 'criteria_13') \
    .withColumnRenamed('Landslide_validation_status_13.str', 'criteria_status_13') \
    .withColumnRenamed('Landslide_validation_criteria_14.str', 'criteria_14') \
    .withColumnRenamed('Landslide_validation_status_14.str', 'criteria_status_14') \
    .withColumnRenamed('Landslide_validation_criteria_15.str', 'criteria_15') \
    .withColumnRenamed('Landslide_validation_status_15.str', 'criteria_status_15') \
    .withColumnRenamed('Landslide_validation_criteria_16.str', 'criteria_16') \
    .withColumnRenamed('Landslide_validation_status_16.str', 'criteria_status_16') \
    .withColumnRenamed('Landslide_validation_criteria_17.str', 'criteria_17') \
    .withColumnRenamed('Landslide_validation_status_17.str', 'criteria_status_17') \
    .withColumnRenamed('Landslide_cause_code_key_1.str', 'cause_code_key_1') \
    .withColumnRenamed('Landslide_cause_code_count_1.num', 'cause_code_count_1') \
    .withColumnRenamed('Landslide_cause_code_key_2.str', 'cause_code_key_2') \
    .withColumnRenamed('Landslide_cause_code_count_2.num', 'cause_code_count_2') \
    .withColumnRenamed('Landslide_cause_code_key_3.str', 'cause_code_key_3') \
    .withColumnRenamed('Landslide_cause_code_count_3.num', 'cause_code_count_3') \
    .withColumnRenamed('Landslide_cause_code_key_4.str', 'cause_code_key_4') \
    .withColumnRenamed('Landslide_cause_code_count_4.num', 'cause_code_count_4') \
    .withColumnRenamed('Landslide_cause_code_key_5.str', 'cause_code_key_5') \
    .withColumnRenamed('Landslide_cause_code_count_5.num', 'cause_code_count_5') \
    .withColumnRenamed('Landslide_cause_code_key_6.str', 'cause_code_key_6') \
    .withColumnRenamed('Landslide_cause_code_count_6.num', 'cause_code_count_6') \
    .withColumnRenamed('Test_Case_Name.str', 'test_case_name') \
    .withColumn('env',source_data_ext_df.topologyName.substr(1,11))

source_data_exe_df=source_data_exe_df.withColumnRenamed('result', 'test_result') \
                    .withColumnRenamed('executionID', 'execution_id')
                    
source_data_exe_df.createOrReplaceTempView("source_data_exe_df")
source_data_ext_df.createOrReplaceTempView("source_data_ext_df")

combined_exe_ext_df=spark.sql("""
        SELECT 
        test_case_name,
        env,
        test_result,
        criteria_status_00,
        criteria_status_01,
        criteria_status_02,
        criteria_status_03,
        criteria_status_04,
        criteria_status_05,
        criteria_status_06,
        criteria_status_07,
        criteria_status_08,
        criteria_status_09,
        criteria_status_10,
        criteria_status_11,
        criteria_status_12,
        criteria_status_13,
        criteria_status_14,
        criteria_status_15,
        criteria_status_16,
        criteria_status_17,
        cause_code_key_1,
        cause_code_count_1,
        cause_code_key_2,
        cause_code_count_2,
        cause_code_key_3,
        cause_code_count_3,
        cause_code_key_4,
        cause_code_count_4,
        cause_code_key_5,
        cause_code_count_5,
        cause_code_key_6,
        cause_code_count_6
FROM source_data_exe_df se
INNER JOIN source_data_ext_df sx ON se.execution_id=sx.reportID 
""").persist()

unique_test_case_criteria=spark.sql("""
        SELECT DISTINCT
        test_case_name AS unique_test_case,
        env,
        criteria_00,
        criteria_01,
        criteria_02,
        criteria_03,
        criteria_04,
        criteria_05,
        criteria_06,
        criteria_07,
        criteria_08,
        criteria_09,
        criteria_10,
        criteria_11,
        criteria_12,
        criteria_13,
        criteria_14,
        criteria_15,
        criteria_16,
        criteria_17
FROM source_data_ext_df 
""").persist()

unique_test_case_criteria = unique_test_case_criteria.distinct().persist()
unique_test_case_criteria=unique_test_case_criteria.select('unique_test_case',col('env').alias('unique_env'),sum([F.isnull(unique_test_case_criteria[col]).cast(IntegerType()) for col in unique_test_case_criteria.columns]).alias("CRITERIA_COUNT"))
unique_test_case_criteria=unique_test_case_criteria.select('unique_test_case','unique_env',(18-unique_test_case_criteria.CRITERIA_COUNT).alias('CRITERIA_COUNT'))

uniqueTC = Window.partitionBy("unique_test_case","unique_env").orderBy(col("CRITERIA_COUNT").desc())
unique_test_case_criteria=unique_test_case_criteria.withColumn("row",row_number().over(uniqueTC)) \
  .filter(col("row") == 1).drop("row")
  
unique_test_case_criteria=unique_test_case_criteria.filter('unique_test_case IS NOT NULL')

count_exe_ext_df=combined_exe_ext_df.select('test_case_name','env','test_result',lit(1).cast("int").alias("record_count"))

count_exe_ext_df=count_exe_ext_df.withColumnRenamed('test_case_name', 'TEST_CASE_KEY') \
                .groupBy('TEST_CASE_KEY','env','test_result').agg(count('record_count').alias("TOTAL_RUNS"))
                
count_exe_ext_df=count_exe_ext_df.groupBy("TEST_CASE_KEY","env").pivot("test_result",["PASS","INDETERMINATE","FAIL"]).sum("TOTAL_RUNS") 

if 'FAIL' not in count_exe_ext_df.columns:
   count_exe_ext_df = count_exe_ext_df.withColumn('FAIL', F.lit(0.0))
if 'PASS' not in count_exe_ext_df.columns:
   count_exe_ext_df = count_exe_ext_df.withColumn('PASS', F.lit(0.0))
if 'INDETERMINATE' not in count_exe_ext_df.columns:
   count_exe_ext_df = count_exe_ext_df.withColumn('INDETERMINATE', F.lit(0.0))
   
count_exe_ext_df=count_exe_ext_df.na.fill(0)
count_exe_ext_df=count_exe_ext_df.withColumn('TOTAL_RUNS', count_exe_ext_df.FAIL + count_exe_ext_df.PASS + count_exe_ext_df.INDETERMINATE).persist()

combined_exe_ext_df = combined_exe_ext_df.withColumn('all_cause_null', F.when(F.coalesce('cause_code_key_1','cause_code_key_2','cause_code_key_3','cause_code_key_4','cause_code_key_5','cause_code_key_6').isNotNull(),1))

combined_exe_ext_df=combined_exe_ext_df.filter(combined_exe_ext_df.all_cause_null==1)

criteria_columns = ['criteria_status_00','criteria_status_01','criteria_status_02','criteria_status_03','criteria_status_04','criteria_status_05','criteria_status_06','criteria_status_07','criteria_status_08','criteria_status_09','criteria_status_10','criteria_status_11','criteria_status_12','criteria_status_13','criteria_status_14','criteria_status_15','criteria_status_16','criteria_status_17']
cause_columns = ["cause_code_key_1","cause_code_key_2","cause_code_key_3","cause_code_key_4","cause_code_key_5","cause_code_key_6"] 

combined_exe_ext_df = combined_exe_ext_df.withColumn("temp1", F.array(criteria_columns)).withColumn("group_criteria", 
                      F.expr("FILTER(temp1, x -> x!='PASSED')")) \
                      .withColumn("temp2", F.array(cause_columns)).withColumn("cause_code", F.expr("FILTER(temp2, x -> x is not null)"))\
                      .select("test_case_name" ,"env","group_criteria","cause_code")
                                        
combined_exe_ext_df = combined_exe_ext_df.withColumn("criteria_failed_count", slen(combined_exe_ext_df.group_criteria)).select("test_case_name","env","criteria_failed_count","cause_code")

combined_exe_ext_df=combined_exe_ext_df.groupby('test_case_name','env').agg(F.collect_list(col('cause_code')).alias('UNIQUE_CAUSE_CODE'),round(mean("criteria_failed_count")).alias("AVG_CRITERIA_FAILED_COUNT")).persist()

combined_exe_ext_df=combined_exe_ext_df.select("test_case_name","env","AVG_CRITERIA_FAILED_COUNT",array_distinct(flatten("UNIQUE_CAUSE_CODE")).alias("UNIQUE_CAUSE_CODE"))

summary_df=count_exe_ext_df.join(combined_exe_ext_df,(count_exe_ext_df.TEST_CASE_KEY  ==  combined_exe_ext_df.test_case_name)
            & (count_exe_ext_df.env==combined_exe_ext_df.env) ,"left") \
            .join(unique_test_case_criteria,(count_exe_ext_df.TEST_CASE_KEY  ==  unique_test_case_criteria.unique_test_case)  
            & (count_exe_ext_df.env==unique_test_case_criteria.unique_env),"left") \
            .select('TEST_CASE_KEY',col('unique_env').alias('ENV'),'TOTAL_RUNS','FAIL','PASS','INDETERMINATE','CRITERIA_COUNT','AVG_CRITERIA_FAILED_COUNT','UNIQUE_CAUSE_CODE',lit(today).alias("WEEK_ID")).persist()
summary_df=summary_df.na.fill(0).persist()

try:
    s3.Object(bucket, out_path).load()
except botocore.exceptions.ClientError as e:  
    logger.debug(f'S3 Path Issue')
    
    if e.response['Error']['Code'] == "404":
        logger.debug(f'Object Doesnt exists')
        client.put_object(Bucket=bucket,Key=out_path)
        print("Object Doesn't exists - Creating new Path")
    else:
        print("Error occurred while fetching a file from S3. Try Again.")
        logger.debug(f'Error occurred while fetching a file from S3. Try Again.')
else:
    print("Object Exists")
    logger.debug(f'S3 Object Exists')
    
summary_df.write.mode('append').partitionBy('WEEK_ID','TEST_CASE_KEY').parquet(s3_file_path_target)
