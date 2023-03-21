
import os
import re
import json
import logging
import configparser
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, split, udf, lit, when, flatten, array_distinct, array, current_timestamp, datediff
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, DoubleType, ArrayType, DateType, IntegerType
import numpy as np
import boto3
import botocore


# Instantiate logger
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))

print("SUCCESS: Packages Imported")

s3 = boto3.resource('s3')
client = boto3.client('s3')

config = configparser.ConfigParser()

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("SUCCESS: Spark session Created")

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
    
today = datetime.now()

BUCKET = 'taas-test-artifacts'
FILE_TO_READ = 'Auto_Severity_Data/Config/config.ini'

# FILE_TO_READ = 'Config/config.ini'
obj = client.get_object(Bucket=BUCKET, Key=FILE_TO_READ) 
config.read_string(obj['Body'].read().decode())

# Variables
bucket = config.get('variables','bucket')

# Paths
ext_path=config.get('paths','ext_path')
out_path=config.get('paths','out_path')
test_case_sev_mapping=config.get('paths','test_case_sev_mapping')
cause_code_sev_mapping=config.get('paths','cause_code_sev_mapping')

# Jira
EMAIL=config.get('jira','EMAIL')
TOKEN=config.get('jira','TOKEN')

ext_schemaConf = config.get('schema','ext_schema')
ext_schema=read_schema(ext_schemaConf)

s3_file_path_src= 's3a://{}/{}'.format(bucket,out_path)

past_day=today - timedelta(days=1)

criteria_columns = ['criteria_status_00','criteria_status_01','criteria_status_02','criteria_status_03','criteria_status_04','criteria_status_05','criteria_status_06','criteria_status_07','criteria_status_08','criteria_status_09','criteria_status_10','criteria_status_11','criteria_status_12','criteria_status_13','criteria_status_14','criteria_status_15','criteria_status_16','criteria_status_17']
cause_columns = ["cause_code_key_1","cause_code_key_2","cause_code_key_3","cause_code_key_4","cause_code_key_5","cause_code_key_6"] 

src_schema=StructType([
     StructField('TEST_CASE_KEY',StringType(),True),
     StructField('ENV',StringType(),True),
     StructField('TOTAL_RUNS',LongType(),True),
     StructField('FAIL',LongType(),True),
     StructField('CRITERIA_COUNT',IntegerType(),True),
     StructField('AVG_CRITERIA_FAILED_COUNT',DoubleType(),False),
     StructField('UNIQUE_CAUSE_CODE',ArrayType(StringType(),True),False),
     StructField('WEEK_ID',DateType(),False)])
     
     
# Functions
def weighted_average(TEST_CASE_SEV,TEST_CASE_FAILURE_SEV,JIRA_FAILURE_SEV,CRITERIA_FAILURE_COUNT_SEV,TICKET_COUNT_SEV,DAYS_OPEN_SEV,DEFAULT_SEVERITY,NEW_CAUSE_VALUE,TOTAL_CAUSE_VALUE):
    Severity=DEFAULT_SEVERITY
    if DEFAULT_SEVERITY==None:
        weighted_avg=0.3*(TEST_CASE_SEV)+0.2*(TEST_CASE_FAILURE_SEV)+0.1*(JIRA_FAILURE_SEV)+0.15*(CRITERIA_FAILURE_COUNT_SEV)+0.1*(TICKET_COUNT_SEV)+0.1*(DAYS_OPEN_SEV)+0.1*(NEW_CAUSE_VALUE)+0.15*(TOTAL_CAUSE_VALUE)

        if ((weighted_avg>=0) & (weighted_avg<50)):
            Severity="Sev 3 - Minor"

        elif ((weighted_avg>=50) & (weighted_avg<80)):
            Severity="Sev 2 - Major"

        elif ((weighted_avg>=80) & (weighted_avg<=100)):
            Severity="Sev 1 - Critical"
            
    return Severity

weighted_average_udf = udf(weighted_average, StringType())
slen = udf(lambda s: len(s), IntegerType())
map__cause_values = udf(lambda keys: list(map(cause_code_dict.get, keys)),ArrayType(IntegerType()))

df = spark.read.option("header",True).csv("s3://taas-test-artifacts/Auto_Severity_Data/Raw_Data_Source/Test_Case_Mapping_Spirent.csv")
test_sev_map_df = df.select(col("Defect_Test_Case_Name"),col("Severity "))
test_sev_map_df = test_sev_map_df.withColumnRenamed("Severity ", "TEST_CASE_SEVERITY")
test_sev_map_df.createOrReplaceTempView("test_sev_map")

# Cause Code data frame mapped with severity 
df1 = spark.read.option("header",True).csv("s3a://taas-test-artifacts/Auto_Severity_Data/Raw_Data_Source/Cause Codes Severity Mapping.csv")
df1 = df1.toPandas()

df1.rename(columns = {'Severity':'Old_severity'}, inplace = True)

# Create a new severity column to preserve old column
df1['Severity'] = df1['Old_severity'].replace([1,2,3,'ignore',np.nan,'?'],['Sev 1 - Critical','Sev 2 - Major','Sev 3 - Minor', 'No value','No value','No value'])

# Add the new group_severity column 
df2 = df1[df1['Severity'] != 'No value']
df3 = df2.groupby(['Series Group','Severity']) \
    .size() \
    .sort_values() \
    .groupby(level=0) \
    .tail(1) \
    .reset_index()
df3 = df3[['Series Group', 'Severity']]

# Perform a left join where df1 is the main table and df2 is the new data frame with the max severity of group 
cause_code_sev_map_df = df1.merge(df3 , on = 'Series Group', how = 'left')
cause_code_sev_map_df.rename(
    columns = {'Severity_x':'Severity','Severity_y':'Group Severity' }, 
    inplace = True
    )

#select only required columns in a data frame 
cause_code_sev_map_df = cause_code_sev_map_df[['Series Group','Code','Severity', 'Group Severity']]
cause_code_sev_map_df['Cause_Severity_Value'] = np.where(
    cause_code_sev_map_df['Severity']=='No value',
    cause_code_sev_map_df['Group Severity'],
    cause_code_sev_map_df['Severity']
)

cause_code_sev_map_df=cause_code_sev_map_df.replace(
    to_replace=['Sev 1 - Critical', 'Sev 2 - Major','Sev 3 - Minor'],
    value=[100, 70, 50]
)

cause_code_dict=dict(zip(cause_code_sev_map_df.Code, cause_code_sev_map_df.Cause_Severity_Value))

auth = HTTPBasicAuth(EMAIL, TOKEN)

headers = {
    "Accept": "application/json"
}

query='project=DEF AND reporter in (61955256fe9f300068ec3c17)'

def get_records(startAt):
    url="https://dish-wireless-network.atlassian.net/rest/api/2/search/?jql={0}&startAt={1}&maxResults=100".format(query,startAt)
    response = requests.request( 
        "GET",
        url,
        headers=headers,
        auth=auth
    )
    projectIssues = json.dumps(json.loads(response.text),
                            sort_keys=True,
                            indent=4,
                            separators=(",", ": "))

    dictProjectIssues = json.loads(projectIssues)
    return dictProjectIssues

def get_jira_data(startAt):
    url="https://dish-wireless-network.atlassian.net/rest/api/2/search/?jql={0}&startAt={1}&maxResults=100".format(query,startAt)

    response = requests.request( 
        "GET",
        url,
        headers=headers,
        auth=auth
    )
    projectIssues = json.dumps(json.loads(response.text),
                            sort_keys=True,
                            indent=4,
                            separators=(",", ": "))
    return projectIssues

records=get_records(0)
listAllIssues = []
test_list=[]
total_issues=records['total']
quotient, reminder = divmod(total_issues, 100)
print(quotient+1)

for rec in range(quotient+1):
    dictProjectIssues=json.loads(get_jira_data(rec))
    test_list.append (dictProjectIssues)
    keyIssue,keySummary,keyFailed,keyReporter,keyStatus,KeyComment,KeyAssignee,KeyAssigneeMail,KeyISV,Severity,DaysOpen,Environment,TechnicalRootCause,created,comment= "","","","","","","","","","","","","","",""

def iterateDictIssues(oIssues, listInner):
    for (key, values) in oIssues.items():
        if key == 'fields':
            fieldsDict = dict(values)
            iterateDictIssues(fieldsDict, listInner)
        elif key == 'reporter':
            reporterDict = dict(values)
            iterateDictIssues(reporterDict, listInner)
        elif key == 'key':
            keyIssue = values
            listInner.append(keyIssue)
        elif key == 'summary':
            keySummary = values
            listInner.append(keySummary)
        elif key == 'displayName':
            keyReporter = values
            listInner.append(keyReporter)
        elif key == 'status':
            keyStatus = values['name']
            listInner.append(keyStatus)
        elif key == 'customfield_10092':
            KeyISV = values['value']
            listInner.append(KeyISV)
        elif key == 'customfield_10037':
            Severity = values['value']
            listInner.append(Severity)
        elif key == 'customfield_10281':
            keyFailed = values
            listInner.append(keyFailed)
        elif key == 'customfield_10188':
            DaysOpen = values
            listInner.append(DaysOpen)
        elif key == 'created':
            created = values
            listInner.append(created)
            
for json in range(len(test_list)):
    for key, value in test_list[json].items():
        if(key == "issues"):
            totalIssues = len(value)
            for eachIssue in range(totalIssues):
                listInner = []
                iterateDictIssues(value[eachIssue], listInner)
                listAllIssues.append(listInner)
                

columns=["CREATED_DATE","Severity","ISV","CLOSED_DAYS","TEST_FAILED_COUNT","REPORTER", "CURRENT_STATUS","SUMMARY","ISSUE_KEY"]
jira_data = spark.createDataFrame(listAllIssues, columns)
jira_data = jira_data.fillna(value=0)

# Calculate difference between two dates in days in pyspark
jira_data=jira_data.withColumn("OPEN_DAYS", datediff(current_timestamp(),col("CREATED_DATE")))

jira_data=jira_data.withColumn("SUMMARY_KEY", split(col("SUMMARY"), "/").getItem(0)).withColumn("ENV", split(col("SUMMARY"), "/").getItem(1))
jira_data=jira_data.distinct()
print("SUCCESS: JIRA Dataframe Created, jira_data")

historical_jira_defects=jira_data.filter("Current_Status='Closed'")
historical_jira_defects.createOrReplaceTempView("historical_jira_defects")
historical_jira_data_defects=spark.sql("""
select SUMMARY, SUMMARY_KEY, ENV, round(avg(CLOSED_DAYS)) as HIST_AVG_CLOSED_DAYS,
round(avg(TEST_FAILED_COUNT) ) as HIST_COUNTER_AVG_FAILED_COUNT, count(issue_key) as HIST_TICKET_COUNT from historical_jira_defects where ISSUE_KEY like 'DEF%' group by Summary, Summary_Key, env order by summary
""")
print("SUCCESS: HISTORICAL JIRA Dataframe Created, historical_jira_defects")

opened_jira_data_defects_df=jira_data.filter("Current_Status!='Closed' AND Current_Status!='Rejected' AND  Current_Status!='Retest'")
opened_jira_data_defects_df.createOrReplaceTempView("opened_jira_data_defects")
opened_jira_data_defects_df=spark.sql("""select ISSUE_KEY, SUMMARY , SUMMARY_KEY, ENV,Severity, TEST_FAILED_COUNT AS OPEN_COUNTER_FAILED_COUNT, OPEN_DAYS, CREATED_DATE from opened_jira_data_defects where ISSUE_KEY like 'DEF%'  order by summary""")

opened_jira_data_defects_df= opened_jira_data_defects_df.withColumn('DEFAULT_SEVERITY_VALID',when((past_day > col('CREATED_DATE')) & (col('CREATED_DATE')<=today),True).otherwise(False))
opened_jira_data_defects_df=opened_jira_data_defects_df.drop('CREATED_DATE')
print("SUCCESS: OPEN JIRA Dataframe Created, opened_jira_data_defects_df")

def get_connection(key):
    url="https://dish-wireless-network.atlassian.net/rest/api/3/issue/{0}/comment".format(key)
    headers={
      "Accept": "application/json",
        "Content-Type": "application/json"
    }
    response=requests.get(url,headers=headers,auth=(EMAIL,TOKEN))
    data=response.json()
    return data

def get_comments(key,fail_count):
    data=get_connection(key)
    count=data["total"]
    test_exec=[]
    if count!=0:
      for comment in reversed(range(count)):
        if data["comments"][comment]["body"]['content'][0]['type']=='blockquote':
          execution_string=data["comments"][comment]["body"]['content'][0]['content'][0]['content'][0]['text']
          execution=re.search('Test execution failure #{} \((.*)\)$'.format(fail_count), execution_string)
          if execution!=None:
            return execution.group(1)
        
udf_get_comments= udf(get_comments, StringType())

# logger.info("Getting Execution ID from the comments")
opened_jira_data_defects_df=opened_jira_data_defects_df.withColumn('Execution', udf_get_comments('ISSUE_KEY', 'OPEN_COUNTER_FAILED_COUNT'))

execution_list=opened_jira_data_defects_df.select('Execution').rdd.flatMap(lambda x: x).collect()
list_not_present=[]
list_present=[] 

for i in execution_list:
    path1='{}{}.json'.format(ext_path,i)
    try:
        s3.Object(bucket, path1).load()
    except botocore.exceptions.ClientError as e:
        logger.debug(f'S3 Path Issue')
        if e.response['Error']['Code'] == "404":
            logger.debug(f'Object Doesnt exists:'+path1)
            print('Object Doesnt exists:'+path1)
        else:
            logger.debug(f'Error occurred while fetching a file from S3. Try Again.')
            print('Error occurred while fetching a file from S3. Try Again.')
        
    else:
        list_present.append('s3a://{}/{}'.format(bucket,path1))
        logger.debug(list_present)
        # print("SUCCESS: List Present"+list_present)

# if len(list_present)>0:
open_source_data_exe_df = spark.read.schema(ext_schema).json(path=list_present, multiLine=True)    

    # Rename columns
open_source_data_exe_df = open_source_data_exe_df.withColumnRenamed('Landslide_validation_status_0.str', 'criteria_status_00') \
        .withColumnRenamed('Landslide_validation_status_1.str', 'criteria_status_01') \
        .withColumnRenamed('Landslide_validation_status_2.str', 'criteria_status_02') \
        .withColumnRenamed('Landslide_validation_status_3.str', 'criteria_status_03') \
        .withColumnRenamed('Landslide_validation_status_4.str', 'criteria_status_04') \
        .withColumnRenamed('Landslide_validation_status_5.str', 'criteria_status_05') \
        .withColumnRenamed('Landslide_validation_status_6.str', 'criteria_status_06') \
        .withColumnRenamed('Landslide_validation_status_7.str', 'criteria_status_07') \
        .withColumnRenamed('Landslide_validation_status_8.str', 'criteria_status_08') \
        .withColumnRenamed('Landslide_validation_status_9.str', 'criteria_status_09') \
        .withColumnRenamed('Landslide_validation_status_10.str', 'criteria_status_10') \
        .withColumnRenamed('Landslide_validation_status_11.str', 'criteria_status_11') \
        .withColumnRenamed('Landslide_validation_status_12.str', 'criteria_status_12') \
        .withColumnRenamed('Landslide_validation_status_13.str', 'criteria_status_13') \
        .withColumnRenamed('Landslide_validation_status_14.str', 'criteria_status_14') \
        .withColumnRenamed('Landslide_validation_status_15.str', 'criteria_status_15') \
        .withColumnRenamed('Landslide_validation_status_16.str', 'criteria_status_16') \
        .withColumnRenamed('Landslide_validation_status_17.str', 'criteria_status_17') \
        .withColumnRenamed('Landslide_cause_code_key_1.str', 'cause_code_key_1') \
        .withColumnRenamed('Landslide_cause_code_key_2.str', 'cause_code_key_2') \
        .withColumnRenamed('Landslide_cause_code_key_3.str', 'cause_code_key_3') \
        .withColumnRenamed('Landslide_cause_code_key_4.str', 'cause_code_key_4') \
        .withColumnRenamed('Landslide_cause_code_key_5.str', 'cause_code_key_5') \
        .withColumnRenamed('Landslide_cause_code_key_6.str', 'cause_code_key_6') \
        .withColumnRenamed('Landslide_cause_code_key_7.str', 'cause_code_key_7') \
        .withColumnRenamed('Test_Case_Name.str', 'test_case_name') \
        .select('reportID','test_case_name','criteria_status_00','criteria_status_01','criteria_status_02','criteria_status_03','criteria_status_04','criteria_status_05','criteria_status_06',
                'criteria_status_07','criteria_status_08','criteria_status_09','criteria_status_10','criteria_status_11','criteria_status_12',
                'criteria_status_13','criteria_status_14','criteria_status_15','criteria_status_16','criteria_status_17','cause_code_key_1',
                'cause_code_key_2','cause_code_key_3','cause_code_key_4','cause_code_key_5','cause_code_key_6','cause_code_key_7')
                
open_source_data_exe_df = open_source_data_exe_df.withColumn("temp1", F.array(criteria_columns)).withColumn("group_criteria", F.expr("FILTER(temp1, x -> x!='PASSED')")) \
                            .withColumn("temp2", F.array(cause_columns)).withColumn("OPEN_CAUSE_CODE", F.expr("FILTER(temp2, x -> x is not null)")) \
                            .select("reportID" ,"group_criteria","OPEN_CAUSE_CODE")
                            
open_source_data_exe_df = open_source_data_exe_df.withColumn("OPEN_CRITERIA_FAILURE_COUNT", slen(open_source_data_exe_df.group_criteria)).select("reportID","OPEN_CRITERIA_FAILURE_COUNT","OPEN_CAUSE_CODE")

opened_jira_data_defects_df=opened_jira_data_defects_df.join(open_source_data_exe_df,opened_jira_data_defects_df.Execution==open_source_data_exe_df.reportID,"left")
opened_jira_data_defects_df=opened_jira_data_defects_df.drop(opened_jira_data_defects_df.reportID)

historical_summary_df = spark.read.schema(src_schema).parquet(s3_file_path_src)
historical_summary_df = historical_summary_df.distinct()
historical_summary_df=historical_summary_df.withColumn('CRITERIA_COUNT', (historical_summary_df.TOTAL_RUNS*historical_summary_df.AVG_CRITERIA_FAILED_COUNT)).persist()

historical_summary_df.createOrReplaceTempView("historical_summary_count_df")
historical_summary_count_df=spark.sql("""
SELECT
TEST_CASE_KEY,
ENV,
SUM(TOTAL_RUNS) as TOTAL_RUNS,
SUM(FAIL) as TOTAL_FAIL,
SUM(CRITERIA_COUNT)/SUM(TOTAL_RUNS) as HIST_CRITERIA_AVG_COUNT

FROM 
historical_summary_count_df
GROUP BY
TEST_CASE_KEY,
ENV
""")

historical_summary_df=historical_summary_df.groupby('TEST_CASE_KEY','ENV').agg(F.collect_set(col('UNIQUE_CAUSE_CODE')).alias('HIST_CAUSE_CODE')).persist()
historical_summary_df=historical_summary_df.select("TEST_CASE_KEY","ENV",array_distinct(flatten("HIST_CAUSE_CODE")).alias("HIST_CAUSE_CODE"))

historical_jira_data_defects.createOrReplaceTempView("historical_jira_data_defects")
historical_summary_count_df.createOrReplaceTempView("historical_summary_count_df")
historical_summary_df.createOrReplaceTempView("historical_summary_df")

historical_jira_data_defects= spark.sql("""
SELECT 
hjdd.SUMMARY_KEY,hsc.TOTAL_RUNS,hsc.TOTAL_FAIL,hsc.HIST_CRITERIA_AVG_COUNT,hs.HIST_CAUSE_CODE,SUMMARY,hjdd.ENV,HIST_AVG_CLOSED_DAYS,HIST_COUNTER_AVG_FAILED_COUNT,HIST_TICKET_COUNT FROM
historical_jira_data_defects as hjdd
LEFT JOIN historical_summary_count_df as hsc on hjdd.SUMMARY_KEY == hsc.TEST_CASE_KEY AND hjdd.ENV==hsc.ENV
LEFT JOIN historical_summary_df as hs on hjdd.SUMMARY_KEY == hs.TEST_CASE_KEY AND hjdd.ENV==hs.ENV
""")

historical_jira_data_defects=historical_jira_data_defects.withColumn("TOTAL_FAIL_RATE",(col('TOTAL_FAIL')/col('TOTAL_RUNS'))*100) \
                            .select('SUMMARY_KEY','SUMMARY','ENV','TOTAL_FAIL_RATE','HIST_CRITERIA_AVG_COUNT','HIST_CAUSE_CODE',
                            'HIST_AVG_CLOSED_DAYS','HIST_COUNTER_AVG_FAILED_COUNT','HIST_TICKET_COUNT')
                            
                            
fail_max_limit = historical_jira_data_defects.agg({"TOTAL_FAIL_RATE": "max"}).collect()[0][0]
fail_limit_50 = fail_max_limit*0.5
fail_limit_70 = fail_max_limit*0.7
jira_fail_max_limit = historical_jira_data_defects.agg({"HIST_TICKET_COUNT": "max"}).collect()[0][0]
jira_fail_max_limit_50 = jira_fail_max_limit*0.5
jira_fail_max_limit_70 = jira_fail_max_limit*0.7

historical_jira_data_defects.createOrReplaceTempView("overall_historical")
opened_jira_data_defects_df.createOrReplaceTempView("opened_jira_defects_details")

overall_final_df=spark.sql("""
SELECT 
ISSUE_KEY,
curr.SUMMARY_KEY,
curr.ENV,
curr.Severity AS CURRENT_SEVERITY,
hist.HIST_CAUSE_CODE,
curr.OPEN_CAUSE_CODE,
CASE
    WHEN tc.TEST_CASE_SEVERITY ="Sev 3 - Minor" THEN 50
    WHEN tc.TEST_CASE_SEVERITY ="Sev 2 - Major" THEN 70
    WHEN tc.TEST_CASE_SEVERITY ="Sev 1 - Critical" THEN 100
    END AS TEST_CASE_SEV,
CASE
    WHEN hist.TOTAL_FAIL_RATE >=0 AND hist.TOTAL_FAIL_RATE < {0} THEN 50
    WHEN hist.TOTAL_FAIL_RATE >= {0} AND hist.TOTAL_FAIL_RATE < {1} THEN 70
    WHEN hist.TOTAL_FAIL_RATE >= {2} THEN 100
    END AS TEST_CASE_FAILURE_SEV,
CASE
    WHEN hist.HIST_TICKET_COUNT >=0 AND hist.HIST_TICKET_COUNT < {3} THEN 50
    WHEN hist.HIST_TICKET_COUNT >= {3} AND hist.HIST_TICKET_COUNT < {4} THEN 70
    WHEN hist.HIST_TICKET_COUNT >= {5} THEN 100
    END AS JIRA_FAILURE_SEV,
CASE
    WHEN curr.OPEN_COUNTER_FAILED_COUNT>=0 AND curr.OPEN_COUNTER_FAILED_COUNT<ROUND((0.5)*(hist.HIST_COUNTER_AVG_FAILED_COUNT)) THEN 50
    WHEN curr.OPEN_COUNTER_FAILED_COUNT>=ROUND((0.5)*(hist.HIST_COUNTER_AVG_FAILED_COUNT)) AND curr.OPEN_COUNTER_FAILED_COUNT<(hist.HIST_COUNTER_AVG_FAILED_COUNT) THEN 70
    WHEN curr.OPEN_COUNTER_FAILED_COUNT>=(hist.HIST_COUNTER_AVG_FAILED_COUNT) THEN 100
    END AS TICKET_COUNTER_COUNT_SEV,
CASE
    WHEN curr.OPEN_CRITERIA_FAILURE_COUNT>=0 AND curr.OPEN_CRITERIA_FAILURE_COUNT<ROUND((0.5)*(hist.HIST_CRITERIA_AVG_COUNT)) THEN 50
    WHEN curr.OPEN_CRITERIA_FAILURE_COUNT>=ROUND((0.5)*(hist.HIST_CRITERIA_AVG_COUNT)) AND curr.OPEN_CRITERIA_FAILURE_COUNT<(0.7)*(hist.HIST_CRITERIA_AVG_COUNT) THEN 70
    WHEN curr.OPEN_CRITERIA_FAILURE_COUNT>=(0.7)*(hist.HIST_CRITERIA_AVG_COUNT) THEN 100
    END AS CRITERIA_FAILURE_COUNT_SEV,
CASE
    WHEN curr.OPEN_DAYS>=0 AND curr.OPEN_DAYS<ROUND((0.5)*(hist.HIST_AVG_CLOSED_DAYS)) THEN 50
    WHEN curr.OPEN_DAYS>=ROUND((0.5)*(hist.HIST_AVG_CLOSED_DAYS)) AND curr.OPEN_DAYS<(hist.HIST_AVG_CLOSED_DAYS) THEN 70
    WHEN curr.OPEN_DAYS>=(hist.HIST_AVG_CLOSED_DAYS) THEN 100
    END AS DAYS_OPEN_SEV,
curr.DEFAULT_SEVERITY_VALID
    
FROM opened_jira_defects_details curr
LEFT JOIN overall_historical hist ON curr.SUMMARY=hist.SUMMARY
LEFT JOIN test_sev_map tc ON curr.SUMMARY_KEY=tc.Defect_Test_Case_Name
""".format(fail_limit_50,fail_limit_70,fail_max_limit,jira_fail_max_limit_50,jira_fail_max_limit_70,jira_fail_max_limit))

null_value = array([lit("")])
overall_final_df = overall_final_df.withColumn('HIST_CAUSE_CODE', when(overall_final_df['HIST_CAUSE_CODE'].isNull(), null_value).otherwise(overall_final_df['HIST_CAUSE_CODE'])) \
                                    .withColumn('OPEN_CAUSE_CODE', when(overall_final_df['OPEN_CAUSE_CODE'].isNull(), null_value).otherwise(overall_final_df['OPEN_CAUSE_CODE']))
                                    
zero_list = array([lit(0)])

overall_final_df=overall_final_df.withColumn('NEW_CAUSE_VALUE',when(slen(F.array_except('OPEN_CAUSE_CODE', 'HIST_CAUSE_CODE'))>0,100).otherwise(0))
overall_final_df=overall_final_df.withColumn('TOTAL_CAUSE_VALUE_LIST',when(overall_final_df.OPEN_CAUSE_CODE!=null_value,map__cause_values(overall_final_df.OPEN_CAUSE_CODE)).otherwise(zero_list))
overall_final_df = overall_final_df.withColumn('TOTAL_CAUSE_VALUE',F.expr('AGGREGATE(TOTAL_CAUSE_VALUE_LIST, 0, (acc, x) -> acc + x)'))

overall_final_df = overall_final_df.withColumn('DEFAULT_SEVERITY', F.when(F.coalesce('ENV','TEST_CASE_SEV','TICKET_COUNTER_COUNT_SEV','DAYS_OPEN_SEV').isNull(), 'Sev 1 - Critical'))
overall_final_df=overall_final_df.na.fill(value=0)
overall_final_df=overall_final_df.withColumn('COMPUTED_SEVIRITY',weighted_average_udf(overall_final_df['TEST_CASE_SEV'],overall_final_df['TEST_CASE_FAILURE_SEV'],overall_final_df['JIRA_FAILURE_SEV'],overall_final_df['CRITERIA_FAILURE_COUNT_SEV'],overall_final_df['TICKET_COUNTER_COUNT_SEV'],overall_final_df['DAYS_OPEN_SEV'],overall_final_df['DEFAULT_SEVERITY'],overall_final_df['NEW_CAUSE_VALUE'],overall_final_df['TOTAL_CAUSE_VALUE']))

overall_final_df=overall_final_df.select('ISSUE_KEY','SUMMARY_KEY','ENV','CURRENT_SEVERITY','COMPUTED_SEVIRITY','DEFAULT_SEVERITY_VALID')


overall_final_df=overall_final_df.withColumn("SEVERITY_CHANGE", F.when(col("CURRENT_SEVERITY") != col("COMPUTED_SEVIRITY") , True).otherwise(False))
overall_final_df=overall_final_df.withColumn("SEVERITY_REGRESSION_CHANGE",\
                             when(((col('DEFAULT_SEVERITY_VALID')==False) & ((col("CURRENT_SEVERITY") == "Sev 1 - Critical") & (col("COMPUTED_SEVIRITY") == "Sev 2 - Major"))\
                                  | ((col("CURRENT_SEVERITY") == "Sev 2 - Major") & (col("COMPUTED_SEVIRITY") == "Sev 3 - Minor"))\
                                  | ((col("CURRENT_SEVERITY") == "Sev 1 - Critical") & (col("COMPUTED_SEVIRITY") == "Sev 3 - Minor")))
                                  , True).otherwise(False))
overall_final_df=overall_final_df.filter("SEVERITY_CHANGE==True AND SEVERITY_REGRESSION_CHANGE==False")
overall_final_df=overall_final_df.select('ISSUE_KEY','COMPUTED_SEVIRITY')
overall_final_df=overall_final_df.toPandas()

print(overall_final_df)
if overall_final_df.shape[0]>0:
    overall_final_df.to_csv( "s3a://taas-test-artifacts/Auto_Severity_Data/Daily_Computed_Jira_Severity/{}.csv".format(today) ,index=False)
    print("SUCCESS: Found Computed Severity Change for JIRA tickets and uploaded to S3.")

else:
    print("Not Found any Computed Severity Change for jira tickets.")
    