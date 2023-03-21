import urllib.parse
import boto3
import pandas as pd
from jira import JIRA
import os

print('Loading function')

s3 = boto3.client('s3')

def lambda_handler(event, context):

    EMAIL = os.environ.get('EMAIL')
    TOKEN = os.environ.get('TOKEN')
    HOST = os.environ.get('HOST')

    options = {
        'server': HOST
    }
    jira = JIRA(options, basic_auth=(EMAIL, TOKEN))

    bucket = event['Records'][0]['s3']['bucket']['name']

    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        overall_final_df = pd.read_csv(response['Body']) # 'Body' is a key word
        # INDEX = overall_final_df['INDEX'].tolist()
        INDEX=overall_final_df.count()[0]
        print(INDEX)
        ISSUE_KEY_STRING = ','.join(overall_final_df['ISSUE_KEY'].tolist())

        for x in range(INDEX):
            Issue=overall_final_df['ISSUE_KEY'].values[(x)]
            print(Issue)
            value_update=overall_final_df['COMPUTED_SEVIRITY'].values[(x)]
            print(value_update)
            issue = jira.issue(Issue)
            issue.update(fields={'customfield_10037': {'value':value_update}})
            
        return "Successfully Updated JIRA Tickets: {}".format(ISSUE_KEY_STRING)
            
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e