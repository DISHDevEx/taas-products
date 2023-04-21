# importing packages
from jira import JIRA
import json
import boto3
import logging
import os
import botocore
import re
from botocore.errorfactory import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
cause_list = []
s3 = boto3.resource("s3")


def get_execution_record(Test_Execution, bucket, file_key, pattern):

    logger.debug(f"Enter get_cause_code")

    try:
        # file='{0}{1}.json'.format(file_key,Test_Execution)
        file = f"{file_key}{Test_Execution}.json"
        obj = s3.Object(bucket, file)
        data = json.load(obj.get()["Body"])
        if data["result"] != "PASSED":

            # Iterating the JSON to get respective detials
            for key, value in data.items():
                if bool(re.match(pattern, key)) == True:
                    cause_list.append(value)

                # elif bool(re.match(pattern_count, key))==True:
                #     cause_count.append(value)

        cause_code = ", ".join(cause_list)

        # comment to be added to JIRA
        # Comment_Text = 'Cause Codes:\n {0}'.format(cause_code)

        if cause_code is None:
            return "No Cause Code"
        else:
            return cause_code

    except Exception as e:
        logger.info(f"Error in getting the cause code: {e}")
        raise

