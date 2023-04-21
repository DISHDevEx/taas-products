import json

# from taas_utils import get_param
# from jira import JIRA
import os
import logging

# import boto3
from bs4 import BeautifulSoup
import requests

# from taas_jira_api_response import TaasJiraApiResponse
from requests.auth import HTTPBasicAuth


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))


def get_velocity_report(execution_id, velocity_user, velocity_password, velocity_url):

    # execution_id = event.get('data').get('ExecutionId')
    ## issue =  event.get('issue')
    try:
        logger.info(execution_id)
        tResponse = requests.get(
            velocity_url + "/velocity/api/auth/v2/token",
            auth=HTTPBasicAuth(velocity_user, velocity_password),
            verify=False,
        )
        token = json.loads(tResponse.text)["token"]
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Auth-Token": token,
        }
        url = (
            velocity_url
            + "/ito/reporting/v1/reports/"
            + execution_id
            + "/print?templateId=0f749bdf-77c8-4ebe-9c6c-a24655f14218"
        )
        velocity_report = requests.get(url, headers=headers, verify=False)
        logger.info(velocity_report.text)
        cleantext = BeautifulSoup(velocity_report.text, "html.parser").text
        s = cleantext.split("\n")
        final_list = []
        for i in range(0, len(s)):
            if len(s[i]) > 0:
                final_list.append(s[i])
        keywords = ["Status", " if "]
        new_list = []
        for i in final_list:
            if any(word in i for word in keywords):
                new_list.append(i)

    except Exception as e:
        logger.info(f"!!!!!!!Error in getting the Velocity Messages: {e}")
        raise

    return new_list
