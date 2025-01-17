"""
Unitary tests for exporters/secrets_manager.py.

:author: elturkym

:since: 2021-07-15
"""

import json
import logging

import boto3
import requests
from botocore.exceptions import ClientError
from requests.exceptions import HTTPError

EC2_INFO_URL = "http://169.254.169.254/latest/dynamic/instance-identity/document"
LOGGER = logging.getLogger(__name__)


class SecretsManagerError(ValueError):
    """
    Unable to retrieve secret details
    """


def get_db_credentials(secret_name):
    LOGGER.info('retrieving AWS secret details')

    ec2_info_response = requests.get(EC2_INFO_URL)

    try:
        ec2_info_response.raise_for_status()
    except HTTPError as e:
        raise SecretsManagerError("EC2 information request failed") from e

    ec2_info = ec2_info_response.json()
    region_name = ec2_info["region"]

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise SecretsManagerError("Couldn't retrieve secret details") from e
    else:
        # Decrypts secret using the associated KMS CMK.]
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
