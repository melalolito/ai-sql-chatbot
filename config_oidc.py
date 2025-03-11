import snowflake.connector
import boto3
from requests import request
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

SF_URL = '<snowflake_url>'
SF_ACCOUNT = '<snowflake_account>'
SF_USER = '<snowflake_user>'
SF_ROLE = '<snowflake_role>'
SF_DATABASE = '<snowflake_database>'
SF_SCHEMA = '<snowflake_schema>'
SF_WAREHOUSE = '<snowflake_warehouse>'

AUTH_URL = '<auth_url>'
AUTH_API_REGION = '<region>'
JWT_REQUEST_SCOPE = [f'SESSION:ROLE:{SF_ROLE}']
JWT_REQUEST_AUDIENCE = f'{SF_ACCOUNT}.snowflakecomputing.com'
JWT_REQUEST_HEADERS = {
    'Content-Type': 'application/x-www-form-urlencoded'
}

def request_jwt(session):
    """
    Requests an OAuth token for authenticating the Snowflake connection using AWS Signature V4.
    
    This function:
    1. Signs a request with AWS credentials to obtain an OAuth token.
    2. Adds the required `SESSION:ROLE` scope for Snowflake role-based access.
    3. Returns the OAuth access token required to authenticate to Snowflake.
    
    Args:
    session (boto3.Session): The AWS session used to retrieve temporary credentials.
    
    Returns:
    response (requests.Response): The response from the OAuth endpoint containing the access token.
    """
    data = {
        'scope': JWT_REQUEST_SCOPE,
        'audience': JWT_REQUEST_AUDIENCE
    }
    auth_request = AWSRequest(
        method='POST',
        url=AUTH_URL,
        data=data,
        headers=JWT_REQUEST_HEADERS
    )

    credentials = session.get_credentials()
    frozen_credentials = credentials.get_frozen_credentials()
    SigV4Auth(frozen_credentials, 'execute-api', AUTH_API_REGION).add_auth(auth_request)

    return request(
        method='POST',
        url=AUTH_URL,
        headers=dict(auth_request.headers),
        data=data
    )

auth_response = request_jwt(boto3.Session())
token = auth_response.json().get('access_token')

sf_options = {
    'url': SF_URL,
    'account': SF_ACCOUNT,
    'user': SF_USER,
    'role': SF_ROLE,
    'database': SF_DATABASE,
    'schema': SF_SCHEMA,
    'warehouse': SF_WAREHOUSE,
    'authenticator': 'oauth',
    'token': token
}

sf_conn = snowflake.connector.connect(**sf_options)
