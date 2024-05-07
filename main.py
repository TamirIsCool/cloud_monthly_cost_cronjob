import boto3
import yaml
import os
import sys
from datetime import datetime, timedelta
from azure.identity import ClientSecretCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.costmanagement.models import QueryDefinition, TimeframeType
import psycopg2

def load_config():
    config_path = '/config/config.yaml'  # Ensure this path is correct
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Error: Config file not found at {config_path}")
        exit(1)
    except yaml.YAMLError as exc:
        print(f"Error parsing YAML file: {exc}")
        exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)

def get_previous_month_range():
    today = datetime.today()
    first_of_this_month = today.replace(day=1)
    last_of_previous_month = first_of_this_month - timedelta(days=1)
    first_of_previous_month = last_of_previous_month.replace(day=1)

    # AWS uses 'YYYY-MM-DD'
    aws_start_date = first_of_previous_month.strftime('%Y-%m-%d')
    aws_end_date = last_of_previous_month.strftime('%Y-%m-%d')

    # Azure requires 'YYYY-MM-DDTHH:MM:SSZ'
    azure_start_date = first_of_previous_month.strftime('%Y-%m-%dT00:00:00Z')
    azure_end_date = last_of_previous_month.strftime('%Y-%m-%dT23:59:59Z')

    return (aws_start_date, aws_end_date), (azure_start_date, azure_end_date)

def assume_role(account_id, role_name):
    sts_client = boto3.client('sts')
    assumed_role = sts_client.assume_role(
        RoleArn=f"arn:aws:iam::{account_id}:role/{role_name}",
        RoleSessionName="AssumeRoleSession"
    )
    credentials = assumed_role['Credentials']
    return boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

def fetch_aws_costs(aws_account, start_date, end_date):
    session = assume_role(aws_account['accountId'], aws_account['roleName'])
    client = session.client('ce', region_name='us-east-1')

    # Initialize the filter
    filter = {}

    # Check if tag filtering is required and structure the filter accordingly
    if aws_account['tags'] == "true":
        filter = {
            'Tags': {
                'Key': aws_account['tagKey'],
                'Values': [aws_account['tagValue']],
                'MatchOptions': ['EQUALS']  # Use EQUALS for exact matching, adjust as needed
            }
        }

        try:
            response = client.get_cost_and_usage(
                TimePeriod={'Start': start_date, 'End': end_date},
                Granularity='MONTHLY',
                Metrics=["UnblendedCost"],
                Filter=filter
            )
        except Exception as e:
            print(f"Error fetching AWS costs: {e}", file=sys.stderr)
            return None
        return float(response['ResultsByTime'][0]['Total']['UnblendedCost']['Amount'])
    else:
        try:
            response = client.get_cost_and_usage(
                TimePeriod={'Start': start_date, 'End': end_date},
                Granularity='MONTHLY',
                Metrics=["UnblendedCost"],
            )
        except Exception as e:
            print(f"Error fetching AWS costs: {e}", file=sys.stderr)
            return None
        return float(response['ResultsByTime'][0]['Total']['UnblendedCost']['Amount'])


def fetch_azure_costs(azure_account, start_date, end_date):
    subscription_key = azure_account['Subscription'].upper()  # Convert to uppercase
    minuses_subscription_key = azure_account['Subscription'].replace("_", "-")

    client_id = os.getenv(f"AZURE_{subscription_key}_CLIENT")
    client_secret = os.getenv(f"AZURE_{subscription_key}_SECRET")
    tenant_id = os.getenv(f"AZURE_{subscription_key}_TENANT")

    credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    client = CostManagementClient(credential=credential, subscription_id=azure_account['Subscription'])
    scope = f"/subscriptions/{minuses_subscription_key}"
    query = QueryDefinition(
        type='ActualCost',
        timeframe=TimeframeType.CUSTOM,
        time_period={'from': start_date, 'to': end_date},
        dataset={'granularity': 'Monthly', 'aggregation': {'totalCost': {'name': 'PreTaxCost', 'function': 'Sum'}}}
    )
    try:
        result = client.query.usage(scope, query)
        if result.rows and len(result.rows) > 0:
            cost_value = result.rows[0][0]  # Assuming cost is in the first column of the first row
            return cost_value
        else:
            print("No data available.")
            return None
    except Exception as e:
        print(f"Error fetching Azure costs: {e}", file=sys.stderr)
        return None

def insert_into_db(data, table_name):
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST')
    dbname = os.getenv('POSTGRES_DBNAME')
    sample_date = datetime.today().strftime('%d/%m/%y')  # Current date in dd/mm/yy format

    conn_str = f"dbname={dbname} user={user} password={password} host={host}"
    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {table_name} (accountName, Subscription, monthlyCost, SampleDate) VALUES (%s, %s, %s, %s)",
                    (data['accountName'], data['Subscription'], data['monthlyCost'], sample_date))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting data into database: {e}", file=sys.stderr)

def main():
    config = load_config()
    (aws_start_date, aws_end_date), (azure_start_date, azure_end_date) = get_previous_month_range()
    
    for aws_account in config['aws']:
        aws_cost = fetch_aws_costs(aws_account, aws_start_date, aws_end_date)
        if aws_cost is not None:
            aws_data = {'accountName': aws_account['accountName'], 'Subscription': 'N/A', 'monthlyCost': aws_cost}
            insert_into_db(aws_data, 'aws_monthly_cost')
    
    for azure_account in config['azure']:
        azure_cost = fetch_azure_costs(azure_account, azure_start_date, azure_end_date)
        if azure_cost is not None:
            azure_data = {'accountName': azure_account['accountName'], 'Subscription': azure_account['Subscription'], 'monthlyCost': azure_cost}
            insert_into_db(azure_data, 'azure_monthly_cost')

if __name__ == '__main__':
    main()
