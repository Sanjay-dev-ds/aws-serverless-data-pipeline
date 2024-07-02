import boto3
from botocore.exceptions import ClientError
import sys

ATHENA_CLIENT = boto3.client('athena')
GLUE_CLIENT = boto3.client('glue')
MY_DATABASE = 'cse-aspi'
PROD_TABLE_NAME = 'prod_cse_aspi_index_data_table'
PROD_TABLE_S3_BUCKET = 's3://cse-aspi-index-prod-table-data-bucket/'
QUERY_RESULTS_S3_BUCKET = 's3://athena-query-results-output-data-bucket/'
QUERY_STATUS_CODES = ["FAILED", "SUCCEEDED", "CANCELLED"]


def check_table_exists():
    try:
        GLUE_CLIENT.get_table(DatabaseName=MY_DATABASE, Name=PROD_TABLE_NAME)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False
        else:
            raise e  # Re-raise unexpected errors for handling


def create_production_table():
    query = f"""
    CREATE TABLE {PROD_TABLE_NAME} (
        cse_aspi_index_id int,
        cse_aspi_value float,
        cse_aspi_low_value float,
        cse_aspi_high_value float,
        cse_aspi_change float,
        cse_aspi_sector_id int,
        ist_timestamp timestamp
    )
    LOCATION '{PROD_TABLE_S3_BUCKET}'
    TBLPROPERTIES ( 'table_type' = 'ICEBERG' );
    """

    try:
        query_start = ATHENA_CLIENT.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': MY_DATABASE
            },
            ResultConfiguration={'OutputLocation': QUERY_RESULTS_S3_BUCKET}
        )
        response = ATHENA_CLIENT.get_query_execution(QueryExecutionId=query_start["QueryExecutionId"])

        while response["QueryExecution"]["Status"]["State"] not in QUERY_STATUS_CODES:
            response = ATHENA_CLIENT.get_query_execution(QueryExecutionId=query_start["QueryExecutionId"])

        return response
    except ClientError as e:
        print(f"Error creating production table: {e}")
        sys.exit(1)


if __name__ == '__main__':
    if check_table_exists():
        print(f"Table {PROD_TABLE_NAME} already exists")
    else:
        print(f"Creating table {PROD_TABLE_NAME}")
        response = create_production_table()

        if response["QueryExecution"]["Status"]["State"] == 'FAILED':
            sys.exit(response["QueryExecution"]["Status"]["StateChangeReason"])

        print("Table creation successful")
