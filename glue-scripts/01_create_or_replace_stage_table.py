import sys
import boto3


ATHENA_CLIENT = boto3.client('athena')
GLUE_CLIENT = boto3.client('glue')
MY_DATABASE = 'cse-aspi'

SOURCE_TABLE_NAME = 'cse_aspi_index_raw_data_bucket'

STAGE_TABLE_NAME = 'stage_cse_aspi_index_parquet_data_table'
STAGE_TABLE_S3_BUCKET = 's3://cse-aspi-index-parquet-data-bucket/'


QUERY_RESULTS_S3_BUCKET = 's3://athena-query-results-output-data-bucket/'

QUERY_STATUS_CODES = ["FAILED", "SUCCEEDED", "CANCELLED"]


def create_stage_table():
    queryStart = ATHENA_CLIENT.start_query_execution(
        QueryString=f"""
        CREATE TABLE "{STAGE_TABLE_NAME}" WITH
        (external_location='{STAGE_TABLE_S3_BUCKET}',
        format='PARQUET',
        write_compression='SNAPPY',
        partitioned_by = ARRAY['year', 'month', 'day']
        )
        AS
        SELECT
            id AS cse_aspi_index_id
            ,aspi_value  as cse_aspi_value
            ,lowValue as cse_aspi_low_value
            ,highValue as cse_aspi_high_value
            ,change as cse_aspi_change
            ,sectorId as cse_aspi_sector_id
            ,timestamp as cse_aspi_timestamp
            ,cast(from_unixtime(timestamp / 1000) AT TIME ZONE 'Asia/Kolkata' as timestamp) AS ist_timestamp
            ,year(CAST(from_unixtime(timestamp / 1000) AT TIME ZONE 'Asia/Kolkata' AS timestamp)) AS year
            ,month(CAST(from_unixtime(timestamp / 1000) AT TIME ZONE 'Asia/Kolkata' AS timestamp)) AS month
            ,day(CAST(from_unixtime(timestamp / 1000) AT TIME ZONE 'Asia/Kolkata' AS timestamp)) AS day
        FROM "{MY_DATABASE}"."{SOURCE_TABLE_NAME}"
        ;
        """,
        QueryExecutionContext={
            'Database': f'{MY_DATABASE}'
        },
        ResultConfiguration={'OutputLocation': f'{QUERY_RESULTS_S3_BUCKET}'}
    )

    res = ATHENA_CLIENT.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])

    while res["QueryExecution"]["Status"]["State"] not in QUERY_STATUS_CODES:
        res = ATHENA_CLIENT.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])

    return res


def drop_table_if_exists():
    try:
        GLUE_CLIENT.get_table(DatabaseName=MY_DATABASE, Name=STAGE_TABLE_NAME)
        GLUE_CLIENT.delete_table(DatabaseName=MY_DATABASE, Name=STAGE_TABLE_NAME)
        print(f"Dropped existing table: {STAGE_TABLE_NAME}")
    except GLUE_CLIENT.exceptions.EntityNotFoundException:
        print(f"No existing table found: {STAGE_TABLE_NAME}")


def response_handler(response):
    if response["QueryExecution"]["Status"]["State"] == 'FAILED':
        print(f"Error creating stage table: {response}")
        sys.exit(response["QueryExecution"]["Status"]["StateChangeReason"])
    else:
        print(f"Stage table {STAGE_TABLE_NAME} created successfully")


if __name__ == '__main__':
    drop_table_if_exists()
    response_handler(create_stage_table())
