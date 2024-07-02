import boto3
from botocore.exceptions import ClientError
import sys

ATHENA_CLIENT = boto3.client('athena')
GLUE_CLIENT = boto3.client('glue')
MY_DATABASE = 'cse-aspi'

PROD_TABLE_NAME = 'prod_cse_aspi_index_data_table'
PROD_TABLE_S3_BUCKET = 's3://cse-aspi-index-prod-table-data-bucket/'

STAGE_TABLE_NAME = 'stage_cse_aspi_index_parquet_data_table'
STAGE_TABLE_S3_BUCKET = 's3://cse-aspi-index-parquet-data-bucket/'


QUERY_RESULTS_S3_BUCKET = 's3://athena-query-results-output-data-bucket/'
QUERY_STATUS_CODES = ["FAILED", "SUCCEEDED", "CANCELLED"]


def merge_data_operation():
    query = f"""
        MERGE INTO {PROD_TABLE_NAME} AS target
        USING (
            -- Select the unique records with the latest timestamp
            WITH LatestRecords AS (
                SELECT
                    cse_aspi_index_id,
                    cse_aspi_value,
                    cse_aspi_low_value,
                    cse_aspi_high_value,
                    cse_aspi_change,
                    cse_aspi_sector_id,
                    CAST(ist_timestamp AS TIMESTAMP(6)) AS ist_timestamp,
                    ROW_NUMBER() OVER (PARTITION BY cse_aspi_index_id ORDER BY cse_aspi_timestamp DESC) AS rn
                FROM {STAGE_TABLE_NAME}
            )
            SELECT
                cse_aspi_index_id,
                cse_aspi_value,
                cse_aspi_low_value,
                cse_aspi_high_value,
                cse_aspi_change,
                cse_aspi_sector_id,
                ist_timestamp
            FROM LatestRecords
            WHERE rn = 1
        ) AS source
        ON target.cse_aspi_index_id = source.cse_aspi_index_id
        WHEN MATCHED THEN
            UPDATE SET
                cse_aspi_value = source.cse_aspi_value,
                cse_aspi_low_value = source.cse_aspi_low_value,
                cse_aspi_high_value = source.cse_aspi_high_value,
                cse_aspi_change = source.cse_aspi_change,
                cse_aspi_sector_id = source.cse_aspi_sector_id,
                ist_timestamp = source.ist_timestamp
        WHEN NOT MATCHED THEN
            INSERT (
                cse_aspi_index_id,
                cse_aspi_value,
                cse_aspi_low_value,
                cse_aspi_high_value,
                cse_aspi_change,
                cse_aspi_sector_id,
                ist_timestamp
            ) VALUES (
                source.cse_aspi_index_id,
                source.cse_aspi_value,
                source.cse_aspi_low_value,
                source.cse_aspi_high_value,
                source.cse_aspi_change,
                source.cse_aspi_sector_id,
                source.ist_timestamp
            );
        ;
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

    response = merge_data_operation()

    if response["QueryExecution"]["Status"]["State"] == 'FAILED':
        sys.exit(response["QueryExecution"]["Status"]["StateChangeReason"])

    print(f"Table {PROD_TABLE_NAME} Refreshed successfully")
