import sys
import boto3
import awswrangler as wr

ATHENA_CLIENT = wr.athena
GLUE_CLIENT = boto3.client('glue')
MY_DATABASE = 'cse-aspi'

STAGE_TABLE_NAME = 'stage_cse_aspi_index_parquet_data_table'

QUERY_RESULTS_S3_BUCKET = 's3://athena-query-results-output-data-bucket/'

QUERY_STATUS_CODES = ["FAILED", "SUCCEEDED", "CANCELLED"]


def data_quality_evaluator() -> None:
    NULL_DQ_CHECK = f"""
    SELECT 
      COUNT(CASE WHEN cse_aspi_index_id IS NULL THEN 1 END) +
      COUNT(CASE WHEN cse_aspi_value IS NULL THEN 1 END) +
      COUNT(CASE WHEN cse_aspi_low_value IS NULL THEN 1 END) +
      COUNT(CASE WHEN cse_aspi_high_value IS NULL THEN 1 END) +
      COUNT(CASE WHEN cse_aspi_change IS NULL THEN 1 END) +
      COUNT(CASE WHEN cse_aspi_sector_id IS NULL THEN 1 END) +
      COUNT(CASE WHEN cse_aspi_timestamp IS NULL THEN 1 END) +
      COUNT(CASE WHEN ist_timestamp IS NULL THEN 1 END) AS total_null_values
    FROM {STAGE_TABLE_NAME};
    """

    df = ATHENA_CLIENT.read_sql_query(sql=NULL_DQ_CHECK, database=MY_DATABASE)

    # exit if we get a result > 0
    # else, the check was successful
    if df['total_null_values'][0] > 0:
        sys.exit('Results returned. Quality check failed.')
    else:
        print('Quality check passed.')


if __name__ == '__main__':
    data_quality_evaluator()
