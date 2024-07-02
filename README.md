# serverless-data-pipeline

## Description
This project implements a serverless data pipeline for extracting data from the Colombo Stock Market ASI Index API. The pipeline uses an AWS Lambda function to fetch the data and load it into an Amazon Kinesis Firehose Delivery Stream. The Firehose stream then writes the data to an Amazon S3 bucket, buffering the data either for 5 minutes or until it reaches 5KB in size.

Once the data is stored in S3, an event notification is sent to an Amazon SQS queue. Then on-demand AWS Glue workflow job is triggered, where an AWS Glue Crawler is executed to extract the data from the S3 bucket and create an Amazon Athena table. After the raw data is processed by the Glue Crawler, a subsequent Glue bash script transforms the data and loads it into a staging table.

Finally, an ETL job creates an Apache Iceberg table from the staging table using a MERGE operation, completing the data processing pipeline.


## Technologies Used
- AWS Glue
- AWS Lambda
- Amazon Athena
- Amazon S3
- Amazon SQS
- Amazon Kinesis Firehose

## Prerequisites
- Serverless Framework — Used as Infrastructure as Code
- Python
- AWS Cloud account

## Architecture
![architecture-serverless-etl.png](images%2Farchitecture-serverless-etl.png)

## Steps to Deploy the Project

1. Clone the repository
2. Install the Serverless Framework
    ```bash
    npm install -g serverless
    ```
3. Configure the AWS credentials
    ```bash
    serverless config credentials --provider aws --key <AWS_ACCESS_KEY_ID> --secret <AWS_SECRET_ACCESS_KEY>
    ```
4. Deploy the project 
    ```bash
    sls deploy
    ```
    This will create Lambda Function, S3 Bucket, SQS Queue, and Firehose Delivery Stream.
5. Create Glue workflow (based on the scripts inside the glue-script folder) and Crawler.
6. Test the project!

## Author
Sanjay Jayakumar
