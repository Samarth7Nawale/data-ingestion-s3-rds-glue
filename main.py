import boto3
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import os
import logging
import sys

# Logging setup
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# Environment Variables
s3_bucket = os.getenv('S3_BUCKET')
csv_key = os.getenv('CSV_KEY')
rds_host = os.getenv('RDS_HOST')
rds_user = os.getenv('RDS_USER')
rds_pass = os.getenv('RDS_PASS')
rds_db = os.getenv('RDS_DB')
rds_table = os.getenv('RDS_TABLE')
glue_db = os.getenv('GLUE_DB')
glue_table = os.getenv('GLUE_TABLE')
glue_s3_location = os.getenv('GLUE_S3_LOCATION')


def read_csv_from_s3():
    try:
        logging.info(f"Reading file '{csv_key}' from bucket '{s3_bucket}'...")
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=s3_bucket, Key=csv_key)
        df = pd.read_csv(obj['Body'])
        logging.info("CSV file read successfully from S3.")
        return df
    except Exception as e:
        logging.error(f"Failed to read from S3: {e}")
        raise


def push_to_rds(df):
    try:
        logging.info("Connecting to RDS...")
        engine = create_engine(f'mysql+pymysql://{rds_user}:{rds_pass}@{rds_host}/{rds_db}')
        logging.info(f"Pushing data to RDS table '{rds_table}'...")
        df.to_sql(rds_table, engine, if_exists='replace', index=False)
        logging.info("Data successfully pushed to RDS.")
        return True
    except Exception as e:
        logging.error(f"Failed to upload data to RDS: {e}")
        return False


def fallback_to_glue():
    try:
        logging.info("Falling back to AWS Glue...")
        glue = boto3.client('glue')

        glue.create_table(
            DatabaseName=glue_db,
            TableInput={
                'Name': glue_table,
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'name', 'Type': 'string'},
                        {'Name': 'age', 'Type': 'int'}
                    ],
                    'Location': glue_s3_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {'field.delim': ','}
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        logging.info("Glue fallback succeeded — table created.")
    except Exception as e:
        logging.error(f"Fallback to Glue failed: {e}")


def main():
    try:
        df = read_csv_from_s3()
        success = push_to_rds(df)
        if not success:
            fallback_to_glue()
    except Exception as e:
        logging.error(f"Fatal error in pipeline: {e}")


if __name__ == '__main__':
    main()
