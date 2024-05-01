import boto3
from os import environ
from sqlalchemy import create_engine, text
import psycopg2
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta


def create_s3():
    s3 = boto3.client(
        "s3",
        region_name="us-west-2",
        aws_access_key_id=environ.get("aws_access_key_id"),
        aws_secret_access_key=environ.get("aws_secret_access_key"),
    )
    return s3


def get_df_from_s3_csv(bucket_name, file_name):
    """
    Gets a CSV file from an S3 bucket and returns a DataFrame
    """
    s3 = create_s3()
    file_object = s3.get_object(Bucket=bucket_name, Key=file_name)
    df = pd.read_csv(file_object["Body"])
    return df


def save_df_to_s3_csv(df, bucket_name, file_name):
    """
    Saves a DataFrame to a CSV in an S3 bucket.
    """
    s3 = create_s3()
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())


def create_db_connection():
    """
    Function that creates engine to db,
    """
    server = environ["DB_SERVER"]
    database = environ["DB_NAME"]
    user = environ["DB_USER"]
    password = environ["DB_PASS"]
    port = environ["DB_PORT"]

    # Create the SQLAlchemy engine
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{server}:{port}/{database}"
    )
    return engine


def insert_df_to_db(table_name, schema, data):
    print("inserting data...")
    engine = create_db_connection()
    df_to_insert = data
    print("Inserting {} values".format(len(df_to_insert)))
    df_to_insert.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=10000,
    )


def clean_db(schema, table, yesterday: datetime, days_to_leave_in_db=7):
    date_limit_str = datetime.strftime(
        yesterday - timedelta(days=days_to_leave_in_db), "%Y-%m-%d"
    )
    query_delete = f"""
        DELETE FROM {schema}.{table} where date <= '{date_limit_str}'
        """
    engine = create_db_connection()
    with engine.connect() as connection:
        connection.execute(text(query_delete))
        connection.commit()
