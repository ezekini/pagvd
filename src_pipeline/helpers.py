import boto3
from os import environ
from sqlalchemy import create_engine, text, Table, MetaData, insert
import psycopg2
import psycopg2.extras as extras
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from math import ceil


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


def create_db_connection_psycopg2():
    """Create a connection to the PostgreSQL database."""
    server = environ["DB_SERVER"]
    database = environ["DB_NAME"]
    user = environ["DB_USER"]
    password = environ["DB_PASS"]
    port = environ["DB_PORT"]
    conn = psycopg2.connect(
        host=server, database=database, user=user, password=password, port=port
    )
    return conn


def insert_df_to_db(data, table_name, schema):

    conn = create_db_connection_psycopg2()
    tuples = [tuple(x) for x in data.to_numpy()]
    cols = ",".join(list(data.columns))
    # SQL query to execute
    table = schema + "." + table_name
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


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
