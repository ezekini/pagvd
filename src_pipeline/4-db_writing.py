import pandas as pd
from helpers import get_df_from_s3_csv, create_db_connection, insert_df_to_db, clean_db
from constants import s3_const, db_const
from datetime import datetime, timedelta
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()


print("Reading files...")
bucket_name = s3_const.get("bucket_name")
file_name = f'{s3_const.get("transformed_data_folder")}/computed_top_product.csv'
df_top_product = get_df_from_s3_csv(bucket_name, file_name)
file_name = f'{s3_const.get("transformed_data_folder")}/computed_top_ctr.csv'
df_top_ctr = get_df_from_s3_csv(bucket_name, file_name)

# cast date columns as dates
df_top_ctr["date"] = pd.to_datetime(df_top_ctr["date"], format="%Y-%m-%d").dt.date
df_top_product["date"] = pd.to_datetime(
    df_top_product["date"], format="%Y-%m-%d"
).dt.date

print("Filtering by dates missing in DB")
yesterday = datetime.now().date() - timedelta(days=1)


def get_date_from_db_table(schema, table) -> datetime:
    query_dates = f"""SELECT MAX(date) AS maxdate FROM {schema}.{table}"""
    engine = create_db_connection()
    with engine.connect() as connection:
        result = connection.execute(text(query_dates))
        max_date = result.fetchone()[0]
    if max_date is None:  # return an old date if table is empty
        max_date = datetime.strptime("2024-04-01", "%Y-%m-%d").date()
    return max_date


last_db_date_top_ctr = get_date_from_db_table(
    table=db_const.get("top_ctr_table_name"), schema=db_const.get("schema")
)
last_db_date_top_product = get_date_from_db_table(
    table=db_const.get("top_product_table_name"), schema=db_const.get("schema")
)

filtered_df_top_ctr = df_top_ctr[
    (df_top_ctr["date"] > last_db_date_top_ctr) & (df_top_ctr["date"] <= yesterday)
]
filtered_df_top_product = df_top_product[
    (df_top_product["date"] > last_db_date_top_product)
    & (df_top_product["date"] <= yesterday)
]


print("Inserting into DB")
if not filtered_df_top_ctr.empty:
    insert_df_to_db(
        table_name=db_const.get("top_ctr_table_name"),
        schema=db_const.get("schema"),
        data=filtered_df_top_ctr,
    )
if not filtered_df_top_product.empty:
    insert_df_to_db(
        table_name=db_const.get("top_product_table_name"),
        schema=db_const.get("schema"),
        data=filtered_df_top_product,
    )


print("Cleaning DB")
clean_db(
    schema=db_const.get("schema"),
    table=db_const.get("top_ctr_table_name"),
    yesterday=yesterday,
)
clean_db(
    schema=db_const.get("schema"),
    table=db_const.get("top_product_table_name"),
    yesterday=yesterday,
)
