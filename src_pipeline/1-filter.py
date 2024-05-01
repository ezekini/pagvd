import pandas as pd
from helpers import get_df_from_s3_csv, save_df_to_s3_csv
from constants import s3_const
from dotenv import load_dotenv

load_dotenv()

print("Reading files...")
bucket_name = s3_const.get("bucket_name")
file_name = f'{s3_const.get("raw_data_folder")}/advertiser_ids.csv'
df_active_advertisers = get_df_from_s3_csv(bucket_name, file_name)
file_name = f'{s3_const.get("raw_data_folder")}/ads_views.csv'
df_raw_ads_views = get_df_from_s3_csv(bucket_name, file_name)
file_name = f'{s3_const.get("raw_data_folder")}/product_views.csv'
df_raw_product_views = get_df_from_s3_csv(bucket_name, file_name)

print("Filtering files...")
filtered_ads_views = df_raw_ads_views.merge(
    df_active_advertisers, how="inner", on="advertiser_id"
)
filtered_prod_views = df_raw_product_views.merge(
    df_active_advertisers, how="inner", on="advertiser_id"
)

print("Storing filtered files in S3 bucket")
file_name = f'{s3_const.get("transformed_data_folder")}/filtered_ads_views.csv'
save_df_to_s3_csv(filtered_ads_views, bucket_name, file_name)
file_name = f'{s3_const.get("transformed_data_folder")}/filtered_product_views.csv'
save_df_to_s3_csv(filtered_prod_views, bucket_name, file_name)
