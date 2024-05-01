import pandas as pd
from helpers import get_df_from_s3_csv, save_df_to_s3_csv
from constants import s3_const
from dotenv import load_dotenv

load_dotenv()

print("Reading file...")
bucket_name = s3_const.get("bucket_name")
file_name = f'{s3_const.get("transformed_data_folder")}/filtered_ads_views.csv'
df_filtered_ads_views = get_df_from_s3_csv(bucket_name, file_name)

print("Grouping, sorting, filtering...")
df_grouped = (
    df_filtered_ads_views.groupby(["date", "advertiser_id", "product_id"])["type"]
    .value_counts()
    .unstack(fill_value=0)
)
df_grouped["ctr"] = df_grouped["click"] / df_grouped["impression"]


def sort_and_filter(group):
    return group.sort_values("ctr", ascending=False).head(20)


result = df_grouped.groupby(
    ["date", "advertiser_id", "product_id"], group_keys=False
).apply(sort_and_filter)
result = result.reset_index()

print("Storing computed file in S3 bucket")
file_name = f'{s3_const.get("transformed_data_folder")}/computed_top_ctr.csv'
save_df_to_s3_csv(result, bucket_name, file_name)
