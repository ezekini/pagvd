import pandas as pd
from helpers import get_df_from_s3_csv, save_df_to_s3_csv
from constants import s3_const
from dotenv import load_dotenv

load_dotenv()


print("Reading file...")
bucket_name = s3_const.get("bucket_name")
file_name = f'{s3_const.get("transformed_data_folder")}/filtered_product_views.csv'
df_filtered_product_views = get_df_from_s3_csv(bucket_name, file_name)


print("Grouping, sorting, filtering...")
df_grouped = (
    df_filtered_product_views.groupby(["date", "advertiser_id", "product_id"])
    .size()
    .reset_index(name="count")
)
df_grouped_sorted = df_grouped.sort_values(
    by=["date", "advertiser_id", "count"], ascending=[True, True, False]
)


def top_20_rows(group):
    return group.head(20)


result_top_20_per_group = (
    df_grouped_sorted.groupby(["date", "advertiser_id"])
    .apply(top_20_rows)
    .reset_index(drop=True)
)

print("Saving filtered files in S3 bucket")
file_name = f'{s3_const.get("transformed_data_folder")}/computed_top_product.csv'
save_df_to_s3_csv(result_top_20_per_group, bucket_name, file_name)
