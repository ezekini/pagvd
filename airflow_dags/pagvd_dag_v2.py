import os
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

dag_dir = os.path.dirname(os.path.realpath(__file__))
scripts_folder_path = os.path.join(dag_dir, "..", "src_pipeline")

with DAG(
    dag_id="pagvd",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(seconds=300),
        "trigger_rule": "all_success",
    },
    schedule="05 16 * * *",
    start_date=datetime(2024, 4, 30),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    filter_advertisers = BashOperator(
        task_id="filter",
        bash_command=f'python3 {os.path.join(scripts_folder_path, "1-filter.py")}',
    )

    compute_top_ctr_model = BashOperator(
        task_id="top_ctr",
        bash_command=f'python3 {os.path.join(scripts_folder_path, "2-compute_top_ctr.py")}',
    )

    compute_top_product_model = BashOperator(
        task_id="top_products",
        bash_command=f'python3 {os.path.join(scripts_folder_path, "3-compute_top_product.py")}',
    )

    load_models_to_db = BashOperator(
        task_id="load_models_to_DB",
        bash_command=f'python3 {os.path.join(scripts_folder_path, "4-db_writing.py")}',
    )

    (
        filter_advertisers
        >> [compute_top_ctr_model, compute_top_product_model]
        >> load_models_to_db
    )
