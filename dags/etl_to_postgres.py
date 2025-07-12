from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile

##
KAGGLE_DATASETS = ["gpreda/covid19-tweets"]
DOWNLOAD_PATH = "/opt/airflow/dags/data"


def download_kaggle_dataset():
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    api = KaggleApi()
    api.authenticate()

    # Download only the first dataset in the list
    dataset_name = KAGGLE_DATASETS[0]
    print(f"Downloading dataset: {dataset_name}")
    api.dataset_download_files(dataset_name, path=DOWNLOAD_PATH, unzip=False)


def unzip_dataset():
    zip_files = [f for f in os.listdir(DOWNLOAD_PATH) if f.endswith(".zip")]
    for zip_file in zip_files:
        zip_path = os.path.join(DOWNLOAD_PATH, zip_file)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(DOWNLOAD_PATH)
        print(f"Unzipped {zip_file}")


def list_downloaded_files():
    print("Files in download path:")
    for f in os.listdir(DOWNLOAD_PATH):
        print(f)


with DAG(
    dag_id="kaggle_download_and_merge_dag",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["kaggle", "merge", "bash"],
) as dag:

    download_task = PythonOperator(
        task_id="download_kaggle_dataset",
        python_callable=download_kaggle_dataset,
    )

    unzip_task = PythonOperator(
        task_id="unzip_dataset",
        python_callable=unzip_dataset,
    )

    run_script = BashOperator(
        task_id="run_ingest_clean_tweets",
        bash_command="/usr/bin/env python /opt/airflow/include/dataset_prep.py",
    )

    download_task >> unzip_task >> run_script
