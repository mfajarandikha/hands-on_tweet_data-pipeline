from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile

# Define constants
KAGGLE_DATASET = [
    "gpreda/pfizer-vaccine-tweets",
    "gpreda/covid19-tweets",
]  # change this to your dataset
DOWNLOAD_PATH = "/opt/airflow/dags/data"  # mounted in DAGs directory


def download_kaggle_dataset():
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    for x in KAGGLE_DATASET:
        print(f"Downloading dataset: {x}")
        api.dataset_download_files(x, path=DOWNLOAD_PATH, unzip=False)


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
    dag_id="kaggle_download_dag",
    schedule="@daily",  # run manually for now
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["kaggle", "download"],
) as dag:

    download_task = PythonOperator(
        task_id="download_kaggle_dataset",
        python_callable=download_kaggle_dataset,
    )

    unzip_task = PythonOperator(
        task_id="unzip_dataset",
        python_callable=unzip_dataset,
    )

    list_files_task = PythonOperator(
        task_id="list_files",
        python_callable=list_downloaded_files,
    )

    download_task >> unzip_task >> list_files_task
