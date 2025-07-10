from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile

KAGGLE_DATASETS = [
    "gpreda/pfizer-vaccine-tweets",
    "gpreda/covid19-tweets",
    "kaushiksuresh147/covidvaccine-tweets",
]
DOWNLOAD_PATH = "/opt/airflow/dags/data"


def download_kaggle_dataset():
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    api = KaggleApi()
    api.authenticate()

    for dataset in KAGGLE_DATASETS:
        print(f"Downloading dataset: {dataset}")
        api.dataset_download_files(dataset, path=DOWNLOAD_PATH, unzip=False)


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

    list_files_task = PythonOperator(
        task_id="list_files",
        python_callable=list_downloaded_files,
    )

    merge_csv_task = BashOperator(
        task_id="merge_csv_with_bash",
        bash_command=(
            f"cd {DOWNLOAD_PATH} && "
            "first_file=$(ls *.csv | head -n 1) && "
            "head -n 1 $first_file > merged.csv && "
            'for f in *.csv; do tail -n +2 "$f" >> merged.csv; done && '
            "echo 'CSV files merged into merged.csv'"
        ),
    )

    download_task >> unzip_task >> list_files_task >> merge_csv_task
