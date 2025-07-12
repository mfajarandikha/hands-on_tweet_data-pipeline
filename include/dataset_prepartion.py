import pandas as pd
from sqlalchemy import create_engine
import os

folder_path = "/opt/airflow/dags/data"
csv_file = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

if not csv_file:
    raise FileNotFoundError("No CSV file found in the folder.")

file_path = os.path.join(folder_path, csv_file[0])
df = pd.read_csv(file_path)
essential_columns = [
    "date",
    "text",
    "user_location",
    "source",
    "hashtags",
]
existing_essentials = [col for col in essential_columns if col in df.columns]
df_essential = df[existing_essentials]
df_essential["date"] = pd.to_datetime(df_essential["date"], errors="coerce")
df_essential = df_essential.dropna(subset=["date", "text"])
df_essential = df_essential.drop_duplicates(subset=["text"])

db_url = "postgresql+psycopg2://dest_user:dest_pass@destination_postgres:5432/dest_db"
engine = create_engine(db_url)
TABLE_NAME = "final_covid_tweets"

try:
    df_essential.to_sql(name=TABLE_NAME, con=engine, index=False, if_exists="replace")
    print(f"Inserted {len(df)} rows into table '{TABLE_NAME}'")
except Exception as e:
    print("Error during insertion:", e)
