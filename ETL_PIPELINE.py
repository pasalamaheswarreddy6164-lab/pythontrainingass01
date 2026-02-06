import pandas as pd
import os
from datetime import datetime, date
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    account=os.getenv("SF_ACCOUNT"),
    warehouse=os.getenv("SF_WAREHOUSE"),
    database=os.getenv("SF_DATABASE"),
    schema=os.getenv("SF_SCHEMA"),
    role=os.getenv("SF_ROLE")
)

cursor = conn.cursor()

load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  #


csv_df = pd.read_csv("data/source_data.csv")
excel_df = pd.read_excel("data/source_data.xlsx")

csv_df["SOURCE"] = "CSV"
excel_df["SOURCE"] = "EXCEL"

raw_df = pd.concat([csv_df, excel_df], ignore_index=True)

def standardize_gender(val):
    val = str(val).lower().strip()
    if val in ["male", "m"]:
        return "M"
    elif val in ["female", "f"]:
        return "F"
    else:
        return "O"

raw_df["GENDER"] = raw_df["GENDER"].apply(standardize_gender)

raw_df["DOB"] = pd.to_datetime(raw_df["DOB"], errors="coerce") \
                  .dt.strftime("%d-%m-%Y")

raw_df["LOAD_TIMESTAMP"] = load_timestamp

print("\nRAW LAYER DATA")
print(raw_df)

csv_users = raw_df[raw_df["SOURCE"] == "CSV"]
excel_users = raw_df[raw_df["SOURCE"] == "EXCEL"]

joined_df = pd.merge(
    csv_users,
    excel_users,
    on="USER_ID",
    how="inner",
    suffixes=("_CSV", "_EXCEL")
)

print("\nJOINED USER_IDS:", joined_df["USER_ID"].unique())


dob = pd.to_datetime(joined_df["DOB_CSV"], format="%d-%m-%Y")
today = pd.to_datetime(date.today())
joined_df["AGE"] = (today - dob).dt.days // 365

joined_df = joined_df[joined_df["AGE"] > 18]

final_df = pd.DataFrame({
    "USER_ID": joined_df["USER_ID"],
    "NAME": joined_df["NAME_CSV"],
    "GENDER": joined_df["GENDER_CSV"],
    "DOB": joined_df["DOB_CSV"],
    "CITY": joined_df["CITY_CSV"],
    "AGE": joined_df["AGE"],
    "LOAD_TIMESTAMP": load_timestamp
})

print("\nFINAL LAYER DATA")
print(final_df)

cursor.execute("""
CREATE OR REPLACE TABLE RAW_USER_DATA (
    USER_ID INT,
    NAME STRING,
    GENDER STRING,
    DOB STRING,
    CITY STRING,
    SOURCE STRING,
    LOAD_TIMESTAMP TIMESTAMP_NTZ
)
""")

cursor.execute("""
CREATE OR REPLACE TABLE FINAL_USER_DATA (
    USER_ID INT,
    NAME STRING,
    GENDER STRING,
    DOB STRING,
    CITY STRING,
    AGE INT,
    LOAD_TIMESTAMP TIMESTAMP_NTZ
)
""")

write_pandas(conn, raw_df, "RAW_USER_DATA")
write_pandas(conn, final_df, "FINAL_USER_DATA")

print("\n ETL PIPELINE COMPLETED SUCCESSFULLY")

cursor.close()
conn.close()
