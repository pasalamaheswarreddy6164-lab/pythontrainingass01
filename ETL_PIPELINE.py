import pandas as pd
import os
from datetime import datetime, date
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

con = snowflake.connector.connect(
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    account=os.getenv("SF_ACCOUNT"),
    warehouse=os.getenv("SF_WAREHOUSE"),
    database=os.getenv("SF_DATABASE"),
    schema=os.getenv("SF_SCHEMA"),
    role=os.getenv("SF_ROLE")
)

cur = con.cursor()

# Get current time
ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Read files
d1 = pd.read_csv("data/employee.csv")
d2 = pd.read_excel("data/employee.xlsx")

d1["SOURCE"] = "CSV"
d2["SOURCE"] = "EXCEL"

data = pd.concat([d1, d2], ignore_index=True)

def fix_gen(x):
    s = str(x).lower().strip()
    if s in ["male", "m"]:
        return "M"
    elif s in ["female", "f"]:
        return "F"
    else:
        return "O"

# Clean columns
data["GENDER"] = data["GENDER"].apply(fix_gen)
data["DOB"] = pd.to_datetime(data["DOB"], errors="coerce").dt.strftime("%d-%m-%Y")
data["LOAD_TIMESTAMP"] = ts

print("\n--- RAW DATA ---")
print(data)

set_a = data[data["SOURCE"] == "CSV"]
set_b = data[data["SOURCE"] == "EXCEL"]

merged = pd.merge(
    set_a, 
    set_b, 
    on="USER_ID", 
    how="inner", 
    suffixes=("_CSV", "_EXCEL")
)

print("\nIDs FOUND:", merged["USER_ID"].unique())

bday = pd.to_datetime(merged["DOB_CSV"], format="%d-%m-%Y")
now = pd.to_datetime(date.today())
merged["AGE"] = (now - bday).dt.days // 365

merged = merged[merged["AGE"] > 18]

res = pd.DataFrame({
    "USER_ID": merged["USER_ID"],
    "NAME": merged["NAME_CSV"],
    "GENDER": merged["GENDER_CSV"],
    "DOB": merged["DOB_CSV"],
    "CITY": merged["CITY_CSV"],
    "AGE": merged["AGE"],
    "LOAD_TIMESTAMP": ts
})

print("\n--- FINAL DATA ---")
print(res)

cur.execute("""
CREATE OR REPLACE TABLE RAW_EMPLOYEE_DATA (
    USER_ID INT,
    NAME STRING,
    GENDER STRING,
    DOB STRING,
    CITY STRING,
    SOURCE STRING,
    LOAD_TIMESTAMP TIMESTAMP_NTZ
)
""")

cur.execute("""
CREATE OR REPLACE TABLE FINAL_EMPLOYEE_DATA (
    USER_ID INT,
    NAME STRING,
    GENDER STRING,
    DOB STRING,
    CITY STRING,
    AGE INT,
    LOAD_TIMESTAMP TIMESTAMP_NTZ
)
""")

write_pandas(con, data, "RAW_EMPLOYEE_DATA")
write_pandas(con, res, "FINAL_EMPLOYEE_DATA")

print("\nDONE.")

cur.close()
con.close()