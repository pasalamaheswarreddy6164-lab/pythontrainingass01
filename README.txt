Assignment: Multi-Source ETL Pipeline

This assignment is about loading data from two files into Snowflake.

There are two input files:
- One CSV file
- One Excel file

Both files contain user details.

First, the data is loaded into a Raw layer.
In the Raw layer:
- CSV and Excel data are combined
- Gender values are cleaned (M, F, O)
- Date of Birth is formatted
- A load timestamp is added
- Data is stored in RAW_USER_DATA table

Next, the Final layer is created.
In the Final layer:
- Only users present in both files are selected
- Age is calculated
- Users older than 18 are kept
- Final data is stored in FINAL_USER_DATA table

The data is loaded into Snowflake using write_pandas.
Tables are created using CREATE OR REPLACE so the program can run many times.

This project shows basic ETL concepts using Python and Snowflake.

End of file
