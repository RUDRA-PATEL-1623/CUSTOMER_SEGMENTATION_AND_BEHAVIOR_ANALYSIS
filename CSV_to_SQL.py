import pandas as pd
import mysql.connector
import os

# List of CSV Files & Their Corresponding Table Names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('geolocation.csv', 'geolocation'),
    ('order_items.csv', 'order_items'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('payments.csv', 'payments')  # Added payments.csv for Specific Handling
]

# Connect to The MySQL Database
conn = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='RdP@170904',
    database='customer_segmentation_and_behavior_analysis',
    port=3307
)
cursor = conn.cursor()

# Folder Containing The CSV Files
folder_path = 'E:\Projects\End to End Data Analysis Projects\Customer_Segmentation_And_Behavior_Analysis\Dataset'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read The CSV File Into a Pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Replace NaN With None to Handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check For NaN Values
    print(f"Processing {csv_file}")
    print(f"NaN Values Before Replacement : \n{df.isnull().sum()}\n")

    # Clean Column Names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate The CREATE TABLE Statement With Appropriate Data Types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame Data Into The MySQL Table
    for _, row in df.iterrows():
        # Convert Row to Tuple & Handle NaN / None Explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit The Transaction for The Current CSV File
    conn.commit()

# Close The Connection
conn.close()