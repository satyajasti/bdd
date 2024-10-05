import pandas as pd
from snowflake_connection import get_snowflake_connection

# Function to get column names from the specified table in Snowflake
def get_columns_from_table(conn, table_name):
    query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}' 
        AND TABLE_SCHEMA = (SELECT CURRENT_SCHEMA())
    """
    cur = conn.cursor()
    cur.execute(query)
    columns = [row[0] for row in cur.fetchall()]
    cur.close()
    return columns

# Function to check columns for NULL values and write to Excel
def check_null_columns(conn, table_name, output_file):
    # Dynamically get column names from the table
    columns = get_columns_from_table(conn, table_name)
    
    cur = conn.cursor()
    null_columns = []

    for column in columns:
        query = f"SELECT COUNT(*) AS total_rows, COUNT({column}) AS non_null_count FROM {table_name}"
        print(f"Executing query: {query}")
        cur.execute(query)
        result = cur.fetchone()
        total_rows = result[0]
        non_null_count = result[1]

        if total_rows > 0 and non_null_count == 0:
            null_columns.append(column)

    # Print the columns that have all NULL values
    print("\nColumns with all NULL values:", null_columns)

    # Write the null columns to an Excel file
    if null_columns:
        df_null_columns = pd.DataFrame(null_columns, columns=['NULL Columns'])
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df_null_columns.to_excel(writer, sheet_name='NULL_Columns', index=False)

        print(f"\nColumns with all NULL values successfully written to {output_file}")
    
    cur.close()

if __name__ == "__main__":
    # Get connection and table name from JSON config
    conn, table_name = get_snowflake_connection('config.json')

    # Output file for null columns
    output_file = 'null_columns.xlsx'
    check_null_columns(conn, table_name, output_file)

    # Close the Snowflake connection
    conn.close()
