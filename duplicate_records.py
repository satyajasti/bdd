import snowflake.connector
import pandas as pd
from snowflake_connection import get_snowflake_connection

# Function to retrieve column names from the specified table
def get_columns_from_table(conn, schema_name, table_name):
    query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
        AND TABLE_NAME = '{table_name}'
    """
    cur = conn.cursor()
    cur.execute(query)
    columns = [row[0] for row in cur.fetchall()]
    cur.close()
    return columns

# Function to check for duplicate records in the entire table
def check_duplicate_records(conn, schema_name, table_name):
    # Get all column names
    columns = get_columns_from_table(conn, schema_name, table_name)
    
    # Build the GROUP BY clause dynamically
    group_by_clause = ', '.join(columns)

    # Construct the query
    query = f"""
        SELECT *, COUNT(*) AS cnt
        FROM {schema_name}.{table_name}
        GROUP BY {group_by_clause}
        HAVING COUNT(*) > 1
    """

    print(f"Executing duplicate records query: {query}")
    cur = conn.cursor()
    cur.execute(query)
    duplicates = cur.fetchall()
    cur.close()

    # Fetch column names for the result (include count)
    column_names = columns + ['cnt']

    # Process the result
    if duplicates:
        df_duplicates = pd.DataFrame(duplicates, columns=column_names)
        print("\nDuplicate records found in the table:")
        print(df_duplicates)
        return df_duplicates
    else:
        print("\nNo duplicate records found in the table.")
        return None

# Main function to run the process and write results to Excel
def main():
    # Get connection, schema, and table name from JSON config
    conn, database_name, schema_name, table_name = get_snowflake_connection('config.json')

    # Check for duplicate records
    df_duplicates = check_duplicate_records(conn, schema_name, table_name)

    # Write the DataFrame to an Excel file if duplicates are found
    if df_duplicates is not None:
        output_file = 'duplicate_records_report.xlsx'
        df_duplicates.to_excel(output_file, sheet_name='Duplicate_Records', index=False)
        print(f"\nDuplicate records report successfully written to {output_file}")

    # Close the Snowflake connection
    conn.close()

if __name__ == "__main__":
    main()
