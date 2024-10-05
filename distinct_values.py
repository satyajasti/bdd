import pandas as pd
from snowflake_connection import get_snowflake_connection

# Function to get column names from the specified table in Snowflake
def get_columns_from_table(conn, schema_name, table_name):
    query = f"""
        SELECT COLUMN_NAME 
        FROM {schema_name}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}' 
        AND TABLE_SCHEMA = '{schema_name}'
    """
    cur = conn.cursor()
    cur.execute(query)
    columns = [row[0] for row in cur.fetchall()]
    cur.close()
    
    return columns

# Function to fetch distinct values for columns and write to Excel
def fetch_distinct_values(conn, schema_name, table_name, columns, output_file):
    cur = conn.cursor()
    data_list = []

    for column in columns:
        # Get distinct values, limit to 100
        query = f"SELECT DISTINCT {column} FROM {schema_name}.{table_name} LIMIT 100"
        print(f"Executing query: {query}")
        cur.execute(query)
        data = cur.fetchall()

        # Add the column header to the list and collect up to 100 distinct values
        data_list.append([f'Column "{column}":', None])  # Column name in A, empty in B
        for row in data:
            data_list.append([None, row[0]])  # None in A, value in B

        # Add an empty row for separation
        data_list.append([None, None])

    # Create a DataFrame from the list
    df_combined = pd.DataFrame(data_list, columns=['Column_Name', 'Distinct_Values'])

    # Print the DataFrame to the console
    print("\nDistinct values for each column:")
    print(df_combined)

    # Write the data to an Excel file
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df_combined.to_excel(writer, sheet_name='Distinct_Values', index=False)

    print(f"\nDistinct values successfully written to {output_file}")
    cur.close()

if __name__ == "__main__":
    # Get connection, schema, and table name from JSON config
    conn, database_name, schema_name, table_name = get_snowflake_connection('config.json')

    # Dynamically get columns from the table
    columns = get_columns_from_table(conn, schema_name, table_name)

    # Output file for distinct values
    output_file = 'formatted_distinct_values.xlsx'
    fetch_distinct_values(conn, schema_name, table_name, columns, output_file)

    # Close the Snowflake connection
    conn.close()
