import pandas as pd
from snowflake_connection import get_snowflake_connection

# Function to read column names from a text file
def read_columns_from_file(file_path):
    with open(file_path, 'r') as file:
        columns = [line.strip() for line in file.readlines() if line.strip()]
    return columns

# Function to fetch distinct values for columns and write to Excel
def fetch_distinct_values(conn, table_name, columns, output_file):
    cur = conn.cursor()
    data_list = []

    for column in columns:
        query = f"SELECT DISTINCT {column} FROM {table_name}"
        print(f"Executing query: {query}")
        cur.execute(query)
        data = cur.fetchall()

        # Add the column header to the list
        data_list.append([f'Column "{column}":'])
        
        # Add the distinct values to the list
        for row in data:
            data_list.append([row[0]])
        
        # Add an empty row for separation
        data_list.append([''])

    # Create a DataFrame from the list
    df_combined = pd.DataFrame(data_list, columns=['Distinct_Values'])

    # Print the DataFrame to the console
    print("\nDistinct values for each column:")
    print(df_combined)

    # Write the data to an Excel file
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df_combined.to_excel(writer, sheet_name='Distinct_Values', index=False)

    print(f"\nDistinct values successfully written to {output_file}")
    cur.close()

if __name__ == "__main__":
    # Get connection and table name from JSON config
    conn, table_name = get_snowflake_connection('config.json')
    
    # Path to the text file containing column names
    column_file_path = 'columns.txt'
    columns = read_columns_from_file(column_file_path)

    # Output file for distinct values
    output_file = 'formatted_distinct_values.xlsx'
    fetch_distinct_values(conn, table_name, columns, output_file)

    # Close the Snowflake connection
    conn.close()
