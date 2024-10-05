import pandas as pd
from snowflake_connection import get_snowflake_connection

# Function to check for duplicate records in the entire table
def check_duplicate_records(conn, schema_name, table_name):
    cur = conn.cursor()
    query = f"""
        SELECT *, COUNT(*) AS cnt
        FROM {schema_name}.{table_name}
        GROUP BY *
        HAVING COUNT(*) > 1
    """
    print(f"Executing duplicate records query: {query}")
    cur.execute(query)
    duplicates = cur.fetchall()
    column_names = [desc[0] for desc in cur.description] + ['Count']
    cur.close()

    if duplicates:
        df_duplicates = pd.DataFrame(duplicates, columns=column_names)
        print("\nDuplicate records found in the table:")
        print(df_duplicates)
        return df_duplicates
    else:
        print("\nNo duplicate records found in the table.")
        return None

if __name__ == "__main__":
    # Get connection, schema, and table name from JSON config
    conn, database_name, schema_name, table_name = get_snowflake_connection('config.json')

    # Output file for duplicate records validation
    output_file = 'duplicate_records.xlsx'
    writer = pd.ExcelWriter(output_file, engine='openpyxl')

    # Perform duplicate records validation
    df_duplicates = check_duplicate_records(conn, schema_name, table_name)
    if df_duplicates is not None:
        df_duplicates.to_excel(writer, sheet_name='Duplicate_Records', index=False)

    # Save results to Excel file
    writer.save()
    print(f"\nDuplicate records results successfully written to {output_file}")

    # Close the Snowflake connection
    conn.close()
