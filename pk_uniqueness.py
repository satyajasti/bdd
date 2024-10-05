import pandas as pd
from snowflake_connection import get_snowflake_connection, get_primary_keys

# Function to check for primary key uniqueness
def check_primary_key_uniqueness(conn, schema_name, table_name):
    # Get primary keys dynamically
    primary_keys = get_primary_keys(conn, schema_name, table_name)
    
    if not primary_keys:
        print(f"No primary key defined for table {table_name}.")
        return None

    cur = conn.cursor()
    pk_columns = ', '.join(primary_keys)
    query = f"""
        SELECT {pk_columns}, COUNT(*) AS cnt
        FROM {schema_name}.{table_name}
        GROUP BY {pk_columns}
        HAVING COUNT(*) > 1
    """
    print(f"Executing primary key uniqueness query: {query}")
    cur.execute(query)
    duplicates = cur.fetchall()
    cur.close()

    # Process the results
    if duplicates:
        df_duplicates = pd.DataFrame(duplicates, columns=primary_keys + ['Count'])
        print("\nDuplicate primary key records found:")
        print(df_duplicates)
        return df_duplicates
    else:
        print("\nNo duplicate primary key records found.")
        return None

if __name__ == "__main__":
    # Get connection, schema, and table name from JSON config
    conn, database_name, schema_name, table_name = get_snowflake_connection('config.json')

    # Output file for primary key uniqueness validation
    output_file = 'pk_uniqueness.xlsx'
    writer = pd.ExcelWriter(output_file, engine='openpyxl')

    # Perform primary key uniqueness validation
    df_pk_duplicates = check_primary_key_uniqueness(conn, schema_name, table_name)
    if df_pk_duplicates is not None:
        df_pk_duplicates.to_excel(writer, sheet_name='PK_Uniqueness', index=False)

    # Save results to Excel file
    writer.save()
    print(f"\nPrimary key uniqueness results successfully written to {output_file}")

    # Close the Snowflake connection
    conn.close()
