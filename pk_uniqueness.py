import pandas as pd
from snowflake_connection import get_snowflake_connection, read_connection_details

# Function to check for primary key uniqueness
def check_primary_key_uniqueness(conn, schema_name, table_name, primary_keys):
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

# Main function
def main():
    # Get connection, schema, and table name from JSON config
    conn, database_name, schema_name, table_name = get_snowflake_connection('config.json')

    # Read primary keys from config.json
    config = read_connection_details('config.json')
    primary_keys = config.get('primary_keys', [])

    # Validate primary key uniqueness
    if primary_keys:
        df_pk_duplicates = check_primary_key_uniqueness(conn, schema_name, table_name, primary_keys)
        
        if df_pk_duplicates is not None:
            output_file = 'pk_uniqueness_results.xlsx'
            df_pk_duplicates.to_excel(output_file, sheet_name='PK_Uniqueness', index=False)
            print(f"\nPrimary key uniqueness results successfully written to {output_file}")

    # Close the Snowflake connection
    conn.close()

if __name__ == "__main__":
    main()
