import pandas as pd
from snowflake_connection import get_snowflake_connection, read_connection_details

# Function to validate data patterns (e.g., email, date)
def validate_data_patterns(conn, schema_name, table_name, data_pattern_checks):
    cur = conn.cursor()
    results = []

    for column, pattern in data_pattern_checks.items():
        query = f"""
            SELECT {column}
            FROM {schema_name}.{table_name}
            WHERE {column} IS NOT NULL AND {column} NOT REGEXP '{pattern}'
        """
        print(f"Executing data pattern validation query for column '{column}': {query}")
        cur.execute(query)
        invalid_data = cur.fetchall()

        # Add to results if there are invalid entries
        if invalid_data:
            results.append((column, invalid_data))

    cur.close()
    
    if results:
        print("\nData pattern validation errors found:")
        for column, invalid_data in results:
            df_invalid = pd.DataFrame(invalid_data, columns=[column])
            print(f"\nInvalid data for column '{column}':")
            print(df_invalid)
        return results
    else:
        print("\nNo data pattern validation errors found.")
        return None

if __name__ == "__main__":
    # Get connection, schema, and table name from JSON config
    conn, database_name, schema_name, table_name = get_snowflake_connection('config.json')

    # Read data pattern checks from config.json
    config = read_connection_details('config.json')
    data_pattern_checks = config.get('data_pattern_checks', {})

    # Output file for pattern validation
    output_file = 'pattern_validation.xlsx'
    writer = pd.ExcelWriter(output_file, engine='openpyxl')

    # Perform data pattern validation
    pattern_errors = validate_data_patterns(conn, schema_name, table_name, data_pattern_checks)
    if pattern_errors:
        for column, invalid_data in pattern_errors:
            df_invalid = pd.DataFrame(invalid_data, columns=[column])
            df_invalid.to_excel(writer, sheet_name=f'{column}_Pattern_Errors', index=False)

    # Save results to Excel file
    writer.save()
    print(f"\nPattern validation results successfully written to {output_file}")

    # Close the Snowflake connection
    conn.close()
