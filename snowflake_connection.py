import snowflake.connector
import json

# Function to read connection details from a JSON file
def read_connection_details(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

# Function to create a Snowflake connection
def get_snowflake_connection(config_file):
    config = read_connection_details(config_file)
    snowflake_details = config['snowflake']
    
    # Create the Snowflake connection
    conn = snowflake.connector.connect(
        user=snowflake_details.get('user'),
        password=snowflake_details.get('password'),
        account=snowflake_details.get('account'),
        warehouse=snowflake_details.get('warehouse'),
        database=snowflake_details.get('database'),
        schema=snowflake_details.get('schema')
    )
    
    return conn, snowflake_details.get('database'), snowflake_details.get('schema'), config.get('table_name')

# Function to fetch primary key columns dynamically from Snowflake
def get_primary_keys(conn, schema_name, table_name):
    query = f"""
        SELECT column_name
        FROM {schema_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE table_name = '{table_name}'
        AND table_schema = '{schema_name}'
        AND constraint_name IN (
            SELECT constraint_name 
            FROM {schema_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE table_name = '{table_name}' 
            AND table_schema = '{schema_name}'
            AND constraint_type = 'PRIMARY KEY'
        )
    """
    cur = conn.cursor()
    cur.execute(query)
    primary_keys = [row[0] for row in cur.fetchall()]
    cur.close()
    return primary_keys
