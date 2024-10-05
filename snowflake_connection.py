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
    
    conn = snowflake.connector.connect(
        user=snowflake_details.get('user'),
        password=snowflake_details.get('password'),
        account=snowflake_details.get('account'),
        warehouse=snowflake_details.get('warehouse'),
        database=snowflake_details.get('database'),
        schema=snowflake_details.get('schema')
    )
    return conn, config['table_name']
