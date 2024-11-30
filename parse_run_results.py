import psycopg2
import json

# Function to insert rows into the database
def insert_rows_into_db(rows):
    # Connect to your postgres DB
    conn = psycopg2.connect(
        dbname="mydatabase",  # replace with your database name
        user="admin",       # replace with your username
        password="adminpassword", # replace with your password
        host="127.0.0.1",       # replace with your host
        port=5432        # replace with your port
    )
    cursor = conn.cursor()

    # SQL query to insert data
    insert_query = """
    INSERT INTO SRC_DBT_RUN_LOGS (
        resource_type,
        resource_name,
        run_status,
        resource_compiled,
        compiled_code,
        compilation_started_at,
        compilation_completed_at,
        execution_started_at,
        execution_completed_at,
        execution_time,
        failures
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Insert each row into the database
    cursor.executemany(insert_query, rows)
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()


# Function to process results from the JSON file and prepare data for insertion
def run_results_json():
    with open("dbt_transformation/target/run_results.json", mode="r") as f:
        data = json.loads(f.read())['results']
    
    rows_to_insert = []
    
    for result in data:
        resource_type = result['unique_id'].split('.')[0]  # Assuming the resource_type is the first part of the unique_id
        
        if resource_type == 'test':
            resource_name = result['unique_id'].split('.')[-2]  # Assuming the resource_name is the last part of the unique_id
        else:
            resource_name = result['unique_id'].split('.')[-1]
            
        run_status = result['status']
        resource_compiled = result['compiled']
        compiled_code = result.get('compiled_code', '')

        # Extracting timing information
        compile_timing = next((t for t in result['timing'] if t['name'] == 'compile'), {})
        execution_timing = next((t for t in result['timing'] if t['name'] == 'execute'), {})

        compilation_started_at = compile_timing.get('started_at')
        compilation_completed_at = compile_timing.get('completed_at')
        execution_started_at = execution_timing.get('started_at')
        execution_completed_at = execution_timing.get('completed_at')
        execution_time = result['execution_time']

        # Extracting the number of failures
        failures = result.get('failures', 0)  # Default to 0 if 'failures' is not present


        # Creating a tuple with the extracted data
        row = (
            resource_type,
            resource_name,
            run_status,
            resource_compiled,
            compiled_code,
            compilation_started_at,
            compilation_completed_at,
            execution_started_at,
            execution_completed_at,
            execution_time,
            failures
        )

        rows_to_insert.append(row)

    # Insert the prepared rows into the database
    insert_rows_into_db(rows_to_insert)


def main():
    run_results_json()


if __name__ == '__main__':
    main()
