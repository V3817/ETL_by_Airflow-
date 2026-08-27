from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import logging

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nasa_apod_postgres',
    default_args=default_args,
    description='A DAG to extract NASA APOD data and load into PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['nasa', 'api', 'postgres'],
) as dag:
    
    # Step 1: Create the table if it does not exist
    @task
    def create_table():
        """Create the APOD data table in PostgreSQL"""
        try:
            # Initialize the PostgresHook
            postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
            
            # SQL query to create the table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS apod_data (
                id SERIAL PRIMARY KEY,
                title VARCHAR(500),
                explanation TEXT,
                url TEXT,
                hdurl TEXT,
                date DATE UNIQUE,
                media_type VARCHAR(50),
                copyright VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Execute the table creation query
            postgres_hook.run(create_table_query)
            logging.info("Table created successfully or already exists")
            return "Table creation completed"
            
        except Exception as e:
            logging.error(f"Error creating table: {str(e)}")
            raise

    # Step 2: Extract the NASA API Data (APOD)
    extract_apod = SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',  # Connection ID defined in Airflow for NASA API
        endpoint='planetary/apod',  # NASA API endpoint for APOD
        method='GET',
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},  # Use API key from connection
        headers={"Content-Type": "application/json"},
        xcom_push=True,
        dag=dag
    )

    # Step 3: Transform the data
    @task
    def transform_apod_data(task_instance):
        """Transform the APOD API response"""
        try:
            # Get the response from the previous task
            response = task_instance.xcom_pull(task_ids='extract_apod')
            
            if not response:
                raise ValueError("No data received from API")
            
            logging.info(f"Raw API response: {response}")
            
            # Transform the data
            apod_data = {
                'title': response.get('title', ''),
                'explanation': response.get('explanation', ''),
                'url': response.get('url', ''),
                'hdurl': response.get('hdurl', ''),
                'date': response.get('date', ''),
                'media_type': response.get('media_type', ''),
                'copyright': response.get('copyright', '')
            }
            
            logging.info(f"Transformed data: {apod_data}")
            return apod_data
            
        except Exception as e:
            logging.error(f"Error transforming data: {str(e)}")
            raise

    # Step 4: Load the data into PostgreSQL
    @task
    def load_data_to_postgres(apod_data):
        """Load transformed data into PostgreSQL"""
        try:
            # Initialize the PostgresHook
            postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
            
            # Check if record already exists for this date
            check_query = "SELECT COUNT(*) FROM apod_data WHERE date = %s"
            result = postgres_hook.get_first(check_query, parameters=(apod_data['date'],))
            
            if result[0] > 0:
                logging.info(f"Record for date {apod_data['date']} already exists. Skipping insert.")
                return "Record already exists"
            
            # Define the SQL Insert Query
            insert_query = """
            INSERT INTO apod_data (title, explanation, url, hdurl, date, media_type, copyright)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            
            # Execute the SQL Query
            postgres_hook.run(insert_query, parameters=(
                apod_data['title'],
                apod_data['explanation'],
                apod_data['url'],
                apod_data['hdurl'],
                apod_data['date'],
                apod_data['media_type'],
                apod_data['copyright']
            ))
            
            logging.info(f"Data loaded successfully for date: {apod_data['date']}")
            return "Data loaded successfully"
            
        except Exception as e:
            logging.error(f"Error loading data: {str(e)}")
            raise

    # Step 5: Verification task
    @task
    def verify_data_load():
        """Verify that data was loaded correctly"""
        try:
            postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
            
            # Get count of records
            count_query = "SELECT COUNT(*) FROM apod_data"
            count_result = postgres_hook.get_first(count_query)
            
            # Get latest record
            latest_query = "SELECT title, date FROM apod_data ORDER BY date DESC LIMIT 1"
            latest_result = postgres_hook.get_first(latest_query)
            
            logging.info(f"Total records in table: {count_result[0]}")
            if latest_result:
                logging.info(f"Latest record: {latest_result[0]} for date {latest_result[1]}")
            
            return f"Verification complete. Total records: {count_result[0]}"
            
        except Exception as e:
            logging.error(f"Error during verification: {str(e)}")
            raise

    # Define the task dependencies
    table_created = create_table()
    api_response = extract_apod
    transformed_data = transform_apod_data()
    data_loaded = load_data_to_postgres(transformed_data)
    verification = verify_data_load()
    
    # Set the task flow
    table_created >> api_response >> transformed_data >> data_loaded >> verification