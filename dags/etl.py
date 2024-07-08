from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import requests
import pandas as pd
import io


def yesterday_date():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


with DAG(
    dag_id="market_etl",
    start_date=datetime(2024, 6, 24, 6, 17),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:
    @task()
    def hit_polygon_api(**context):
        #stock_ticker = "AMZN"
        #polygon_api_key = "cHZwKLb2pCvHxpOk4nA4ozx09ODT0rKz"
        date_yesterday = yesterday_date()
        
        # Create the URL for aggregate data for the current date

        url = f"https://05c6-110-44-115-243.ngrok-free.app/stocks_data_date?date=2024-07-04"
        response = requests.get(url)

        response_data = response.json()

        # Check if data is returned from the API
        if 'data' not in response_data or not response_data['data']:
            # Log the lack of data and raise an exception to skip further task execution
            print("No data received from API. Skipping subsequent tasks.")
            raise AirflowSkipException("No data received from API.")

        return response_data

  
    @task
    def flatten_market_data(api_response, **context):
        if 'data' not in api_response:
            raise ValueError("No data found in the API response")

        data = api_response['data']
        flattened_dataframe = pd.DataFrame(data)

        # Convert 'Close_Date' to datetime if needed (assuming 'Close_Date' holds the date information)
        flattened_dataframe['Close_Date'] = pd.to_datetime(flattened_dataframe['Close_Date'])

        return flattened_dataframe


    @task
    def load_market_data(flattened_dataframe):
        market_database_hook = PostgresHook("market_database_conn")
        market_database_conn = market_database_hook.get_sqlalchemy_engine()

        # Exclude 'Sn' column if it's not needed
        if 'Sn' in flattened_dataframe.columns:
            flattened_dataframe = flattened_dataframe.drop(columns=['Sn'])

        flattened_dataframe.to_sql(
            name="nepse_market_data",
            con=market_database_conn,
            if_exists="append",
            index=False
        )
        return flattened_dataframe


    # @task
    # def upload_to_s3(flattened_dataframe, bucket_name, file_path, **context):
    #     s3_hook = S3Hook(aws_conn_id="aws_default")
    #     csv_buffer = io.StringIO()
    #     flattened_dataframe.to_csv(csv_buffer, index=False)

    #     # Retrieve the execution date from the context
    #     execution_date = context['ds']  # Format 'YYYY-MM-DD'
    #     # Modify the file path to include the execution date
    #     dynamic_file_path = f"{file_path}_{execution_date}.csv"

    #     s3_hook.load_string(
    #         string_data=csv_buffer.getvalue(),
    #         bucket_name=bucket_name,
    #         key=dynamic_file_path,
    #         replace=True
    #     )
    #     return dynamic_file_path

    # @task
    # def extract_positive_ltp_records(dynamic_file_path, source_bucket_name, **context):
    #     s3_hook = S3Hook(aws_conn_id="aws_default")
    #     obj = s3_hook.read_key(key=dynamic_file_path, bucket_name=source_bucket_name)
    #     df = pd.read_csv(io.StringIO(obj))
    #     df = df.dropna()  # Remove rows with null values
    #     df = df[~df['LTP'].astype(str).str.contains('-')]  # Filter out rows where 'LTP' contains '-'
    #     csv_buffer = io.StringIO()
    #     df.to_csv(csv_buffer, index=False)
    #     csv_buffer.seek(0)  # Rewind the buffer
    #     return csv_buffer.getvalue()

    # @task
    # def upload_positive_ltp_records(csv_data, target_bucket_name, file_path, **context):
    #     s3_hook = S3Hook(aws_conn_id="aws_default")
    #     new_file_path = f"{file_path}.csv"
    #     s3_hook.load_string(
    #         string_data=csv_data, 
    #         bucket_name=target_bucket_name, 
    #         key=new_file_path, 
    #         replace=True
    #     )

    # @task
    # def extract_negative_ltp_records(dynamic_file_path, source_bucket_name, **context):
    #     s3_hook = S3Hook(aws_conn_id="aws_default")
    #     obj = s3_hook.read_key(key=dynamic_file_path, bucket_name=source_bucket_name)
    #     df = pd.read_csv(io.StringIO(obj))
    #     negative_ltp_df = df[df['LTP'].astype(str).str.contains('-')]  # Extract rows where 'LTP' contains '-'
    #     csv_buffer = io.StringIO()
    #     negative_ltp_df.to_csv(csv_buffer, index=False)
    #     csv_buffer.seek(0)  # Rewind the buffer
    #     return csv_buffer.getvalue()

    # @task
    # def upload_negative_ltp_records(csv_data, target_bucket_name, file_path, **context):
    #     s3_hook = S3Hook(aws_conn_id="aws_default")
    #     new_file_path = f"{file_path}"
    #     s3_hook.load_string(string_data=csv_data, bucket_name=target_bucket_name, key=new_file_path, replace=True)

    # Task sequence
    raw_market_data = hit_polygon_api()
    transformed_market_data = flatten_market_data(raw_market_data)
    load_market_data(transformed_market_data)
    # market_data = load_market_data(transformed_market_data)
    # file_path = upload_to_s3(market_data, bucket_name='nepse-source', file_path='nepse')
    # transformed_csv_positive_data = extract_positive_ltp_records(file_path, source_bucket_name='nepse-source')
    # upload_positive_ltp_records(transformed_csv_positive_data, target_bucket_name='nepse-positive', file_path=file_path)
    # negative_csv_data = extract_negative_ltp_records(file_path, source_bucket_name='nepse-source')
    # upload_negative_ltp_records(negative_csv_data, target_bucket_name='nepse-negative', file_path=file_path)


