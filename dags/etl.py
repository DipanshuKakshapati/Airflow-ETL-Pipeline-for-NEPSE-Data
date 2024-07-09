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
        date_yesterday = yesterday_date()
        url = f"https://d504-110-34-1-227.ngrok-free.app/stocks_data_date?date={date_yesterday}"
        response = requests.get(url)
        response_data = response.json()

        if 'data' not in response_data or not response_data['data']:
            print("No data received from API. Skipping subsequent tasks.")
            raise AirflowSkipException("No data received from API.")
        
        return response_data

    @task
    def flatten_market_data(api_response, **context):
        if 'data' not in api_response:
            raise ValueError("No data found in the API response")

        data = api_response['data']
        flattened_dataframe = pd.DataFrame(data)
        flattened_dataframe['Close_Date'] = pd.to_datetime(flattened_dataframe['Close_Date'])

        # print(flattened_dataframe.columns)
        # print(flattened_dataframe.head())

        return flattened_dataframe

    @task
    def create_fact_table(df):
        fact_table = df[['Symbol', 'Close_Date', 'Total_Traded_Quantity', 'Close_Price_Rs', 'LTP', 'Total_Trades', 'Average_Traded_Price_Rs']].copy()
        fact_table['PRICE_ID'] = fact_table.apply(lambda x: f"{x['Symbol']}_{x['Close_Date']}", axis=1)
        fact_table.rename(columns={'Symbol': 'SECURITY_ID', 'Close_Date': 'BUSINESS_DATE', 'Close_Price_Rs': 'CLOSE_PRICE', 'LTP': 'LAST_UPDATED_PRICE', 'Total_Traded_Quantity': 'TOTAL_TRADED_QUANTITY', 'Average_Traded_Price_Rs': 'AVERAGE_TRADED_PRICE'}, inplace=True)
        return fact_table

    @task
    def create_security_dimension(df):
        security_dimension = df[['Symbol', 'Market_Capitalization_Rs__Amt_in_Millions', 'Fifty_Two_Week_High_Rs', 'Fifty_Two_Week_Low_Rs']].drop_duplicates().copy()
        security_dimension.rename(columns={'Symbol': 'SECURITY_ID', 'Market_Capitalization_Rs__Amt_in_Millions': 'MARKET_CAPITALIZATION', 'Fifty_Two_Week_High_Rs': 'FIFTY_TWO_WEEKS_HIGH', 'Fifty_Two_Week_Low_Rs': 'FIFTY_TWO_WEEKS_LOW'}, inplace=True)
        return security_dimension

    @task
    def create_date_dimension(df):
        date_dimension = df[['Close_Date']].drop_duplicates().copy()
        date_dimension['YEAR'] = date_dimension['Close_Date'].dt.year
        date_dimension['MONTH'] = date_dimension['Close_Date'].dt.month
        date_dimension['WEEK'] = date_dimension['Close_Date'].dt.isocalendar().week
        date_dimension['DAY'] = date_dimension['Close_Date'].dt.day
        date_dimension.rename(columns={'Close_Date': 'BUSINESS_DATE'}, inplace=True)
        return date_dimension

    @task
    def create_price_dimension(df):
        price_dimension = df[['Symbol', 'Open_Price_Rs', 'High_Price_Rs', 'Low_Price_Rs', 'Close_Price_Rs', 'LTP', 'Previous_Day_Close_Price_Rs', 'Close_Date']].drop_duplicates().copy()
        price_dimension['PRICE_ID'] = price_dimension.apply(lambda x: f"{x['Symbol']}_{x['Close_Date']}", axis=1)
        price_dimension.rename(columns={'Symbol': 'SECURITY_ID', 'Open_Price_Rs': 'OPEN_PRICE', 'High_Price_Rs': 'HIGH_PRICE', 'Low_Price_Rs': 'LOW_PRICE', 'Close_Price_Rs': 'CLOSE_PRICE', 'LTP': 'LAST_UPDATED_PRICE', 'Previous_Day_Close_Price_Rs': 'PREVIOUS_DAY_PRICE'}, inplace=True)
        return price_dimension

    @task
    def load_data_to_postgres(fact_table, security_dimension, date_dimension, price_dimension):
        market_database_hook = PostgresHook("market_database_conn")
        market_database_conn = market_database_hook.get_sqlalchemy_engine()

        fact_table.to_sql(
            name="fact_table",
            con=market_database_conn,
            if_exists="append",
            index=False
        )
        security_dimension.to_sql(
            name="security_dimension",
            con=market_database_conn,
            if_exists="append",
            index=False
        )
        date_dimension.to_sql(
            name="date_dimension",
            con=market_database_conn,
            if_exists="append",
            index=False
        )
        price_dimension.to_sql(
            name="price_dimension",
            con=market_database_conn,
            if_exists="append",
            index=False
        )

    api_response = hit_polygon_api()
    flattened_dataframe = flatten_market_data(api_response)

    fact_table = create_fact_table(flattened_dataframe)
    security_dimension = create_security_dimension(flattened_dataframe)
    date_dimension = create_date_dimension(flattened_dataframe)
    price_dimension = create_price_dimension(flattened_dataframe)

    load_data_to_postgres(fact_table, security_dimension, date_dimension, price_dimension)

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
    # raw_market_data = hit_polygon_api()
    # transformed_market_data = flatten_market_data(raw_market_data)
    # load_market_data(transformed_market_data)
    # market_data = load_market_data(transformed_market_data)
    # file_path = upload_to_s3(market_data, bucket_name='nepse-source', file_path='nepse')
    # transformed_csv_positive_data = extract_positive_ltp_records(file_path, source_bucket_name='nepse-source')
    # upload_positive_ltp_records(transformed_csv_positive_data, target_bucket_name='nepse-positive', file_path=file_path)
    # negative_csv_data = extract_negative_ltp_records(file_path, source_bucket_name='nepse-source')
    # upload_negative_ltp_records(negative_csv_data, target_bucket_name='nepse-negative', file_path=file_path)


