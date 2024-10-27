import io
import requests
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def yesterday_date():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

with DAG(
    dag_id="market_etl",
    start_date=datetime(2024, 6, 24, 6, 17),
    schedule_interval="15 6 * * *",  # Runs daily at 06:15 UTC, which is 12:00 NPT
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:
    
    @task()
    def hit_polygon_api(**context):
        date_yesterday = yesterday_date()
        url = f"https://6066-43-245-85-164.ngrok-free.app/stocks_data_date?date={date_yesterday}"
        
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

        return flattened_dataframe

    @task
    def get_business_date(df):
        business_date = df['Close_Date'].iloc[0]  # Take the date from the first row
        return business_date.date()  # Convert to string
    
    @task
    def create_fact_table(df):

        fact_table = df[['Symbol', 'Close_Date', 'Total_Traded_Quantity', 'Close_Price_Rs', 'LTP', 'Total_Trades', 'Average_Traded_Price_Rs']].copy()
        fact_table['SECURITY_STOCK_ID'] = fact_table.apply(lambda x: f"S_ID_{x['Symbol']}_{x['Close_Date'].date()}", axis=1)
        fact_table['PRICE_ID'] = fact_table.apply(lambda x: f"P_ID_{x['Symbol']}_{x['Close_Date'].date()}", axis=1)
        fact_table['LTP'] = fact_table['LTP'].str.replace(',', '', regex=True)
        fact_table.rename(columns={
            'Symbol': 'SECURITY_ID', 
            'Close_Date': 'BUSINESS_DATE', 
            'Close_Price_Rs': 'CLOSE_PRICE', 
            'LTP': 'LAST_UPDATED_PRICE', 
            'Total_Traded_Quantity': 'TOTAL_TRADED_QUANTITY', 
            'Average_Traded_Price_Rs': 'AVERAGE_TRADED_PRICE'
        }, inplace=True)
        return fact_table

    @task
    def create_security_dimension(df, business_date):

        security_dimension = df[['Symbol', 'Market_Capitalization_Rs__Amt_in_Millions', 'Fifty_Two_Week_High_Rs', 'Fifty_Two_Week_Low_Rs']].drop_duplicates().copy()
        security_dimension['SECURITY_STOCK_ID'] = security_dimension.apply(lambda x: f"S_ID_{x['Symbol']}_{business_date}", axis=1)
        security_dimension.rename(columns={
            'Symbol': 'SECURITY_ID', 
            'Market_Capitalization_Rs__Amt_in_Millions': 'MARKET_CAPITALIZATION', 
            'Fifty_Two_Week_High_Rs': 'FIFTY_TWO_WEEKS_HIGH', 
            'Fifty_Two_Week_Low_Rs': 'FIFTY_TWO_WEEKS_LOW'
        }, inplace=True)
        return security_dimension
    
    @task
    def create_date_dimension(df):

        date_dimension = df[['Close_Date']].drop_duplicates().copy()
        date_dimension['YEAR'] = date_dimension['Close_Date'].dt.year
        date_dimension['MONTH'] = date_dimension['Close_Date'].dt.month
        date_dimension['WEEK'] = date_dimension['Close_Date'].dt.isocalendar().week
        date_dimension['DAY'] = date_dimension['Close_Date'].dt.day
        date_dimension.rename(columns={'Close_Date': 'BUSINESS_DATE'}, inplace=True)
        date_dimension['BUSINESS_DATE'] = date_dimension['BUSINESS_DATE'].dt.date.astype(str)  # Convert to string without time
        return date_dimension

    @task
    def create_price_dimension(df, business_date):
        df = pd.DataFrame(df)  # Ensure df is a DataFrame
        price_dimension = df[['Symbol', 'Open_Price_Rs', 'High_Price_Rs', 'Low_Price_Rs', 'Close_Price_Rs', 'LTP', 'Previous_Day_Close_Price_Rs']].drop_duplicates().copy()
        price_dimension['PRICE_ID'] = price_dimension.apply(lambda x: f"P_ID_{x['Symbol']}_{business_date}", axis=1)
        price_dimension['LTP'] = price_dimension['LTP'].str.replace(',', '', regex=True)
        price_dimension.rename(columns={'Symbol': 'SECURITY_ID', 'Open_Price_Rs': 'OPEN_PRICE', 'High_Price_Rs': 'HIGH_PRICE', 'Low_Price_Rs': 'LOW_PRICE', 'Close_Price_Rs': 'CLOSE_PRICE', 'LTP': 'LAST_UPDATED_PRICE', 'Previous_Day_Close_Price_Rs': 'PREVIOUS_DAY_PRICE'}, inplace=True)
        return price_dimension

    @task
    def upload_to_s3(dataframe, bucket_name, file_path, **context):
        date_yesterday = yesterday_date()
        s3_hook = S3Hook(aws_conn_id="aws_default")
        csv_buffer = io.StringIO()
        dataframe.to_csv(csv_buffer, index=False)

        # Modify the file path to include the execution date
        dynamic_file_path = f"{file_path}_{date_yesterday}.csv"

        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            bucket_name=bucket_name,
            key=dynamic_file_path,
            replace=True
        )
        return dynamic_file_path

    api_response = hit_polygon_api()
    flattened_dataframe = flatten_market_data(api_response)
    business_date = get_business_date(flattened_dataframe)
    fact_table = create_fact_table(flattened_dataframe)
    security_dimension = create_security_dimension(flattened_dataframe, business_date)
    date_dimension = create_date_dimension(flattened_dataframe)
    price_dimension = create_price_dimension(flattened_dataframe, business_date)
    
    upload_fact_table = upload_to_s3(fact_table, 'nepse-fact-table', 'nepse_fact_table')
    upload_security_dimension = upload_to_s3(security_dimension, 'nepse-security-dimension', 'nepse_security_dimension')
    upload_date_dimension = upload_to_s3(date_dimension, 'nepse-date-dimension', 'nepse_date_dimension')
    upload_price_dimension = upload_to_s3(price_dimension, 'nepse-price-dimension', 'nepse_price_dimension')

    fact_table >> upload_fact_table
    security_dimension >> upload_security_dimension
    date_dimension >> upload_date_dimension
    price_dimension >> upload_price_dimension

    


