import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import tabula
import requests
import boto3
from io import StringIO


class DataExtractor:
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def read_rds_table(self, cls, db_creds, db, table_name):
        engine = cls.init_db_engine(db_creds, db)
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            table = pd.read_sql_table(table_name, engine)
            return table
        
    def retrieve_pdf_data(self, pdf_url):
        tables = tabula.read_pdf(pdf_url, pages='all', multiple_tables=True)

        if tables:
            for i, table in enumerate(tables):
                print(f"Table {i + 1}")
                print(table)
                
                # Save the table to a DataFrame
                df = pd.DataFrame(table)

                # Print the DataFrame
                print(df)
                return df
        else:
            print("No tables found in the PDF.")   

    def list_number_of_stores(self, no_stores_endpoint, headers_dict):
        payload = {}

        response = requests.request("GET", no_stores_endpoint, headers=headers_dict, data=payload)

        response_dict = response.json()
        return response_dict["number_stores"]

    def retrieve_stores_data(self, store_endpoint, headers_dict, no_of_stores):
        payload = {}
        stores_dict = []
        for i in range(0, no_of_stores-1):
            url = f"{store_endpoint}/{i}"
            response = requests.request("GET", url, headers=headers_dict, data=payload)
            response_dict = response.json()
            stores_dict.append(response_dict)
        df = pd.DataFrame(stores_dict)
        print(df.head())
        return df

    def extract_from_s3(self, s3_address):
        # Parse the S3 address
        bucket_name, key = self.parse_s3_address(s3_address)
        
        # Download the CSV file from S3
        csv_obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        
        # Convert the CSV string to a pandas DataFrame
        df = pd.read_csv(StringIO(csv_string))
        
        return df
    
    @staticmethod
    def parse_s3_address(s3_address):
        if s3_address.startswith('s3://'):
            s3_address = s3_address[len('s3://'):]
        bucket_name, key = s3_address.split('/', 1)
        return bucket_name, key
#         Step 1:
# Create a method in DataExtractor called extract_from_s3 which uses the boto3 package to download and extract the information returning a pandas DataFrame.

# The S3 address for the products data is the following s3://data-handling-public/products.csv the method will take this address in as an argument and return the pandas DataFrame.


# You will need to be logged into the AWS CLI before you retrieve the data from the bucket.
         
    def extract_json_from_s3(self, url):
        # Send a GET request to the URL
        response = requests.get(url)
        
        # Raise an HTTPError if the HTTP request returned an unsuccessful status code
        response.raise_for_status()
        
        # Parse the JSON content
        data = response.json()
        
        # Convert the JSON data to a pandas DataFrame
        df = pd.DataFrame(data)
        
        return df


if __name__ == "__main__":
    connector = DatabaseConnector()
    cleaner = DataCleaning()
    extractor = DataExtractor()

    # # dim_card_details #
    # #card data extract and clean 
    # pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # card_df = extractor.retrieve_pdf_data(pdf_url)
    # cleaned_card_df = cleaner.clean_card_data(card_df)

    # # dim_store_details #
    # #Store data extract and clean
    # no_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    # headers = {
    #         'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    #         }
    # stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"

    # no_of_stores = extractor.list_number_of_stores(no_stores_endpoint, headers)
    # stores_df = extractor.retrieve_stores_data(stores_endpoint, headers, no_of_stores)
    # cleaned_stores_df = cleaner.clean_store_data(stores_df)


    #products
    # Convert product weights to kg
    # s3_address = 's3://data-handling-public/products.csv'
    # extractor = DataExtractor()
    # products_df = extractor.extract_from_s3(s3_address)
    # print(products_df.head())
    # products_df = cleaner.convert_product_weights(products_df)
    # print("After converting weights to kg:")
    # print(products_df)
    
    # # Clean the products data
    # cleaned_products_df = cleaner.clean_products_data(products_df)
    # print("After cleaning the data:")
    # print(cleaned_products_df)



    # orders 
    # connector.list_db_tables('db_creds.yaml', 'RDS')
    # # ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
    
    # orders_df = extractor.read_rds_table(connector,'db_creds.yaml', 'RDS', 'orders_table')
    # print(orders_df.head())
    # cleaned_orders_df = cleaner.clean_orders_data(orders_df)
    # print(cleaned_orders_df.head())

    # date_time data

    # URL of the JSON file on S3
    url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    
    # Create an instance of DataExtractor
    extractor = DataExtractor()
    
    # Extract the JSON data and convert it to a DataFrame
    dates_df = extractor.extract_json_from_s3(url)
    cleaned_dates_df = cleaner.clean_date_times_data(dates_df)
    





    #upload
    pg_connector = DatabaseConnector()
    # connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_card_df, 'dim_card_details')
    # connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_stores_df, 'dim_store_details')
    # connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_products_df, 'dim_products')
    # pg_connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_orders_df, 'orders_table')
    connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_dates_df, 'dim_date_times')
    pg_connector.list_db_tables('pg_db_creds.yaml', 'PG')



