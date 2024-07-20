import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import tabula
import requests
import boto3
from io import StringIO


class DataExtractor:
    """Class for extracting data from various sources including PDF, S3, and APIs."""

    def __init__(self):
        """Initialize DataExtractor with an S3 client."""
        self.s3_client = boto3.client('s3')

    def read_rds_table(self, cls, db_creds, db, table_name):
        """
        Read a table from an RDS database into a DataFrame.

        Args:
            cls: The class used to initialize the database engine.
            db_creds: Path to database credentials file.
            db: Database name.
            table_name: Name of the table to read.

        Returns:
            pd.DataFrame: DataFrame containing the table data.
        """
        engine = cls.init_db_engine(db_creds, db)
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            table = pd.read_sql_table(table_name, engine)
            return table

    def retrieve_pdf_data(self, pdf_url):
        """
        Retrieve data from a PDF and combine tables into a single DataFrame.

        Args:
            pdf_url: URL of the PDF containing tables.

        Returns:
            pd.DataFrame: Combined DataFrame of all tables extracted from the PDF.
        """
        tables = tabula.read_pdf(pdf_url, pages='all', multiple_tables=True)

        if tables:
            all_dfs = []
            for i, table in enumerate(tables):
                print(f"Table {i + 1}")
                print(table)

                # Save the table to a DataFrame
                df = pd.DataFrame(table)
                all_dfs.append(df)

            # Concatenate all DataFrames if they have the same columns
            combined_df = pd.concat(all_dfs, ignore_index=True)
            print("Combined DataFrame:")
            print(combined_df)
            return combined_df
        else:
            print("No tables found in the PDF.")
            return pd.DataFrame()  # Return an empty DataFrame if no tables are found

    def list_number_of_stores(self, no_stores_endpoint, headers_dict):
        """
        Retrieve the number of stores from an API endpoint.

        Args:
            no_stores_endpoint: URL of the API endpoint.
            headers_dict: Dictionary containing request headers.

        Returns:
            int: Number of stores.
        """
        payload = {}
        response = requests.get(no_stores_endpoint, headers=headers_dict, data=payload)
        response_dict = response.json()
        return response_dict["number_stores"]

    def retrieve_stores_data(self, store_endpoint, headers_dict, no_of_stores):
        """
        Retrieve store data from an API endpoint.

        Args:
            store_endpoint: URL of the API endpoint.
            headers_dict: Dictionary containing request headers.
            no_of_stores: Number of stores to retrieve.

        Returns:
            pd.DataFrame: DataFrame containing store data.
        """
        payload = {}
        stores_dict = []
        for i in range(no_of_stores):
            url = f"{store_endpoint}/{i}"
            response = requests.get(url, headers=headers_dict, data=payload)
            response_dict = response.json()
            stores_dict.append(response_dict)

        df = pd.DataFrame(stores_dict)
        print(df.head())
        return df

    def extract_from_s3(self, s3_address):
        """
        Extract data from a CSV file stored in S3.

        Args:
            s3_address: S3 address of the CSV file (e.g., 's3://bucket/key').

        Returns:
            pd.DataFrame: DataFrame containing the CSV data.
        """
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
        """
        Parse the bucket name and key from an S3 address.

        Args:
            s3_address: S3 address (e.g., 's3://bucket/key').

        Returns:
            tuple: (bucket_name, key)
        """
        if s3_address.startswith('s3://'):
            s3_address = s3_address[len('s3://'):]
        bucket_name, key = s3_address.split('/', 1)
        return bucket_name, key

    def extract_json_from_s3(self, url):
        """
        Extract JSON data from a URL and convert it to a DataFrame.

        Args:
            url: URL to fetch JSON data from.

        Returns:
            pd.DataFrame: DataFrame containing the JSON data.
        """
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        return df


if __name__ == "__main__":
    connector = DatabaseConnector()
    cleaner = DataCleaning()
    extractor = DataExtractor()

    # Extract and clean card data
    pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_df = extractor.retrieve_pdf_data(pdf_url)
    cleaned_card_df = cleaner.clean_card_data(card_df)


    # Extract and clean store data
    # # Uncomment and update the following for store data extraction and cleaning
    # no_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    # headers = {
    #     'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    # }
    # stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
    # no_of_stores = extractor.list_number_of_stores(no_stores_endpoint, headers)
    # stores_df = extractor.retrieve_stores_data(stores_endpoint, headers, no_of_stores)
    # cleaned_stores_df = cleaner.clean_store_data(stores_df)


    # Extract and clean products data
    # # Uncomment and update the following for products data extraction and cleaning
    # s3_address = 's3://data-handling-public/products.csv'
    # products_df = extractor.extract_from_s3(s3_address)
    # products_df = cleaner.convert_product_weights(products_df)
    # print(products_df.head())
    # cleaned_products_df = cleaner.clean_products_data(products_df)
    # print(cleaned_products_df.head())

    # Extract and clean orders data
    # # Uncomment and update the following for orders data extraction and cleaning
    # orders_df = extractor.read_rds_table(connector, 'db_creds.yaml', 'RDS', 'orders_table')
    # cleaned_orders_df = cleaner.clean_orders_data(orders_df)

    # Extract and clean users data
    # # Uncomment and update the following for users data extraction and cleaning
    # users_df = extractor.read_rds_table(connector, 'db_creds.yaml', 'RDS', 'legacy_users')
    # print(users_df.head())
    # filtered_df = users_df[users_df['date_of_birth'].notna() & ~users_df['date_of_birth'].str.match(r'^\d{4}-\d{2}-\d{2}$')]
    # print(filtered_df)
    # cleaned_users_df = cleaner.clean_user_data(users_df)

    # Extract and clean date data
    # # Uncomment and update the following for date-time data extraction and cleaning
    # url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    # dates_df = extractor.extract_json_from_s3(url)
    # cleaned_dates_df = cleaner.clean_date_times_data(dates_df)

    # Upload cleaned data to the database
    pg_connector = DatabaseConnector()
    # pg_connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_card_df, 'dim_card_details')
    # pg_connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_users_df, 'dim_users')
    # pg_connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_stores_df, 'dim_store_details')
    # pg_connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_products_df, 'dim_products')
    # pg_connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_orders_df, 'orders_table')
    # pg_connector.upload_to_db('pg_db_creds.yaml', 'PG', cleaned_dates_df, 'dim_date_times')
    pg_connector.list_db_tables('pg_db_creds.yaml', 'PG')
