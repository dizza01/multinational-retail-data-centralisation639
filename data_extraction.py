import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import tabula
import requests

class DataExtractor:
    
    def read_rds_table(self, cls, table_name):
        engine = cls.init_db_engine()
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

    # Create another method retrieve_stores_data which will take the retrieve a store endpoint as an argument 
    # and extracts all the stores from the API saving them in a pandas DataFrame.
        

# The two endpoints for the API are as follows:

# Retrieve a store: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}
# Return the number of stores: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores
        
  
    #         Step 2:
#   Create a method in your DataExtractor class called retrieve_pdf_data, which takes in a link as an argument and returns a pandas DataFrame.
#   Use the tabula-py Python package, imported with tabula to extract all pages from the pdf document at following link .
#   Then return a DataFrame of the extracted data.
    
   



pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
extractor = DataExtractor()
# df = extractor.retrieve_pdf_data(pdf_url)


# connector = DatabaseConnector()
# cleaner = DataCleaning()
# cleaned_df = cleaner.clean_card_data(df)


no_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
headers = {
        'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        }
stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"

no_of_stores = extractor.list_number_of_stores(no_stores_endpoint, headers)

df = extractor.retrieve_stores_data(stores_endpoint, headers, no_of_stores)
print(df)