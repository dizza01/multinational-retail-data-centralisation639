import pandas as pd
import database_utils
import tabula
import camelot
from PyPDF2 import PdfReader

class DataExtractor:
    
    def read_rds_table(self, cls, table_name):
        engine = cls.init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            table = pd.read_sql_table(table_name, engine)
            return table
        # Develop a method inside your DataExtractor class to read the data from the RDS database.

#         Develop a method called read_rds_table in your DataExtractor class which will extract the database table to a pandas DataFrame.

# It will take in an instance of your DatabaseConnector class and the table name as an argument and return a pandas DataFrame.
# Use your list_db_tables method to get the name of the table containing user data.
# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
        
        
    def retrieve_pdf_data(self, pdf_url):
        tables = PdfReader(pdf_url)
        num_pages = len(tables.pages)

        return tables[0].df
    #         Step 2:
#   Create a method in your DataExtractor class called retrieve_pdf_data, which takes in a link as an argument and returns a pandas DataFrame.
#   Use the tabula-py Python package, imported with tabula to extract all pages from the pdf document at following link .
#   Then return a DataFrame of the extracted data.
    
   


pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
extractor = DataExtractor()
pdf_data_df = extractor.retrieve_pdf_data(pdf_url)


# Displaying the DataFrame
print(pdf_data_df)