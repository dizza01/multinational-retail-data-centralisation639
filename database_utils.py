import yaml
import psycopg2
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
        def __init__(self):
            self.DATABASE_TYPE = 'postgresql'
            self.DBAPI = 'psycopg2'
            
        
        def read_db_creds(self, file_path):
            with open(file_path, 'r') as file:
                config_data = yaml.safe_load(file)
            return config_data
    #         Create a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
    #           You will need to pip install PyYAML and import yaml to do this.

        def init_db_engine(self):
            config_data = self.read_db_creds('pg_db_creds.yaml')
            DATABASE_TYPE = self.DATABASE_TYPE
            DBAPI = self.DBAPI
            HOST = config_data['PG_HOST']
            USER = config_data["PG_USER"]
            PASSWORD = config_data["PG_PASSWORD"]
            DATABASE = config_data["PG_DATABASE"]
            PORT = config_data["PG_PORT"]
            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
            return engine
            # Now create a method init_db_engine which will read the credentials from the return of read_db_creds and 
            #initialise and return an sqlalchemy database engine.

        def list_db_tables(self):
            engine  = self.init_db_engine()
            engine.execution_options(isolation_level='AUTOCOMMIT').connect()
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            print(tables)
    #         Using the engine from init_db_engine create a method list_db_tables to list all the tables 
            #in the database so you know which tables you can extract data from.
            
        def upload_to_db(self, df, table_name):
            engine = self.init_db_engine()
            df.to_sql(table_name, engine, if_exists='replace')
            # Now create a method in your DatabaseConnector class called upload_to_db. This method will take in a Pandas DataFrame and table name to upload to as an argument.
            #Once extracted and cleaned use the upload_to_db method to store the data in your sales_data database in a table named dim_users.

if __name__ == "__main__":    
    connector = DatabaseConnector()
    connector.init_db_engine()
    connector.list_db_tables()


