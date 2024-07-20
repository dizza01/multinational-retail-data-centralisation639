import yaml
import psycopg2
from sqlalchemy import create_engine, inspect
import pandas as pd


class DatabaseConnector:
    """A class for connecting to and interacting with a PostgreSQL database."""

    def __init__(self):
        """Initialize the DatabaseConnector with default database type and API."""
        self.DATABASE_TYPE = 'postgresql'
        self.DBAPI = 'psycopg2'

    def read_db_creds(self, file_path):
        """
        Read database credentials from a YAML file.

        Args:
            file_path (str): Path to the YAML file containing database credentials.

        Returns:
            dict: A dictionary of database credentials.
        """
        with open(file_path, 'r') as file:
            config_data = yaml.safe_load(file)
        return config_data

    def init_db_engine(self, db_creds, db):
        """
        Initialize and return a SQLAlchemy database engine.

        Args:
            db_creds (str): Path to the YAML file with database credentials.
            db (str): The database name to use for retrieving credentials.

        Returns:
            sqlalchemy.engine.Engine: The initialized database engine.
        """
        config_data = self.read_db_creds(db_creds)
        DATABASE_TYPE = self.DATABASE_TYPE
        DBAPI = self.DBAPI
        HOST = config_data[f'{db}_HOST']
        USER = config_data[f'{db}_USER']
        PASSWORD = config_data[f'{db}_PASSWORD']
        DATABASE = config_data[f'{db}_DATABASE']
        PORT = config_data[f'{db}_PORT']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    def list_db_tables(self, db_creds, db):
        """
        List all tables in the specified database.

        Args:
            db_creds (str): Path to the YAML file with database credentials.
            db (str): The database name to use for retrieving credentials.

        Returns:
            None: Prints the list of tables.
        """
        engine = self.init_db_engine(db_creds, db)
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(tables)

    def upload_to_db(self, db_creds, db, df, table_name):
        """
        Upload a DataFrame to the specified database table.

        Args:
            db_creds (str): Path to the YAML file with database credentials.
            db (str): The database name to use for retrieving credentials.
            df (pandas.DataFrame): The DataFrame to upload.
            table_name (str): The name of the table to upload the DataFrame to.

        Returns:
            None: Uploads the DataFrame to the database.
        """
        engine = self.init_db_engine(db_creds, db)
        df.to_sql(table_name, engine, if_exists='replace')


if __name__ == "__main__":
    connector = DatabaseConnector()
    connector.list_db_tables('db_creds.yaml', 'RDS')
    # Expected output: List of tables, e.g., ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
