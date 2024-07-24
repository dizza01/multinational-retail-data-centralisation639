

# Project Title

**Multinational Retail Data Centralization**

## Table of Contents

1. [Description](#description)
2. [Installation Instructions](#installation-instructions)
3. [Usage Instructions](#usage-instructions)
4. [File Structure](#file-structure)
5. [Data Sources](#data-sources)


## Description

This project centralizes and cleans data from a multinational retail dataset, focusing on the following key areas:

- **Data Extraction**: Extracts data from various sources including PDFs and databases.
- **Data Cleaning**: Cleans and preprocesses data to ensure consistency and accuracy. This includes handling missing values, correcting data types, and managing foreign key constraints.
- **Data Integration**: Merges and integrates data from different tables to create a comprehensive dataset suitable for analysis.


### Aims

- Centralize data from multiple sources into a unified format.
- Clean and preprocess data to ensure high quality and consistency.
- Prepare the dataset for further analysis and reporting.

### What I Learned

- Techniques for handling and cleaning large datasets from various sources.
- Methods for data type conversion and managing inconsistencies.
- Implementation of data integrity checks and foreign key constraints in SQL.

## Installation Instructions

To set up this project on your local machine, follow these steps:

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/your-username/multinational-retail-data-centralization.git
    ```

2. **Navigate to the Project Directory**:
    ```bash
    cd multinational-retail-data-centralization
    ```

3. **Install Dependencies**:
    Ensure you have Python 3.x installed, then install the required packages using pip:
    ```bash
    pip install -r requirements.txt
    ```



## Usage Instructions

1. **Data Extraction**:
   - Use `data_cleaning.py` to clean and preprocess the extracted data.

2. **Data Utilities**:
   - Utilize `database_utils.py` for additional data manipulation and utility functions.

Example command to run the data cleaning script:
```bash
python data_cleaning.py
```

## File Structure

Here is the structure of the project directory:

```plaintext
.
├── requirements.txt           # Python dependencies
├── sales_queries.sql          # SQL queries related to sales data
├── database_utils.py          # Python script for database utilities
├── data_cleaning.py           # Python script for data cleaning operations
├── pg_db_creds.yaml           # YAML file for PostgreSQL database credentials
├── db_creds.yaml              # YAML file for database credentials
├── sales_modelling.sql        # SQL script for sales modeling
├── .gitignore                 # Specifies files and directories to ignore in version control
└── README.md                  # This file
```


## Data Sources

The project extracts data from various sources using the `data_extraction.py` script. Below is a summary of each data source and the methods used for extraction:

1. **PDF Files**:
   - **Method**: `retrieve_pdf_data(pdf_url)`
   - **Description**: Extracts tables from a PDF file available via a URL. Multiple tables are extracted and combined into a single DataFrame.
   - **Example**: Extracting card details from a PDF stored in an S3 bucket.
   - **Library**: `tabula` for reading PDF tables.

2. **Amazon S3**:
   - **CSV Files**:
     - **Method**: `extract_from_s3(s3_address)`
     - **Description**: Retrieves data from a CSV file stored in S3. The CSV file is read into a DataFrame.
     - **Example**: Extracting product data from a CSV file in S3.
     - **Library**: `boto3` for interacting with S3.
   - **JSON Files**:
     - **Method**: `extract_json_from_s3(url)`
     - **Description**: Fetches JSON data from a URL (e.g., an S3 URL) and converts it to a DataFrame.
     - **Example**: Extracting date details from a JSON file in S3.

3. **APIs**:
   - **Number of Stores**:
     - **Method**: `list_number_of_stores(no_stores_endpoint, headers_dict)`
     - **Description**: Retrieves the number of stores from an API endpoint that returns a JSON object with the count.
     - **Example**: Fetching the number of stores from a specified API endpoint.
   - **Store Details**:
     - **Method**: `retrieve_stores_data(store_endpoint, headers_dict, no_of_stores)`
     - **Description**: Retrieves detailed information about stores from an API endpoint. Data for each store is collected and combined into a DataFrame.
     - **Example**: Fetching detailed store information from a specified API endpoint.

5. **Relational Databases**:
   - **Method**: `read_rds_table(cls, db_creds, db, table_name)`
   - **Description**: Reads data from a table in an RDS database into a DataFrame. Utilizes database credentials and the specified table name to fetch the data.
   - **Example**: Extracting order data from an RDS database table.
   - **Library**: `pandas` for reading SQL tables.




### SQL Files

- **`sales_queries.sql`**: Contains SQL queries used to retrieve and analyze sales data.
- **`sales_modelling.sql`**: Includes SQL scripts for modeling sales data.

