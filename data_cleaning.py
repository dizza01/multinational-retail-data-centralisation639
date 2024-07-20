import re
import pandas as pd

class DataCleaning:

    # Define functions to identify valid entries
    def is_valid_float(self, x):
        try:
            float(x)
            return True
        except (ValueError, TypeError):
            return False

    def is_valid_int(self, x):
        try:
            int(x)
            return True
        except (ValueError, TypeError):
            return False

    def is_valid_date(self, x):
        try:
            pd.to_datetime(x, format="%Y-%m-%d")
            return True
        except (ValueError, TypeError):
            return False
        
    # Function to convert date strings to a standard format
    def convert_to_date(self, date_str):
        date_formats = [
            "%Y/%m/%d", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", # Common formats
            "%B %Y %d", "%Y %B %d", "%d %B %Y", "%d %b %Y", # Formats with month names
            "%Y.%m.%d", "%d.%m.%Y", "%d-%b-%Y"              # Other common separators
        ]
        for date_format in date_formats:
            try:
                return pd.to_datetime(date_str, format=date_format).date()
            except ValueError:
                continue
        return None

    def clean_user_data(self, df):
        # Convert columns to string type to avoid errors with .str accessor
        df['user_uuid'] = df['user_uuid'].astype(str)
        df['date_of_birth'] = df['date_of_birth'].astype(str)
        df['join_date'] = df['join_date'].astype(str)

        # Apply the date conversion and store the results back in the DataFrame
        df['date_of_birth'] = df['date_of_birth'].apply(lambda x: self.convert_to_date(x))
        df['join_date'] = df['join_date'].apply(lambda x: self.convert_to_date(x))

        # Convert nulls to None and filter invalid UUIDs
        df['user_uuid'] = df['user_uuid'].apply(lambda x: x if pd.notna(x) and x != "NULL" and len(x) == 36 else None)
        df['date_of_birth'] = df['date_of_birth'].apply(lambda x: x if pd.notna(x) and x != "NULL" else None)
        df['join_date'] = df['join_date'].apply(lambda x: x if pd.notna(x) and x != "NULL" else None)

        # Drop rows with any None values and duplicates
        df = df.dropna(subset=['user_uuid'])
        df.drop_duplicates()
        return df

        # Function to clean and validate card numbers
    # Function to clean and validate card numbers
    def clean_card_number(self, value):
        try:
            value_str = str(int(value))
            return value_str if len(value_str) <= 16 else None
        except (ValueError, TypeError):
            return None

    # Function to clean expiry dates
    def clean_expiry_date(self, value):
        if pd.notnull(value) and isinstance(value, str) and len(value) == 5 and '/' in value:
            return value
        return None

    # Function to clean and validate dates
    def clean_date(self, value):
        try:
            return pd.to_datetime(value).date() if pd.notnull(value) else None
        except (ValueError, TypeError):
            return None

    def clean_card_data(self, df):
        card_number_to_query = 2314734659486501

# Using boolean indexing to find the specific card number
        result_df = df[df['card_number'] == card_number_to_query]
        print(result_df)

        distinct_product_card_number = df['card_number'].nunique()
        print(f"Number of distinct card_number: {distinct_product_card_number}")
        # Convert columns to string type to avoid errors with .str accessor
        df['card_number'] = df['card_number'].apply(lambda x: self.clean_card_number(x))
        df['expiry_date'] = df['expiry_date'].apply(lambda x: self.clean_expiry_date(x))
        df['date_payment_confirmed'] = df['date_payment_confirmed'].astype(str)
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(lambda x: self.convert_to_date(x))
        distinct_product_card_number = df['card_number'].nunique()
        print(f"Number of distinct card_number: {distinct_product_card_number}")
        df = df.dropna(subset=['card_number'])
        distinct_product_card_number = df['card_number'].nunique()
        print(f"Number of distinct card_number: {distinct_product_card_number}")
        result_df = df[df['card_number'] == card_number_to_query]
        print(result_df)
        df.drop_duplicates()  
        return df
    
    def clean_store_data(self, df):
        # Debugging: Show initial DataFrame
        print("Initial DataFrame:\n", df)

        # Apply the valid float check function
        df['longitude'] = df['longitude'].apply(lambda x: x if self.is_valid_float(x) else None)
        df['latitude'] = df['latitude'].apply(lambda x: x if self.is_valid_float(x) else None)
        df['lat'] = df['lat'].apply(lambda x: x if self.is_valid_float(x) else None)
        
        # Debugging: Show DataFrame after float validation
        print("\nAfter float validation:\n", df)

        # Convert the columns to numeric, coercing errors to NaN
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        
        # Debugging: Show DataFrame after converting to numeric
        print("\nAfter converting to numeric:\n", df)

        # Apply the valid integer check function
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x: x if self.is_valid_int(x) else None)
        
        # Debugging: Show DataFrame after integer validation
        print("\nAfter integer validation:\n", df)

        # Convert date strings to date objects
        df['opening_date'] = df['opening_date'].apply(lambda x: self.convert_to_date(x))
        df['opening_date'] = df['opening_date'].apply(lambda x: x if pd.notna(x) and x != "NULL" else None)

        # Debugging: Show DataFrame after date conversion
        print("\nAfter date conversion:\n", df)

        # Drop rows with NaN values in the specified columns
        df = df.dropna(subset=['longitude'])
        df = df.dropna(subset=['latitude', 'lat'], how='all')
        # Debugging: Show DataFrame after dropping NaN values
        print("\nAfter dropping NaN values:\n", df)

        # Drop duplicates
        df = df.drop_duplicates()
        
        # Debugging: Show final cleaned DataFrame
        print("\nFinal cleaned DataFrame:\n", df)
        return df
        
    @staticmethod
    def convert_product_weights(df):
        def to_kg(value):
            # Convert the value to a string, remove spaces, and convert to lowercase
            value = str(value).lower().replace(' ', '')
            
            # Conversion logic
            if 'kg' in value:
                return float(re.sub('[^0-9.]', '', value))  # Remove non-numeric characters and return as float
            elif 'g' in value:
                return float(re.sub('[^0-9.]', '', value)) / 1000  # Convert grams to kg
            elif 'ml' in value:
                return float(re.sub('[^0-9.]', '', value)) / 1000  # Convert ml to kg assuming 1:1 ratio with grams
            elif 'l' in value:
                return float(re.sub('[^0-9.]', '', value))  # Convert liters to kg
            else:
                try:
                    return float(re.sub('[^0-9.]', '', value))  # Try converting to float
                except ValueError:
                    return None  # Return None if conversion fails

       # Apply the conversion function to the weight column
        df['weight'] = df['weight'].apply(to_kg)
        return df
    
    
    def clean_products_data(self, df):
        # df['product_price'] = df['product_price'].apply(lambda x: x if self.is_valid_float(x) else None)
        distinct_product_codes_count = df['product_code'].nunique()

        print(f"Number of distinct product codes: {distinct_product_codes_count}")
        def clean_product_price(price):
            try:
                return float(str(price).replace('£', ''))
            except ValueError:
                return None
        df['product_price'] = df['product_price'].apply(clean_product_price)
        # Convert date_added to datetime
        df['date_added'] = df['date_added'].apply(lambda x: self.convert_to_date(x))
        df['date_added'] = df['date_added'].apply(lambda x: x if pd.notna(x) and x != "NULL" else None)
        # Ensure still_available is boolean
        df['still_available'] = df['removed'].apply(lambda x: True if isinstance(x, str) and x.lower() == 'still_available' else False if isinstance(x, str) and x.lower() == 'removed' else None)
        df.drop(columns=['removed'], inplace=True)

        # Ensure uuid is valid
        df['uuid'] = df['uuid'].apply(lambda x: x if isinstance(x, str) and len(x) == 36 else None)
        df = df.dropna(subset=['product_code'])
        df.drop_duplicates()  # Drop duplicate rows
        distinct_product_codes_count = df['product_code'].nunique()
        print(f"Number of distinct product codes: {distinct_product_codes_count}")
        return df

    def clean_orders_data(self, df):
        distinct_product_card_number = df['card_number'].nunique()
        print(f"Number of distinct card_number: {distinct_product_card_number}")
        df.drop(columns=['first_name', 'last_name', '1', 'level_0'], inplace=True)
        df['user_uuid'] = df['user_uuid'].apply(lambda x: x if pd.notna(x) and x != "NULL" and len(x) == 36 else None)
        df['card_number'] = df['card_number'].apply(lambda x: self.clean_card_number(x))
        distinct_product_card_number = df['card_number'].nunique()
        print(f"Number of distinct card_number: {distinct_product_card_number}")
        df = df.dropna(subset=['card_number'])
        distinct_product_card_number = df['card_number'].nunique()
        print(f"Number of distinct card_number: {distinct_product_card_number}")
        # df.dropna()  # Drop rows with any missing values
        df.drop_duplicates()  # Drop duplicate rows
        return df


    @staticmethod
    def clean_date_times_data(df):
        df['date_uuid'] = df['date_uuid'].apply(lambda x: x if isinstance(x, str) and len(x) == 36 else None)
        df.dropna(inplace=True)  # Drop rows with any missing values
        df.drop_duplicates(inplace=True)  # Drop duplicate rows
        return df
    



