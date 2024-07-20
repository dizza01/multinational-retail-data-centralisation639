import re
import pandas as pd


class DataCleaning:
    """A class for cleaning various types of data."""

    def is_valid_float(self, x):
        """
        Check if the given value can be converted to a float.

        Args:
            x: The value to check.

        Returns:
            bool: True if the value can be converted to float, otherwise False.
        """
        try:
            float(x)
            return True
        except (ValueError, TypeError):
            return False

    def is_valid_int(self, x):
        """
        Check if the given value can be converted to an integer.

        Args:
            x: The value to check.

        Returns:
            bool: True if the value can be converted to int, otherwise False.
        """
        try:
            int(x)
            return True
        except (ValueError, TypeError):
            return False

    def is_valid_date(self, x):
        """
        Check if the given value can be parsed as a date in the format 'YYYY-MM-DD'.

        Args:
            x: The value to check.

        Returns:
            bool: True if the value can be parsed as a date, otherwise False.
        """
        try:
            pd.to_datetime(x, format="%Y-%m-%d")
            return True
        except (ValueError, TypeError):
            return False

    def convert_to_date(self, date_str):
        """
        Convert a date string to a standard format.

        Args:
            date_str: The date string to convert.

        Returns:
            datetime.date: The converted date, or None if the date format is invalid.
        """
        date_formats = [
            "%Y/%m/%d", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y",  # Common formats
            "%B %Y %d", "%Y %B %d", "%d %B %Y", "%d %b %Y",  # Formats with month names
            "%Y.%m.%d", "%d.%m.%Y", "%d-%b-%Y"               # Other common separators
        ]
        for date_format in date_formats:
            try:
                return pd.to_datetime(date_str, format=date_format).date()
            except ValueError:
                continue
        return None

    def clean_user_data(self, df):
        """
        Clean and validate user data.

        Args:
            df: DataFrame containing user data.

        Returns:
            DataFrame: The cleaned DataFrame with valid user data.
        """
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
        df = df.drop_duplicates()
        return df

    def clean_card_number(self, value):
        """
        Clean and validate card numbers.

        Args:
            value: The card number to clean.

        Returns:
            str or None: The cleaned card number if valid, otherwise None.
        """
        try:
            value_str = str(int(value))
            return value_str if len(value_str) <= 16 else None
        except (ValueError, TypeError):
            return None

    def clean_expiry_date(self, value):
        """
        Clean and validate expiry dates.

        Args:
            value: The expiry date to clean.

        Returns:
            str or None: The cleaned expiry date if valid, otherwise None.
        """
        if pd.notnull(value) and isinstance(value, str) and len(value) == 5 and '/' in value:
            return value
        return None

    def clean_date(self, value):
        """
        Clean and validate dates.

        Args:
            value: The date to clean.

        Returns:
            datetime.date or None: The cleaned date if valid, otherwise None.
        """
        try:
            return pd.to_datetime(value).date() if pd.notnull(value) else None
        except (ValueError, TypeError):
            return None

    def clean_card_data(self, df):
        """
        Clean and validate card data.

        Args:
            df: DataFrame containing card data.

        Returns:
            DataFrame: The cleaned DataFrame with valid card data.
        """
        card_number_to_query = 2314734659486501

        # Using boolean indexing to find the specific card number
        result_df = df[df['card_number'] == card_number_to_query]
        print(result_df)

        distinct_product_card_number = df['card_number'].nunique()
        print(f"Number of distinct card_number: {distinct_product_card_number}")

        # Clean card number, expiry date, and payment confirmation date
        df['card_number'] = df['card_number'].apply(lambda x: self.clean_card_number(x))
        df['expiry_date'] = df['expiry_date'].apply(lambda x: self.clean_expiry_date(x))
        df['date_payment_confirmed'] = df['date_payment_confirmed'].astype(str)
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(lambda x: self.convert_to_date(x))

        # Drop rows with invalid card numbers
        df = df.dropna(subset=['card_number'])
        distinct_product_card_number = df['card_number'].nunique()
        print(f"Number of distinct card_number: {distinct_product_card_number}")

        result_df = df[df['card_number'] == card_number_to_query]
        print(result_df)
        
        df = df.drop_duplicates()
        return df

    def clean_store_data(self, df):
        """
        Clean and validate store data.

        Args:
            df: DataFrame containing store data.

        Returns:
            DataFrame: The cleaned DataFrame with valid store data.
        """
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
        """
        Convert product weights to kilograms.

        Args:
            df: DataFrame containing product weights.

        Returns:
            DataFrame: The DataFrame with weights converted to kilograms.
        """
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
        """
        Clean and validate product data.

        Args:
            df: DataFrame containing product data.

        Returns:
            DataFrame: The cleaned DataFrame with valid product data.
        """
        distinct_product_codes_count = df['product_code'].nunique()
        print(f"Number of distinct product codes: {distinct_product_codes_count}")

        def clean_product_price(price):
            """
            Clean product price by removing currency symbols.

            Args:
                price: The product price to clean.

            Returns:
                float or None: The cleaned price if valid, otherwise None.
            """
            try:
                return float(str(price).replace('£', ''))
            except ValueError:
                return None

        df['product_price'] = df['product_price'].apply(clean_product_price)
        df['date_added'] = df['date_added'].apply(lambda x: self.convert_to_date(x))
        df['date_added'] = df['date_added'].apply(lambda x: x if pd.notna(x) and x != "NULL" else None)
        df['still_available'] = df['removed'].apply(lambda x: True if isinstance(x, str) and x.lower() == 'still_available' else False if isinstance(x, str) and x.lower() == 'removed' else None)
        df.drop(columns=['removed'], inplace=True)

        # Ensure UUID is valid
        df['uuid'] = df['uuid'].apply(lambda x: x if isinstance(x, str) and len(x) == 36 else None)
        df = df.dropna(subset=['product_code'])
        df = df.drop_duplicates()
        distinct_product_codes_count = df['product_code'].nunique()
        print(f"Number of distinct product codes: {distinct_product_codes_count}")
        return df

    def clean_orders_data(self, df):
        """
        Clean and validate orders data.

        Args:
            df: DataFrame containing orders data.

        Returns:
            DataFrame: The cleaned DataFrame with valid orders data.
        """
        df.drop(columns=['first_name', 'last_name', '1', 'level_0'], inplace=True)
        df['user_uuid'] = df['user_uuid'].apply(lambda x: x if pd.notna(x) and x != "NULL" and len(x) == 36 else None)
        df['card_number'] = df['card_number'].apply(lambda x: self.clean_card_number(x))
        distinct_product_card_number = df['card_number'].nunique()
        print(f"Number of distinct card_number: {distinct_product_card_number}")
        df = df.dropna(subset=['card_number'])
        distinct_product_card_number = df['card_number'].nunique()
        print(f"Number of distinct card_number: {distinct_product_card_number}")
        df = df.drop_duplicates()
        return df

    @staticmethod
    def clean_date_times_data(df):
        """
        Clean and validate date-time data.

        Args:
            df: DataFrame containing date-time data.

        Returns:
            DataFrame: The cleaned DataFrame with valid date-time data.
        """
        df['date_uuid'] = df['date_uuid'].apply(lambda x: x if isinstance(x, str) and len(x) == 36 else None)
        df.dropna(inplace=True)  # Drop rows with any missing values
        df.drop_duplicates(inplace=True)  # Drop duplicate rows
        return df
