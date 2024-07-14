import re
class DataCleaning:
    def clean_user_data(self, df):
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True) 
        return df
    
    def clean_card_data(self, df):
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True) 
        return df
    
    def clean_store_data(self, df):
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True) 
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
    
    @staticmethod
    def clean_products_data(df):
        df.dropna(inplace=True)  # Drop rows with any missing values
        df.drop_duplicates(inplace=True)  # Drop duplicate rows
        return df

    @staticmethod
    def clean_orders_data(df):
        df.drop(columns=['first_name', 'last_name', '1', 'level_0'], inplace=True)
        df.dropna(inplace=True)  # Drop rows with any missing values
        df.drop_duplicates(inplace=True)  # Drop duplicate rows
        return df


    @staticmethod
    def clean_date_times_data(df):
        df.dropna(inplace=True)  # Drop rows with any missing values
        df.drop_duplicates(inplace=True)  # Drop duplicate rows
        return df


