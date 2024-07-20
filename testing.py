import pandas as pd

class DataCleaner:
    def is_valid_float(self, x):
        try:
            # Check if the value can be converted to a float
            float(x)
            return True
        except (ValueError, TypeError):
            return False

    def clean_data(self, df):
        # Apply the valid float check function
        df['longitude'] = df['longitude'].apply(lambda x: x if self.is_valid_float(x) else None)
        df['latitude'] = df['latitude'].apply(lambda x: x if self.is_valid_float(x) else None)
        
        # Convert the columns to numeric, coercing errors to NaN
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        
        return df

# Example usage
data = {
    'longitude': ['12.345', 'abc', '67.890', 'def', 'NaN'],
    'latitude': ['-45.678', 'ghi', '90.123', 'jkl', 'NaN']
}
df = pd.DataFrame(data)

cleaner = DataCleaner()
cleaned_df = cleaner.clean_data(df)

print(cleaned_df)
