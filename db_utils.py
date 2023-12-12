import pandas as pd
import sqlalchemy 
import yaml

with open('credentials.yaml') as file:
    credentials_dict = yaml.safe_load(file)
  
class RDSDatabaseConnector:
  
    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(self):
        engine = sqlalchemy.create_engine(f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}")
        sql = "SELECT * FROM loan_payments"
        df = pd.read_sql(sql, con = engine, index_col='id')

        return df
    
    def save_file(self, dataframe):
        dataframe.to_csv('new_file.csv')

class DataTransform:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def check_empty(self, dataframe_col):
        empty_vals = 0

        for cell in dataframe_col:
            if cell.empty:
                empty_vals += 1

        return 'empty', empty_vals

    def fill_zeros(self, column_name):
        new_dataframe = column_name.fillna(0)

        return new_dataframe
    
    def change_type(self, dataframe_column, data_type):
        dataframe_column = dataframe_column.astype(data_type)

        return dataframe_column
    
    def date_data(self, dataframe, dataframe_column):
        dataframe.dataframe_column = pd.to_datetime(dataframe.dataframe_column)

        return dataframe

credentials = RDSDatabaseConnector(credentials_dict)
loan_payments = credentials.initialise_database()

def load_csv(file):
    return pd.read_csv(file)
credentials.save_file(loan_payments)
database = pd.read_csv('new_file.csv', index_col = 'id')

cleaned_data = DataTransform(database)
#no_null_data = cleaned_data.fill_zeros(database)

for item in database['term']:
    item = item.replace('month', '')


print(cleaned_data.head())
print(cleaned_data.info())