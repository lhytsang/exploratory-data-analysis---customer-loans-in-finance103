import pandas as pd
import sqlalchemy 

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
    
    def change_type(self, dataframe, dataframe_column, data_type):
        dataframe[dataframe_column] = dataframe[dataframe_column].astype(data_type)

        return dataframe, dataframe_column
    
    def date_data(self, dataframe, dataframe_column):
        dataframe[dataframe_column] = pd.to_datetime(dataframe[dataframe_column], format = 'mixed')

        return dataframe, dataframe_column
    
class DataFrameInfo:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def info(self, dataframe):
        return dataframe.info()
    
    def stats(self, dataframe):
        return dataframe.describe()

    def unique_vals(self, dataframe, dataframe_column):
        distinct_values = dataframe[dataframe_column].unique()

        return distinct_values
    
    def dataframe_shape(self, dataframe):
        return dataframe.shape
    
    def missing(self, dataframe, dataframe_column):
        count = dataframe[dataframe_column].isna().sum()
        percentage_count = (count/54231) * 100

        return count, percentage_count

class Plotter:

    def __init__(self, dataframe):
        self.dataframe = dataframe

class DataFrameTransform:

    def __init__(self, dataframe): 
        self.dataframe = dataframe
    
    

def load_csv(file):
    return pd.read_csv(file)