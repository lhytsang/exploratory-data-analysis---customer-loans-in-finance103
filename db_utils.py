import pandas as pd, sqlalchemy, matplotlib.pyplot as plt

class RDSDatabaseConnector:
  
    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(self):
        engine = sqlalchemy.create_engine(f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}")
        sql = "SELECT * FROM loan_payments"
        df = pd.read_sql(sql, con = engine)

        return df
    
    def save_file(self, dataframe, filename):
        dataframe.to_csv(filename)

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
        percentage_count = round(percentage_count, 2)

        return count, percentage_count

class DataFrameTransform:

    def __init__(self, dataframe): 
        self.dataframe = dataframe
    
    def fill_null(self, dataframe, values):
        dataframe = dataframe.fillna(value = values)

        return dataframe

class Plotter:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def plot_missing(dataframe):
        column_headings = dataframe.columns.values.tolist()
        null_values = []
        for columns in column_headings:
            null_values.append(dataframe[columns].isna().sum())

        plt.bar(column_headings, null_values)
        plt.show()


def load_csv(file):
    return pd.read_csv(file)

def make_list(dataframe, column):
    return list(dataframe[column])