import pandas as pd, sqlalchemy, matplotlib.pyplot as plt, numpy as np, seaborn as sns
from scipy import stats

class RDSDatabaseConnector:
  
    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(self):
        engine = sqlalchemy.create_engine(f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}")
        sql = "SELECT * FROM loan_payments"
        df = pd.read_sql(sql, con = engine, index_col='id')

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
        percentage_count = (count/dataframe.shape[0]) * 100
        percentage_count = round(percentage_count, 2)

        return count, percentage_count
    
    def df_skew(self, dataframe):
        skew = {col: dataframe[col].skew() for col in dataframe.columns if dataframe.dtypes[col] in ['float64', 'int64']}
        return skew
    
    def print_skew(self, dataframe):
        for col, skew in self.df_skew(dataframe).items():
            print(f'{col}: {skew}')
        print('\n\n')

class DataFrameTransform:

    def __init__(self, dataframe): 
        self.dataframe = dataframe
    
    def fill_null(self, dataframe, values):
        dataframe = dataframe.fillna(value = values)

        return dataframe

    def log_transform(self, dataframe):
        for column in dataframe.columns:
            dataframe.loc[:,column] = (dataframe[[column]].map(lambda i: np.log(i) if i > 0 else 0)).copy()
        
        return dataframe
    
    def yeojohnson_transform(self, dataframe):
        for column in dataframe.columns:
            dataframe.loc[:,column] = (stats.yeojohnson(dataframe[column])[0]).copy()

        return dataframe
    
    def boxcox_transform(self, dataframe):
        for column in dataframe.columns:
            for element in dataframe[column].values:
                if element % 10 == 0:
                    dataframe = dataframe.drop(columns = column)
                    break

        for column in dataframe.columns:    
            dataframe.loc[:,column]  = (stats.boxcox(dataframe[column])[0]).copy()
        
        return dataframe
    
class Plotter:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def plot_missing(self, dataframe):
        column_headings = dataframe.columns.values.tolist()
        null_values = []
        for columns in column_headings:
            null_values.append(dataframe[columns].isna().sum())

        plt.bar(column_headings, null_values)
        plt.show()

    def plot_hist(self, dataframe):
        dataframe.hist()

    def plot_boxplot(self, dataframe, df_column):
        ax = dataframe.plot.box(column = df_column)
        plt.title(f'{df_column} Data Distribution')

    def plot_scatter(self, dataframe, df_column):
        x = np.linspace(0, max(df_column))
        y = df_column
        plt.scatter(x, y)

def load_csv(file):
    return pd.read_csv(file, index_col='id')

def make_list(dataframe, column):
    return list(dataframe[column])