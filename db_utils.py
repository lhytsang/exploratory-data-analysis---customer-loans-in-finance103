import pandas as pd, sqlalchemy, matplotlib.pyplot as plt, numpy as np
from scipy import stats

class RDSDatabaseConnector:
    '''
    A class used to extract the data from a specific website.

    
    Attributes
    ----------
    


    Methods
    -------
    

    '''
    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(self):
        engine = sqlalchemy.create_engine(f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}")
        sql = "SELECT * FROM loan_payments"
        df = pd.read_sql(sql, con = engine)

        return df
    
    def save_file(self, df, filename):
        df.to_csv(filename)

    def load_csv(self, file):
        return pd.read_csv(file, index_col=[0])

class DataTransform:
    '''
    A class used to alter the data of a dataframe.

    
    Attributes
    ----------
    Dataframe: Pandas dataframe
        a table containing the data of all members and their financial details


    Methods
    -------
    

    '''
    def __init__(self, df):
        self.df = df
    
    def change_type(self, df_column, data_type):
        self.df[df_column] = self.df[df_column].astype(data_type)
    
    def date_data(self, df_column):
        self.df[df_column] = pd.to_datetime(self.df[df_column], format = 'mixed')
    
    def make_list(self, df_column):
        return list(self.df[df_column])
    
class DataFrameInfo:
    '''
    A class used to find out information about a dataframe.

    
    Attributes
    ----------
    Dataframe: Pandas dataframe
        a table containing the data of all members and their financial details


    Methods
    -------
    
    

    '''
    def __init__(self, df):
        self.df = df

    def info(self):
        return self.df.info()
    
    def stats(self):
        return self.df.describe()

    def unique_vals(self, df_column):
        return self.df[df_column].unique()
    
    def dataframe_shape(self):
        return self.df.shape
    
    def missing(self, df_column):
        count = self.df[df_column].isna().sum()
        percentage_count = (count/self.df.shape[0]) * 100
        percentage_count = round(percentage_count, 2)

        return count, percentage_count
    
    def df_skew(self):
        skew = {col: self.df[col].skew() for col in self.df.columns if self.df.dtypes[col] in ['float64', 'int64']}
        return skew
    
    def print_skew(self):
        for df_column, skew in self.df_skew(self.df).items():
            print(f'{df_column}: {skew}')
        print('\n\n')

class DataFrameTransform:
    '''
    A class used to change and alter the data within a dataframe.

    
    Attributes
    ----------
    Dataframe: Pandas dataframe
        a table containing the data of all members and their financial details


    Methods
    -------
    

    '''
    def __init__(self, df): 
        self.df = df
    
    def fill_null(self, values):
        self.df = self.df.fillna(value = values)
    
    def log_transform(self):
        for df_column, col_data in self.df.items():
            self.df[df_column] = (self.df[[df_column]].map(lambda i: np.log(i) if i > 0 else 0)).copy()
    
    def yeojohnson_transform(self):
        for df_column, col_data in self.df.items():
            self.df[df_column] = (stats.yeojohnson(self.df[df_column])[0]).copy()
    
    def boxcox_transform(self):
        for df_column, col_data in self.df.items():
            for element in self.df[df_column].values:
                if element % 10 == 0:
                    self.df= self.df.drop(columns = df_column)
                    break

        for df_column, col_data in self.df.items():    
            self.df[df_column]  = (stats.boxcox(self.df[df_column])[0]).copy()
    
class Plotter:
    '''
    A class used to print graphs of the data.

    
    Attributes
    ----------
    Dataframe: Pandas dataframe
        a table containing the data of all members and their financial details


    Methods
    -------
    plot_missing()
        Creates a bar chart of how many null values there are in each column
    
    plot_hist()
        Creates a histogram for each column in the dataframe

    plot_boxplot(df_column)
        Creates a boxplot of the data in the column df_column of the dataframe

    plot_scatter(df_column)
        Plots a scatter graph of the data in the column df_column of the dataframe

    '''
    def __init__(self, df):
        self.df = df

    def plot_missing(self):
        '''
        Plot the missing values of the dataframe in a bar chart

        Parameters: dataframe
            The dataframe which we want to find the amount of missing values in
        '''
        
        column_headings = self.df.columns.values.tolist()
        null_values = []
        for columns in column_headings:
            null_values.append(self.df[columns].isna().sum())

        plt.bar(column_headings, null_values)
        plt.show()

    def plot_hist(self):
        self.df.hist()

    def plot_boxplot(self, df_column):
        ax = self.df.plot.box(column = df_column)
        plt.title(f'{df_column} Data Distribution')

    def plot_scatter(self, df_column):
        col_data = self.df[df_column]
        
        x = np.linspace(0, max(col_data))
        y = col_data
        plt.scatter(x, y, title = df_column)

