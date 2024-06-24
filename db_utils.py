from scipy import stats
import numpy as np, matplotlib.pyplot as plt, pandas as pd, plotly.express as px, sqlalchemy 

class RDSDatabaseConnector:
    '''
    A class used to extract the data from a specific website.

    
    Attributes
    ----------
    credentials: Python dictionary
        collection of key: value pairs containing the database credentials 

    Methods
    -------
    initialise_database()
        Sets up the dataset and returns it as a dataframe 
    
    load_csv(file)
        Opens the CSV file

    save_file(df, filename)
        Saves the dataframe given as a file called 'filename'

    '''

    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(self):
        '''
        Establishing the dataset from the credentials given when calling the class and returning the data as a Pandas dataframe
        '''

        engine = sqlalchemy.create_engine(f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}")
        sql = "SELECT * FROM loan_payments"
        df = pd.read_sql(sql, con = engine)

        return df
    
    def load_csv(self, file):
        '''
        Accessing the data given in the file provided. 
        

        Parameters: 
        
        file (str)     the filename of the document we wish to retrieve the data from
        '''
        return pd.read_csv(file, index_col=[0])

    def save_file(self, df, filename):
        '''
        Making a copy of the 'df' on the local machine as a CSV file and labelling the document as 'filename'#

        
        Parameters: 
        
        df              the dataframe which we want to make a copy of
        filename (str)  the name that we want to save the file as
        '''
        df.to_csv(filename)

class DataTransform:
    '''
    A class used to alter the data of a dataframe.

    
    Attributes
    ----------
    dataframe: Pandas dataframe
        a table containing the data of all members and their financial details


    Methods
    -------
    change_type(df_column, data_type)
        Alters the type of the dataframe column to the data type given

    date_data(df_column)
        Changes the values of the dataframe column to type 'datetime'

    '''
    def __init__(self, df):
        self.df = df
    
    def change_type(self, df_column, data_type):
        '''
        Changes the dtype of the df_column to the given 'data_type'

        Parameters: 
        
        df_column (str)     the dataframe column that we want to change the dtype of
        data_type (str)     the dtype we want to change the dataframe column data into
        '''
        self.df[df_column] = self.df[df_column].astype(data_type)
    
    def date_data(self, df_column):
        '''
        Changes the data of the dataframe column given into dtype 'datetime'

        Parameters: 
        
        df_column (str)     the name of the dataframe column we wish to convert to dtype 'datetime'
        '''

        self.df[df_column] = pd.to_datetime(self.df[df_column], format = 'mixed')
    
class DataFrameInfo:
    '''
    A class used to find out information about a dataframe.

    
    Attributes
    ----------
    dataframe: Pandas dataframe
        a table containing the data of all members and their financial details


    Methods
    -------
    df_shape()

    df_skew(df, print)

    info()
    
    missing(df_column)

    stats()

    unique_vals(df_column)

    '''
    def __init__(self, df):
        self.df = df


    def df_shape(self):
        return self.df.shape    
              
    @staticmethod
    def df_skew(df, print=0):
        skew = {df_column: df[df_column].skew() for df_column in df.columns if df.dtypes[df_column] in ['float64', 'int64']}
        
        if print == 1:
            for df_col, skews in skew(df).items():
                print(f'{df_col}: {skews}')
        
        return skew
    
    def info(self):
        return self.df.info()

    def missing(self, df_column):
        count = self.df[df_column].isna().sum()
        percentage_count = (count/self.df_shape()[0]) * 100
        percentage_count = round(percentage_count, 2)

        return count, percentage_count
    
    def stats(self):
        '''
        Extract statistical values: median, standard deviation and mean from the columns and the dataFrame
        '''
        return self.df.describe()

    def unique_vals(self, df_column):
        return self.df[df_column].unique()


class DataFrameTransform:
    '''
    A class used to change and alter the data within a dataframe.

    
    Attributes
    ----------
    dataframe: Pandas dataframe
        a table containing the data of all members and their financial details


    Methods
    -------
    boxcox_transform(df)
        Applies the Box-Cox Transform method to correct skew after checking the data is positive in the given dataframe

    fill_null(values)
        Fills the null values in the dataframe with the value provided

    log_transform(df)
        Applies the Log transform method to correct skew in the given dataframe

    remove_outliers()
        Removes any outliers in each column of the dataframe  
    
    yeojohnson_transform(df)
        Applies the Yeo-Johnson Transform to correct skew in the given dataframe

    '''
    def __init__(self, df): 
        self.df = df

    @staticmethod
    def boxcox_transform(df):
        for df_column in df.columns:
            for element in df[df_column].values:
                if element % 10 == 0:
                    df= df.drop(columns = df_column)
                    break

        for df_column in df.columns:    
            df[df_column]  = (stats.boxcox(df[df_column])[0]).copy()

        return df
    
    def fill_null(self, values):
        self.df.fillna(value = values, inplace = True)
    
    @staticmethod
    def log_transform(df):
        for df_column in df.columns:
            df[df_column] = (df[[df_column]].map(lambda i: np.log(i) if i > 0 else 0)).copy()
    
        return df

    def remove_outliers(self):
        for categories in self.df.columns:
            if self.df.dtypes[categories] in ['float64', 'int64']:
        
                q1 = self.df[categories].quantile(0.25)
                q3 = self.df[categories].quantile(0.75)
                iqr = q3 - q1
                self.df = self.df[~((self.df[categories]<(q1-1.5*iqr)) | (self.df[categories]>(q3+1.5*iqr)))]
                self.df = self.df.dropna().reset_index(drop=True)
    
    @staticmethod
    def yeojohnson_transform(df):
        for df_column in df.columns:
            df[df_column] = (stats.yeojohnson(df[df_column])[0]).copy()
        
        return df
     
class Plotter:
    '''
    A class used to print graphs of the data.

    
    Attributes
    ----------
    Dataframe: Pandas dataframe
        a table containing the data of all members and their financial details


    Methods
    -------
    plot_boxplot(df_column)
        Creates a boxplot of the data in the column df_column of the dataframe

    plot_missing()
        Creates a bar chart of how many null values there are in each column of the dataframe

    plot_pie(plot_values, section_names, plot_title)
        Plots a pie chart with the 'plot_values' given.

    plot_scatter(df_column)
        Plots a scatter graph of the data in the column df_column of the dataframe

    '''
    def __init__(self, df):
        self.df = df

    @staticmethod
    def plot_bar(x_vals, y_vals):
        plt.bar(x_vals, y_vals)

    def plot_boxplot(self, df_column):
        ax = self.df.plot.box(column = df_column)
        plt.title(f'{df_column} Data Distribution')

    def plot_missing(self):
        '''
        Plots the amount of missing values in each column of the dataframe in a bar chart
        '''
        
        column_headings = self.df.columns.values.tolist()
        null_values = []
        for columns in column_headings:
            null_values.append(self.df[columns].isna().sum())

        self.plot_bar(column_headings, null_values)
        plt.show()

    @staticmethod
    def plot_pie(plot_values, section_names, plot_title):
        fig = px.pie(values=plot_values, names=section_names, title=plot_title)
        fig.show()

    def plot_scatter(self):
        df_cols = self.df.columns.values.tolist()
        
        for df_col in df_cols:
            x = self.df.index
            y = self.df[df_col]
            plt.scatter(x, y, alpha=0.1)
            plt.title(df_col)
            plt.show()

