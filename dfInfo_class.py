import numpy as np

class DataFrameInfo:
    """
    A class used to find out information about a dataframe.

    
    Attributes
    ----------
    dataframe: Pandas dataframe
        a table containing the data of all members and their financial details


    Methods
    -------
    check_correlation(df)
        Produces a correlation matrix for the dataframe

    df_shape()
        Gives the dimensions of the dataframe in a tuple

    df_skew(df, print)
        Calculates how skewed the data in each dataframe column is

    info()
        Provides a summary of the dataframe
    
    missing(df_column)
        Caluculates the amount of missing values in the dataframe column

    print_skew(df)
        Prints out how skewed each column in a dataframe is
    
    stats()
        Prints out descriptive statistics about the dataframe
    
    unique_vals(df_column)
        Returns the unique values in the dataframe column
    """

    def __init__(self, df):
        self.df = df

    @staticmethod
    def check_correlation(df):
        """
        Calculates the correlation between columns in the dataframe and returns it as a correlation matrix


        Parameters:
        ------------
        df (dataframe)      Pandas dataframe

        Returns:
        ---------
        corr_matrix (type)...
        """
        
        corr_matrix = df.corr()
        np.seterr(divide='ignore', invalid='ignore')

        return corr_matrix

    def df_shape(self):
        """
        Gives the number of rows and columns in a dataframe in the form of a tuple

        Returns:
        ---------
        A tuple containing the number of rows and columns of the dataframe in the form (rows, columns)
        """
        
        return self.df.shape    
              
    @staticmethod
    def df_skew(df):
        """
        Calculates how skewed the data is in each column of the dataframe and prints the results if specified
        

        Parameters:
        ------------
        df (dataframe)      Pandas dataframe

        Returns:
        --------
        Dictionary containing the column names as the keys and their skews as the values
        """
        
        skew = {df_column: df[df_column].skew() for df_column in df.columns if df.dtypes[df_column] in ['float64', 'int64']}
        
        return skew
    
    def info(self):
        """
        Prints out information of a dataframe including index dtypes and columns, non-null values and memory usage
        
        Returns:
        --------

        """
        
        return self.df.info()

    def missing(self, df_column):
        """
        Works out the amount of missing values in the dataframe column and also returns it as a percentage 

        Parameters:
        ------------
        df_column (str)     dataframe column

        Returns:
        --------
        count (int?)    the number of missing values in a dataframe column
        percentage_count (float)    the amount of missing values as a percentage of the whole column
        """
        
        count = self.df[df_column].isna().sum()
        percentage_count = (count/self.df_shape()[0]) * 100
        percentage_count = round(percentage_count, 2)

        return count, percentage_count
    
    @staticmethod
    def print_skew(df):
        """
        Prints out the dataframe columns as well as their corresponding skew values calculated in 'df_skew()'
        """
        
        data_skew = DataFrameInfo.df_skew(df)

        for df_col, skews in data_skew.items():
            print(f'{df_col}: {skews}')

    def stats(self):
        """
        Extracts statistical values like measures of central tendency and dispersion

        Returns:
        --------

        """
        df_stats = self.df.describe()
        
        return df_stats

    def unique_vals(self, df_column):
        """
        Finds the distinctive values in a dataframe column

        Parameters:
        ------------
        df_column (str)     dataframe column

        Returns:
        --------
        distinct_vals (list?)       
        """
        
        distinct_vals = self.df[df_column].unique()
        return distinct_vals