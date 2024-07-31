import pandas as pd

class DataTransform:
    """
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
    """
    
    def __init__(self, df):
        self.df = df
    
    def change_type(self, df_column, data_type):
        """
        Changes the dtype of the dataframe column to the given data type

        
        Parameters:
        ------------
        df_column (str)     the dataframe column that we want to change the dtype of
        data_type (str)     the dtype we want to change the dataframe column data into
        """
        
        self.df[df_column] = self.df[df_column].astype(data_type)
    
    def date_data(self, df_column):
        """
        Changes the data of the dataframe column given into dtype 'datetime'

        
        Parameters:
        ------------
        df_column (str)     the name of the dataframe column we wish to convert to dtype 'datetime'
        """
        
        self.df[df_column] = pd.to_datetime(self.df[df_column], format = 'mixed')
    