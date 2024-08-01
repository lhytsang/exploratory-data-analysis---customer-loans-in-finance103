from scipy import stats
import numpy as np
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
    
class DataFrameTransform:
    """     
    A class used to change and alter the data within a dataframe.

    
    Attributes
    ----------
    dataframe: Pandas dataframe
        a table containing the data of all members and their financial details


    Methods
    -------
    boxcox_transform(df)
        Applies the Box-Cox Transform method to correct skew after checking the data is positive in the given dataframe

    dummy_df(df, df2)
        Creates dummy columns for categorical columns in the dataframe and merges these columns to the second dataframe

    fill_null(values)
        Fills the null values in the dataframe with the value provided

    log_transform(df)
        Applies the Log transform method to correct skew in the given dataframe

    remove_outliers()
        Removes any outliers in each column of the dataframe  
    
    yeojohnson_transform(df)
        Applies the Yeo-Johnson Transform to correct skew in the given dataframe
    """

    def __init__(self, df): 
        self.df = df

    @staticmethod
    def boxcox_transform(df):
        """
        Uses the Box Cox Transform method to find the skew of each dataframe column

        Parameters:
        ------------
        df (dataframe)      Pandas dataframe

        Returns:
        --------
        df (dataframe)      Pandas dataframe that have had the Box Cox Transform method applied to all its columns
        """
        
        for df_column in df.columns:
            for element in df[df_column].values:
                if element % 10 == 0:
                    df= df.drop(columns = df_column)
                    break

        for df_column in df.columns:    
            df[df_column]  = (stats.boxcox(df[df_column])[0]).copy()

        return df
    
    @staticmethod
    def dummy_df(df, df2):
        """
        Produces dummy columns for any categorical dataframe columns in the dataframe and adds them to a second dataframe

        Parameters:
        ------------
        df (dataframe)      Pandas dataframe whose columns we want to make dummies of
        df2 (dataframe)     Pandas dataframe which contain the updated columns

        Returns:
        --------
        df2 (dataframe)     Pandas dataframe that contains dummy columns for all its categorical columns
        """
        
        for df_column in df.columns:
            if df[df_column].dtype.name == 'object':
                df_dummies = pd.get_dummies(df[df_column], dtype=float)
                df2 = pd.concat([df2, df_dummies], axis=1)
                del df2[df_column]
        
        return df2    
    
    def fill_null(self, values):
        """
        Replaces the null elements in a dataframe with a specific value

        Parameters:
        ------------
        values (dict)       A dictionary of the columns we want to be filled and the values that we want to replace the null values with
        """
        
        self.df.fillna(value = values, inplace = True)
    
    @staticmethod
    def log_transform(df):
        """
        Uses the Log Transform method to find the skew of each dataframe column

        Parameters:
        ------------
        df (dataframe)      Pandas dataframe 

        Returns:
        --------
        df (dataframe)      Pandas dataframe whose columns have had the Log Transform method applied to
        """
        
        for df_column in df.columns:
            df[df_column] = (df[[df_column]].map(lambda i: np.log(i) if i > 0 else 0)).copy()
    
        return df

    def remove_outliers(self):
        """
        Finds and removes any outliers that exist in the dataframe
        """
        
        numerical_cols = [num_categories for num_categories in self.df.columns if self.df.dtypes[num_categories] in ['float64', 'int64']]
        
        for num_cat in numerical_cols:
            q1 = self.df[num_cat].quantile(0.25)
            q3 = self.df[num_cat].quantile(0.75)
            iqr = q3 - q1
            self.df = self.df[~((self.df[num_cat]<(q1-1.5*iqr)) | (self.df[num_cat]>(q3+1.5*iqr)))]
            self.df = self.df.dropna().reset_index(drop=True)

    @staticmethod
    def yeojohnson_transform(df):
        """
        Uses the Yeo Johnson Transform method to find the skew of each dataframe column

        Parameters:
        ------------
        df (dataframe)      Pandas dataframe

        Returns:
        --------
        df (dataframe)      Pandas dataframe whose columns have had the Yeo Johnson Transform method applied to
        """
        
        for df_column in df.columns:
            df[df_column] = (stats.yeojohnson(df[df_column])[0]).copy()
        
        return df