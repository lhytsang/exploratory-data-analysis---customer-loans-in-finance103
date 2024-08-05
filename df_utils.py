import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px

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
        (dict) Dictionary containing the column names as the keys and their skews as the values
        """
        
        skew = {df_column: df[df_column].skew() for df_column in df.columns if df.dtypes[df_column] in ['float64', 'int64']}
        
        return skew
    
    def info(self):
        """
        Prints out information of a dataframe including index dtypes and columns, non-null values and memory usage
        
        Returns:
        --------
        (None) Summary of a dataframe

        """
        
        return self.df.info()

    def missing(self):
        """
        Works out the amount of missing values in each dataframe column and also returns it as a percentage 

        Returns:
        --------
        missing_amount (dict)   dictionary with the dataframe columns as keys and its values as a dictionary of 
                                the number of missing values and the percentage of missing values
        
        """
        missing_amount = {}

        for df_col in self.df:
            missing_amount[df_col] = []
            count = self.df[df_col].isna().sum()
            percentage_count = round(((count/self.df_shape()[0]) * 100), 2)

            missing_amount[df_col].append(count)
            missing_amount[df_col].append(percentage_count)
            
        return missing_amount
    
    @staticmethod
    def print_skew(df):
        """
        Prints out the dataframe columns as well as their corresponding skew values calculated in 'df_skew()'
        """
        
        data_skew = DataFrameInfo.df_skew(df)
        print("{:<35} {:<15}\n".format('Column name','Skew values'))

        for df_col, skews in data_skew.items():
            print("{:<35} {:<15}".format(df_col,skews))

    def stats(self):
        """
        Extracts statistical values like measures of central tendency and dispersion

        Returns:
        --------
        df_stats (Pandas Dataframe)     Dataframe containing descriptive statistics of the dataframe
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
        distinct_vals (numpy.ndarray)   Array containing the unique values of the column
        """
        
        distinct_vals = self.df[df_column].unique()
        return distinct_vals

    
class Plotter:
    """
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
    """

    def __init__(self, df):
        self.df = df

    @staticmethod
    def plot_bar(x_vals, y_vals):
        """
        Plots a bar chart with the given values

        Parameters:
        ------------
        x_vals (list)       A list of the independent variables to be plotted
        y_vals (list)       A list of the dependent variables to be plotted
        """
        
        plt.bar(x_vals, y_vals)

    def plot_boxplot(self):
        """
        Plots the box plot for all dataframe columns whose data only consists of integers and floats
        """

        numerical_data = [df_col for df_col in self.df.columns if self.df[df_col].dtype in ['int64', 'float64']]
        ax = self.df[numerical_data].plot(kind='box',figsize=(10, 5))
        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right', fontsize='x-small')

    def plot_hist(self):
        self.df.hist(xrot = 90, bins=20, figsize = (25,25))

    def plot_missing(self):
        """
        Plots the amount of missing values in each column of the dataframe in a bar chart
        """
        
        column_headings = self.df.columns.values.tolist()
        null_values = []
        for columns in column_headings:
            null_values.append(self.df[columns].isna().sum())

        self.plot_bar(column_headings, null_values)
        plt.show()

    @staticmethod
    def plot_pie(plot_values, section_names, plot_title):
        """
        Plots a pie chart using the values provided

        Parameters:
        ------------
        plot_values (list): the variables that we wish to plot
        section_names (list): a list containing the labels for each pie slice
        plot_title (str): the title of the pie chart 
        """
        
        fig = px.pie(values=plot_values, names=section_names, title=plot_title)
        fig.show()

    def plot_scatter(self):
        """
        Plots a scatter graph for all the columns of the dataframe 
        """
        
        for df_col in self.df.columns:
            x = self.df.index
            y = self.df[df_col]
            plt.scatter(x, y, alpha=0.1)
            plt.title(df_col)
            plt.show()

