import matplotlib.pyplot as plt
import plotly.express as px
    
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
        
        df_cols = self.df.columns.values.tolist()
        
        for df_col in df_cols:
            x = self.df.index
            y = self.df[df_col]
            plt.scatter(x, y, alpha=0.1)
            plt.title(df_col)
            plt.show()

