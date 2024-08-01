import pandas as pd
import sqlalchemy 

class RDSDatabaseConnector:
    """
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
    """

    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(self):
        """
        Establishing the dataset from the credentials given when calling the class and returning the data as a Pandas dataframe
        
        Returns:
        --------
        Pandas Dataframe containing the financial details and loan payments of a banks's customers
        """
        
        engine = sqlalchemy.create_engine(f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}")
        sql = "SELECT * FROM loan_payments"
        df = pd.read_sql(sql, con = engine)

        return df
    
    @staticmethod
    def load_csv(file):
        """
        Accessing the data given in the file provided. 

        
        Parameters:
        ------------
        file (str)     the filename of the document we wish to retrieve the data from

        Returns:
        --------

        """
        
        return pd.read_csv(file, index_col=[0])

    @staticmethod
    def save_file(df, filename):
        """
        Making a copy of the dataframe on the local machine as a CSV file and labelling the document as 'filename'

        
        Parameters:
        ------------
        df (dataframe)  the dataframe which we want to make a copy of
        filename (str)  the name that we want to save the file as
        """
        
        df.to_csv(filename)
