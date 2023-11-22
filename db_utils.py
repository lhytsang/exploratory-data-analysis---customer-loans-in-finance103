import pandas as pd
import sqlalchemy 
import yaml

with open('credentials.yaml') as file:
    credentials_dict = yaml.safe_load(file)
  
class RDSDatabaseConnector:
  
    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(self):
        engine = sqlalchemy.create_engine(f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}")
        sql = "SELECT * FROM loan_payments"
        df = pd.read_sql(sql, con = engine)

        return df
    
    def save_file(self, dataframe):
        dataframe.to_csv('new_file.csv')

credentials = RDSDatabaseConnector(credentials_dict)
loan_payments = credentials.initialise_database()

def load_csv(file):
    return pd.read_csv(file)

database = load_csv('new_file.csv')
print(database)