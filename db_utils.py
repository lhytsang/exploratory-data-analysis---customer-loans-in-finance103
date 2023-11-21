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
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
        df = pd.read_sql(sql, con = engine)

credentials = RDSDatabaseConnector(credentials_dict)
new_data = credentials.initialise_database()