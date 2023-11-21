import pandas as pd
import sqlalchemy

with open('credentials.yaml') as file:
    credentials_dict = file.read()
  
class RDSDatabaseConnector:
  
    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(self):
        engine = sqlalchemy.create_engine("postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}".format(**self.credentials))
        
    def extract_data(self):
        loan_payments = pd.DataFrame(data=self.initialise_database())

credentials = RDSDatabaseConnector(credentials_dict)
new_data = credentials.initialise_database()