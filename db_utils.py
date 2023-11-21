import pandas as pd
import sqlalchemy
import yaml

with open('credentials.yaml') as file:
    credentials_dict = yaml.safe_load(file)
  
class RDSDatabaseConnector:
  
    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(self):
        engine = sqlalchemy.create_engine(f"postgresql://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}")
        loan_payments = pd.DataFrame(data=engine)

credentials = RDSDatabaseConnector(credentials_dict)
new_data = credentials.initialise_database()