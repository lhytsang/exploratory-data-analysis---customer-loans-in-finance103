import pandas as pd
import sqlalchemy

with open('credentials.yaml') as file:
    credentials_dict = file.read()
  
class RDSDatabaseConnector:
  
    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(credentials):
        engine = sqlalchemy.create_engine("postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}".format(**credentials_dict))
        
