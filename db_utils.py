import pandas as pd
import sqlalchemy

with open('credentials.yaml') as file:
    credentials_dict = file.read()
  
class RDSDatabaseConnector:
  
    def __init__(self, credentials):
        self.credentials = credentials
