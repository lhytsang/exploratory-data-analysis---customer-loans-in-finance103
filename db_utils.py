import pandas as pd
import sqlalchemy 
import yaml
import re

with open('credentials.yaml') as file:
    credentials_dict = yaml.safe_load(file)
  
class RDSDatabaseConnector:
  
    def __init__(self, credentials):
        self.credentials = credentials

    def initialise_database(self):
        engine = sqlalchemy.create_engine(f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}")
        sql = "SELECT * FROM loan_payments"
        df = pd.read_sql(sql, con = engine, index_col='id')

        return df
    
    def save_file(self, dataframe):
        dataframe.to_csv('new_file.csv')

class DataTransform:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def fill_zeros(self, dataframe, column_name):
        dataframe[column_name] = dataframe[column_name].fillna(0)

        return column_name
    
    def change_type(self, dataframe_column, data_type):
        dataframe_column = dataframe_column.astype(data_type)

        return dataframe_column
    
    def date_data(self, dataframe, dataframe_column):
        dataframe[dataframe_column] = pd.to_datetime(dataframe[dataframe_column], format = 'mixed')

        return dataframe_column

credentials = RDSDatabaseConnector(credentials_dict)
loan_payments = credentials.initialise_database()

def load_csv(file):
    return pd.read_csv(file)
credentials.save_file(loan_payments)
database = pd.read_csv('new_file.csv', index_col = 'id')

cleaned_data = DataTransform(database)

date_data = ['issue_date', 'earliest_credit_line', 'last_payment_date', 'next_payment_date',
             'last_credit_pull_date']

categorical_data = ['grade', 'sub_grade', 'home_ownership', 'verification_status', 'loan_status', 
                    'payment_plan', 'purpose', 'application_type']
fixed_data = ['id', 'member_id']
string_data = ['term', 'employment_length']

for str_columns in string_data:
    for string in database[str_columns]:
        string = re.sub('\D', '', str(string))

non_numeric_data = date_data + categorical_data + fixed_data
column_headings = database.columns.values.tolist()
numeric_data = [column for column in column_headings if column not in non_numeric_data]

for date_column in date_data:
    database[date_column] = cleaned_data.date_data(database, date_column)

for numeric_column in numeric_data:
    database[numeric_column] = cleaned_data.fill_zeros(database, numeric_column)

print(database.info())