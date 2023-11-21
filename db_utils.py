class RDSDatabaseConnector:
  with open('credentials.yaml') as file:
    credentials_dict = file.read()
