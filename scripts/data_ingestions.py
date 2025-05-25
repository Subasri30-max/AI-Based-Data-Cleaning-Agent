import os 
import pandas as pd
import requests
from sqlalchemy import create_engine
DATA_DIR=os.path.join(os.path.dirname(os.path.abspath(__file__)),"../data")

class DataIngestion:
    def __init__(self,db_url=None):
        """Intialize data ingestion with an otional database connection"""
        self.engine=create_engine(db_url) if db_url else None
    def load_csv(self,file_name):
        """loads a csv file into a dataframe"""
        file_path=os.path.join(DATA_DIR,file_name)
        try:
            df=pd.read_csv(file_path)
            print(f"csv loaded successfully:{file_path}")
            return df
        except Exception as e:
            print(f"error loading csv:{e}")
            return None
    def load_excel(self,file_name,sheet_name=0):
        """loads an excel file into a dataframe"""
        file_path=os.path.join(DATA_DIR,file_name)
        try:
            df=pd.read_excel(file_path,sheet_name=sheet_name)
            print(f"excel loaded successfully:{file_path}")
            return df
        except Exception as e:
            print(f"error loading excel:{e}")
            return None
    def connect_database(self,db_url):
        """establishes a database connection"""
        try:
            self.engine=create_engine(db_url)
            print("database connection successful")
        except Exception as e:
            print(f"error connecting to database:{e}")
    def load_from_database(self,query):
        """fetches data from a database using sql"""
        if not self.engine:
            print("no database connection.call connect_database().first")
            return None
        try:
            df=pd.read_sql(query,self.engine)
            print("data loaded from database successfully")
            return df
        except Exception as e:
            print(f"error loading data from database:{e}")
            return None
    def fetch_from_api(self,api_url,params=None):
        """fetches data from an api and returns it as a dataframe"""
        try:
            response=requests.get(api_url,params=params)
            if response.status_code==200:
                data=response.json()
                df=pd.DataFrame(data)
                print(f"data fetched from api successfully")
                return df
            else:
               print(f"api request failed:{response.status_code}")
               return None
        except Exception as e:
             print(f"error fetching data from api:{e}")
             return None
        