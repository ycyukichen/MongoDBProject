from typing import Any
import os
import pandas as pd
import pymongo
import json
from ensure import ensure_annotations
from pymongo.mongo_client import MongoClient



class mongo_operation:
    __collection=None # here i have created a private/protected variable
    __database=None
    
    def __init__(self, client_url: str, database_name:str=None, collection_name:str=None):
        self.client_url  = client_url
        self.database_name = database_name
        self.collection_name = collection_name
        
    def create_client(self):
        client = MongoClient(self.client_url)
        return client
    
    def create_database(self):
        client = self.create_client()
        db = client[self.database_name]
        return db
    
    def create_collection(self):
        db = self.create_database()
        collection = db[self.collection_name]
        return collection
    
    def insert_record(self, collection_name:str, data):
        collection = self.create_collection()
        if type(data) == list:
            for d in data:
                if type(d) != dict:
                    raise ValueError("Data should be in dictionary format")
            collection.insert_many(data)
        elif type(data) == dict:
            collection.insert_one(data)
        else:
            raise ValueError("Data should be in dictionary format")
        
    def find_all_records(self, collection_name:str):
        collection = self.create_collection()
        records = list(collection.find())  
        for r in records:
            print(r)
    
    def find_records(self, collection_name:str, query:dict):
        collection = self.create_collection()
        records = list(collection.find(query))  
        for r in records:
            print(r)
            
    def insert_in_bulk(self, datafile:str, collection_name:str):
        self.path = datafile
        
        if self.path.endwith(".json"):
            with open(self.path, "r") as file:
              data = json.load(file)

            
        elif self.path.endswith(".cvs"):
            data = pd.read_csv(self.path, encoding='utf-8')
            
        elif self.path.endswith(".xlsx"):
            data = pd.read_excel(self.path, encoding='utf-8')
            
        datajson = json.loads(data.to_json(orient='records'))
        collection = self.create_collection()
        collection.insert_many(datajson)