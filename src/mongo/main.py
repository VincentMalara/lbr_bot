import pymongo
import pandas as pd

from configs import settings
from .utils import clean_list_dict_nan

mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCS = settings.col_RCS


class mongo():
    def __init__(self, db=mongo_DB,  col=col_RCS):
        self.db = db
        self.col = col
        self.conn = pymongo.MongoClient(mongo_ip, mongo_port)
        print('connected to DB:' + self.db)
        print('connected to collection:' + self.col)
        self.collection = self.conn[db][col]

    def find(self, dictin=None, dictout=None): #take as input: dictin: input mongo query, dictout: output columns
        if dictout is None:
            dictout = {}
        if dictin is None:
            dictin = {}
        if dictout == {}: #will return complete collection
            df_found = pd.DataFrame(list(self.collection.find(dictin)))
        else:
            df_found = pd.DataFrame(list(self.collection.find(dictin, dictout))) #in case you need only few columns
        return df_found

    def find_RCS(self, RCS=None, only=False): #take as input a RCS number, a list or a DF with a RCS column
        if RCS is None:
            print('error at mongo.findRCS : at least a RCS  number is needed')
        else:
            dict_ = None
            if isinstance(RCS, pd.DataFrame): # in case input RCS is a dataframe
                print("mongo.findRCS in dataframe mode")
                if 'RCS' in RCS.columns:
                    dict_ = {"RCS": {'$in': RCS['RCS'].to_list()}}
                else:
                    print("mongo.findRCS there is no RCS columns in dataframe")
            elif isinstance(RCS, list): # in case input RCS is a list
                dict_ = {"RCS": {'$in': RCS}}
                print("mongo.findRCS in list mode")
            elif isinstance(RCS, str): # in case input RCS is a single value
                dict_ = {"RCS":  RCS}
                print("mongo.findRCS in unique mode")
            else:
                print('error at mongo.findRCS : not accepted input format. DF, list or dict accepted')

            if dict_ is not None:
                if only:
                    df_found = self.find(dict_,{'RCS': 1, '_id': 0}) #in case you need only RCS list
                    if df_found.shape[0] == 0:
                        print("mongo.findRCS returned empty result")
                    else:
                        df_found = df_found['RCS'].to_list()
                else:
                    df_found = self.find(dict_)
                    if df_found.shape[0]==0:
                        print("mongo.findRCS returned empty result")
            else:
                df_found = []
        return df_found

    def insert(self, data, col=None): #col is used in case RCS is not a unique key such as for financials and input is a DF
        status = False

        if col is not None:
            if not isinstance(data, pd.DataFrame):
                print(f'info at mongo.insert : {col} will not be used as input is not a dataframe')
            if isinstance(data, pd.DataFrame):
                if col not in data.columns:
                    print(f'info at mongo.insert : {col} will not be used as it s not a column from the dataframe')
        else:
            if isinstance(data, pd.DataFrame):
                if 'RCS' in data.columns:
                    col = 'RCS'
                    print(f'info at mongo.insert : RCS will be used instead')

        if isinstance(data, list):
            data = pd.DataFrame(data)

        if isinstance(data, pd.DataFrame):
            if data.shape[0] > 0:
                if '_id' in data.columns:
                    data = data.drop(columns='_id')
                dictout = clean_list_dict_nan(data.fillna('').to_dict('records'))
                self.collection.insert_many(dictout)
            else:
                print('error at mongo.insert : input DF is empty')
        elif isinstance(data, dict):
            self.collection.insert(data)
        else:
            print('error at mongo.insert : not accepted input format. DF, list or dict accepted')
            status = False
        return status

    def delete(self, data=None):
        if data is None:
            data = {}
        status = False
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
            for row in data:
                self.collection.delete_many(row)
        elif isinstance(data, list):
            for row in data:
                self.collection.delete_many(row)
        elif isinstance(data, dict):
            self.collection.delete_many(data)
        else:
            print('error at mongo.delete : not accepted input format. DF, list or dict accepted')
            status = False
        return status

    def update(self, query, newdatas):
        status = False
        if isinstance(newdatas, dict): #check if new datas is a dict
            if isinstance(query, dict): #in case query is a dict
                self.collection.update_many(query, {"$set": newdatas})
            elif isinstance(query, pd.DataFrame): #in case input RCS is a dataframe
                print("mongo.findRCS in dataframe mode")
                if 'RCS' in query.columns: #in case it's a DF, check if there is a RCS
                    dict_ = {"RCS": {'$in': query['RCS'].to_list()}}
                    self.collection.update_many(dict_, {"$set": newdatas})
                else:
                    print("mongo.findRCS there is no RCS columns in dataframe")
            elif isinstance(query, list): #in case input RCS is a list
                dict_ = {"RCS": {'$in': query}}
                self.collection.update_many(dict_, {"$set": newdatas})
            else:
                print('error at mongo.update : not accepted input format: dict accepted')
                status = False
        else:
            print('error at mongo.update : not accepted update input format: dict accepted')
            status = False
        return status

    def drop_duplicates(self, col=None):
        DF = self.find()
        if DF.shape[0] > 0:
            if "extraction_date" in DF.columns:
                DF = DF.sort_values(by="extraction_date", ascending=True).reset_index(drop=True)
            if col is None:
                if "RCS" in DF.columns:
                    col = 'RCS'
                else:
                    if "_id" in DF.columns:
                        print('id')
                        self.delete({"_id": {'$in': list(DF['_id'].unique())}})
                    else:
                        self.delete(DF.to_dict('records'))
                    self.insert(DF.drop_duplicates(keep='last'))

            if col is not None:
                if col in DF.columns:
                    DF = DF.sort_values(by=[col, "extraction_date"], ascending=True).reset_index(drop=True)
                    self.delete({col: {'$in': list(DF[col].unique())}})
                    self.insert(DF.drop_duplicates(subset=[col], keep='first'))
                else:
                    print(f'error at mongo.drop_duplicates : {col} not found in {self.col}')
        else:
            print('at mongo.drop_duplicates : no duplicates')

    def close(self): #close the connection  when finish
        print(f'closing {self.db} , {self.col}')
        self.conn.close()