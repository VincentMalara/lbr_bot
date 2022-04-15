from datetime import datetime
import sys

import pandas as pd
import pymongo

from configs import settings
from .utils import clean_list_dict_nan
from src.utils.handle_RCS_list import main as rcs_input_checker


mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCS = settings.col_RCS


class mongo():
    def __init__(self, db=mongo_DB,  col=col_RCS, ip=mongo_ip, port=mongo_port):
        self.db = db
        self.col = col
        self.conn = pymongo.MongoClient(ip, port)
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

    def get_RCSlist(self, dictin=None): #take as input a dict or list of RCS to check existing one
        df_found = []
        if dictin is None:
            dictin = {}
            print('info at mongo.get_RCSlist : no input dict provided, complete collection will be returned')
        if isinstance(dictin, list):
            print("info at mongo.get_RCSlist input is a list, it will be converted to a mongo query")
            dictin = {"RCS": {'$in': dictin}}
        if isinstance(dictin, dict):
            df_found = self.find(dictin, {'RCS': 1, '_id': 0}) #in case you need only RCS list
            if df_found.shape[0] == 0:
                df_found = []
                print("info at mongo.get_RCSlist returned empty result")
            else:
                df_found = df_found['RCS'].to_list()
        else:
            print(f"error at mongo.get_RCSlist input is not a dict, {type(dictin)} not accepted")
        return df_found


    def find_from_RCSlist(self, RCS=None, only=False): #take as input a RCS number, a list or a DF with a RCS column
        df_found = []
        if RCS is None:
            print('error at mongo.find_from_RCSlist : at least a RCS  number is needed')
        else:
            list_, dict_, status, msg = rcs_input_checker(RCS=RCS)
            if dict_ is not None:
                if only:
                    df_found = self.find(dict_,{'RCS': 1, '_id': 0}) #in case you need only RCS list
                    if df_found.shape[0] == 0:
                        print("mongo.find_from_RCSlist returned empty result")
                    else:
                        df_found = df_found['RCS'].to_list()
                else:
                    df_found = self.find(dict_)
                    if df_found.shape[0] == 0:
                        print("mongo.find_from_RCSlist returned empty result")
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
                if 'task_index' in data.columns:
                    data['task_index'] = data['task_index'].fillna(-1).astype(int)
                dictout = clean_list_dict_nan(data.fillna('').to_dict('records'))
                self.collection.insert_many(dictout)
            else:
                print('error at mongo.insert : input DF is empty')
        elif isinstance(data, dict):
            self.collection.insert_one(data)
        else:
            print('error at mongo.insert : not accepted input format. DF, list or dict accepted')
            status = False
        return status

    def delete(self, data=None, RCS=False):
        if data is None:
            data = {}
        status = False
        if isinstance(data, pd.DataFrame) and not RCS:
            data = data.to_dict('records')
            for row in data:
                self.collection.delete_many(row)
        elif isinstance(data, pd.DataFrame) and RCS:
            print('list of RCS')
            if 'RCS' in data.columns:
                try:
                    dict_ = {"RCS": {'$in': data['RCS'].to_list()}}
                    self.collection.delete_many(dict_)
                except Exception as e:
                    print(f'error at mongo.delete: {e}')
            else:
                print('error at delete: no RCS columns in input DF')
        elif isinstance(data, list) and not RCS:
            print('list of RCS')
            for row in data:
                try:
                    self.collection.delete_many(row)
                    print('list of dict')
                except Exception as e:
                    print(f'error at mongo.delete: {e}')
        elif isinstance(data, list) and RCS:
            print('list of RCS')
            try:
                dict_ = {"RCS": {'$in': data}}
                self.collection.delete_many(dict_)
            except Exception as e:
                print(f'error at mongo.delete: {e}')

        elif isinstance(data, dict):
            self.collection.delete_many(data)
        else:
            print('error at mongo.delete : not accepted input format. DF, list or dict accepted')
            status = False
        return status

    def update(self, query, newdatas : dict):
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

    def set_status(self, newstatus: str = None, RCS=None, dictin=None): #take as input a RCS number, a list or a DF with a RCS column and a dict
        print(f'setting status to {newstatus}')
        print(RCS)
        if newstatus is None:
            print('error at mongo.set_status : no newstatus set')
            sys.exit()
        else:
            updater = {'status': newstatus}
            if dictin is not None:
                dict_ = dictin
            else:
                dict_ = {}

            if RCS is not None:
                list_, dictrcs, status, msg = rcs_input_checker(RCS=RCS)
                dict_ = {**dict_, **dictrcs}
            self.update(dict_, updater)

    def set_to_be_updated(self, RCS = None, dictin=None):
        self.set_status(newstatus='to_be_updated', RCS=RCS, dictin=dictin)

    def drop_duplicates(self, colsel=None, coldup=None, seldict={}):

        dictout = {'RCS': 1,'extraction_date':1,"task_index":1, '_id': 0}
        if coldup is not None:
            dict2 = {coldup: 1}
            dictout = {**dictout, **dict2}
        if colsel is not None:
            dict2 = {colsel: 1}
            dictout = {**dictout, **dict2}

        DF = self.find(dictin=seldict, dictout=dictout)
        if DF.shape[0] > 0:
            if "task_index" in DF.columns:
                if colsel is None:
                    colsel = "task_index"
                    print('coldup set to task_index')
            else:
                print('task_index not in Dataframe ')
                sys.exit()

            if coldup is None:
                if "RCS" in DF.columns:
                    coldup = 'RCS'
                    print('coldup set to RCS')
                else:
                    print('RCS not in Dataframe ')
                    sys.exit()

            if colsel is not None:
                if colsel in DF.columns:
                    if coldup is not None:
                        if coldup in DF.columns:
                            duplicated = DF[DF[coldup].duplicated()].reset_index(drop=True)
                            duplicated_list = duplicated[coldup].to_list()
                            DF = self.find_from_RCSlist(duplicated_list)
                            if "task_index" in DF.columns:
                                DF = DF.sort_values(by=colsel, ascending=True).reset_index(drop=True)
                            self.delete(data=duplicated_list, RCS=True)
                            self.insert(DF.drop_duplicates(subset=[coldup], keep='last'))

                        else:
                            print(f'error at mongo.drop_duplicates : {coldup} not found in {self.col}')
                    else:
                        print(f'error at mongo.drop_duplicates : {coldup} not in input')
                else:
                    print(f'error at mongo.drop_duplicates : {colsel} not found in {self.col}')
            else:
                print(f'error at mongo.drop_duplicates : {colsel} not found in {self.col}')
        else:
            print('at mongo.drop_duplicates : no duplicates')

    def get_index_max(self):
        DF = self.find({},{'task_index': 1, '_id': 0})
        if 'task_index' in DF.columns:
            index = DF['task_index'].fillna(0).apply(lambda x: int(x)).max()
        else:
            index = 0
            print(f'task_index not in {self.col}')
        return index

    def insert_empty_RCS(self, RCS, update_existing=True):

        RCS, dict_, status, msg = rcs_input_checker(RCS=RCS)

        if isinstance(RCS, list):
            existing = self.get_RCSlist(RCS)
            tobecreated = [rcs for rcs in RCS if rcs not in existing]
            if len(tobecreated) > 0:
                DF = pd.DataFrame()
                DF['RCS'] = tobecreated
                DF['extraction_date'] = datetime.today().strftime("%d/%m/%Y")
                DF['status'] = "to_be_updated"
                DF['info'] = ''
                DF['scraper_version'] = ''
                DF['task_index'] = 0
                self.insert(DF)
            else:
                print(f"info at insert_empty_RCS: no RCS to input")
            if update_existing:
                self.set_to_be_updated(RCS=RCS)
        else:
            print(f"error at insert_empty_RCS: {type(RCS)} cannot be used as input")


    def close(self): #close the connection  when finish
        print(f'closing {self.db} , {self.col}')
        self.conn.close()