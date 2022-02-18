import pymongo
import pandas as pd
import sys

from configs import settings
from .utils import clean_list_dict_nan

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
            dict_ = None
            if isinstance(RCS, pd.DataFrame): # in case input RCS is a dataframe
                print("mongo.find_from_RCSlist in dataframe mode")
                if 'RCS' in RCS.columns:
                    dict_ = {"RCS": {'$in': RCS['RCS'].to_list()}}
                else:
                    print("mongo.find_from_RCSlist there is no RCS columns in dataframe")
            elif isinstance(RCS, list): # in case input RCS is a list
                dict_ = {"RCS": {'$in': RCS}}
                print("mongo.find_from_RCSlist in list mode")
            elif isinstance(RCS, str): # in case input RCS is a single value
                dict_ = {"RCS":  RCS}
                print("mongo.find_from_RCSlist in unique mode")
            else:
                print('error at mongo.find_from_RCSlist : not accepted input format. DF, list or dict accepted')

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
                    data['task_index'] = data['task_index'].apply(lambda x: int(x))
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

    def delete(self, data=None, RCS=False):
        if data is None:
            data = {}
        status = False
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
            for row in data:
                self.collection.delete_many(row)
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

    def set_status(self, newstatus: str = None, RCS = None): #take as input a RCS number, a list or a DF with a RCS column
        if newstatus is None:
            print('error at mongo.set_status : no newstatus set')
            sys.exit()
        else:
            updater = {'status': newstatus}
            dict_ = {}
            if RCS is None:
                print('info at mongo.set_status : no RCS set, all col will be updated')
            else:
                if isinstance(RCS, pd.DataFrame): # in case input RCS is a dataframe
                    print("mongo.find_from_RCSlist in dataframe mode")
                    if 'RCS' in RCS.columns:
                        dict_ = {"RCS": {'$in': RCS['RCS'].to_list()}}
                    else:
                        print("mongo.find_from_RCSlist there is no RCS columns in dataframe")
                elif isinstance(RCS, list): # in case input RCS is a list
                    dict_ = {"RCS": {'$in': RCS}}
                    print("mongo.find_from_RCSlist in list mode")
                elif isinstance(RCS, str): # in case input RCS is a single value
                    dict_ = {"RCS":  RCS}
                    print("mongo.find_from_RCSlist in unique mode")
                else:
                    print('error at mongo.find_from_RCSlist : not accepted input format. DF, list or dict accepted')
            self.update(dict_, updater)

    def set_to_be_updated(self, RCS = None):
        self.set_status(newstatus='to_be_updated', RCS=RCS)

    def drop_duplicates(self, colsel=None, coldup=None, seldict={}):
        DF = self.find(seldict)
        if DF.shape[0] > 0:
            if "task_index" in DF.columns:
                if colsel is None:
                    colsel = "task_index"
                    print('coldup set to task_index')
                else:
                    print('task_index not in Dataframe ')
                try:
                    DF = DF.sort_values(by="task_index", ascending=False).reset_index(drop=True)
                except Exception:
                    print('not possible to sort task_index columns')
            else:
                print('no colsel input ')

            if coldup is None:
                if "RCS" in DF.columns:
                    coldup = 'RCS'
                    print('coldup set to RCS')
                else:
                    if "_id" in DF.columns:
                        print('id')
                        self.delete({"_id": {'$in': list(DF['_id'].unique())}})
                    else:
                        print('no id delete all')
                        self.delete(DF.to_dict('records'))
                    print('insert DF after drop dup')
                    self.insert(DF.drop_duplicates(keep='last'))

            if colsel is not None:
                if colsel in DF.columns:
                    if coldup is not None:
                        if coldup in DF.columns:
                            DF = DF.sort_values(by=[coldup, colsel], ascending=True).reset_index(drop=True)
                            list_ = list(DF[coldup].unique())
                            self.delete({coldup: {'$in': list_}})
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
            index = DF['task_index'].apply(lambda x: int(x)).max()
        else:
            index = 0
            print(f'task_index not in {self.col}')
        return index


    def close(self): #close the connection  when finish
        print(f'closing {self.db} , {self.col}')
        self.conn.close()