import pandas as pd
from datetime import datetime

def clean_list_dict_nan(x): #allow to delete nan or empty value in a dict or a list of dict
    if isinstance(x, dict):
        y = {k: x[k] for k in x if not x[k] == ''}
    elif isinstance(x, list):
        y=[]
        for dict_ in x:
            if isinstance(dict_, dict):
                clean_dict = {k: dict_[k] for k in dict_ if not dict_[k] == ''}
                y.append(clean_dict)
            else:
                y.append({})
    return y

def insert_empty_RCS(rcs, mongo):
    if isinstance(rcs, str):
        rcs = [rcs]
    if isinstance(rcs, list):
        existing = mongo.get_RCSlist(rcs)
        tobecreated = [rcs for rcs in rcs if rcs not in existing]
        if len(tobecreated)>0:
            DF = pd.DataFrame()
            DF['RCS'] = rcs
            DF['extraction_date'] = datetime.today().strftime("%d/%m/%Y")
            DF['status'] = "to_be_updated"
            DF['info'] = ''
            DF['scraper_version'] = ''
            DF['task_index'] = 0
            mongo.insert(DF)
        else:
            print(f"info at insert_empty_RCS: no RCS to input")
    else:
        print(f"error at insert_empty_RCS: {type(rcs)} cannot be used as input")