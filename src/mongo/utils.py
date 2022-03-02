import pandas as pd

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

def rcs_input_checker(RCS='', fct_name = ''):
    dict_ = None
    list_ = None
    if isinstance(RCS, pd.DataFrame):  # in case input RCS is a dataframe
        print(f"{fct_name} in dataframe mode")
        if 'RCS' in RCS.columns:
            list_ = RCS['RCS'].to_list()
            dict_ = {"RCS": {'$in': list_}}
        else:
            print(f"{fct_name} there is no RCS columns in dataframe")
    elif isinstance(RCS, list):  # in case input RCS is a list
        list_ = RCS
        dict_ = {"RCS": {'$in': RCS}}
        print(f"{fct_name} in list mode")
    elif isinstance(RCS, str):  # in case input RCS is a single value
        dict_ = {"RCS": RCS}
        list_ = [RCS]
        print(f"{fct_name} in unique mode")
    else:
        print(f'error at {fct_name}: not accepted input format. DF, list or dict accepted')
    return list_, dict_