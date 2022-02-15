import pandas as pd

def HandleRcsList(rcs_list):
    msg = 'no issue'
    status = False
    outputrcs_list = []
    if isinstance(rcs_list, list):
        print('list')
        outputrcs_list = rcs_list
        status = True
    elif isinstance(rcs_list, str):
        print('str')
        outputrcs_list = [rcs_list]
        status = True
    elif isinstance(rcs_list, pd.DataFrame): # in case input RCS is a dataframe
        print("mongo.findRCS in dataframe mode")
        if rcs_list.shape[0] > 0:
            if 'RCS' in rcs_list.columns:
                outputrcs_list = rcs_list['RCS'].to_list()
                status = True
            else:
                msg = 'error at rcs.scrap_list : No RCS column in input DF'
        else:
            msg = 'error at rcs.scrap_list : empty input DF'
    else:
        msg = f'error at rcs.scrap_list : not accepted format {type(rcs_list)} for input. str, list or DF only'
    return outputrcs_list, status, msg