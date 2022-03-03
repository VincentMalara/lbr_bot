import pandas as pd

def main(RCS=''):
    msg = 'no issue'
    status = False
    outputrcs_list = []
    if isinstance(RCS, list):
        #print('list')
        outputrcs_list = RCS
        dict_ = {"RCS": {'$in': outputrcs_list}}
        status = True
    elif isinstance(RCS, str):
        #print('str')
        outputrcs_list = [RCS]
        dict_ = {"RCS": RCS}
        status = True
    elif isinstance(RCS, pd.DataFrame): # in case input RCS is a dataframe
        #print("mongo.findRCS in dataframe mode")
        if RCS.shape[0] > 0:
            if 'RCS' in RCS.columns:
                outputrcs_list = RCS['RCS'].to_list()
                dict_ = {"RCS": {'$in': outputrcs_list}}
                status = True
            else:
                msg = 'error at rcs.scrap_list : No RCS column in input DF'
        else:
            msg = 'error at rcs.scrap_list : empty input DF'
    else:
        msg = f'error at rcs.scrap_list : not accepted format {type(RCS)} for input. str, list or DF only'
    return outputrcs_list, dict_,  status, msg