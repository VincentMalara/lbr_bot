import pandas as pd
import numpy as np
import sys


def main(rcs, n):
    print(f'RCS spliter : spliting RCS in {len(rcs) // n + 1} list')
    if isinstance(rcs, pd.DataFrame): #conversion from DF to list if needed
        if 'RCS' in rcs.columns:
            rcs = rcs['RCS'].to_list()
            print('RCS spliter : conversion from dataframe to list')
        else:
            print('error at RCS spliter : no RCS column id DF')
            #sys.exit
    if isinstance(rcs, list):
        rcs_arrays = np.array_split(rcs, len(rcs) // n + 1)
        rcs_list = [list(array) for array in rcs_arrays]
        #print(rcs_list)
    else:
        print(f'error at RCS spliter : {type(rcs)} not accepted as input (only list or DataFrame)')
        #sys.exit
    print('split done')
    return rcs_list
