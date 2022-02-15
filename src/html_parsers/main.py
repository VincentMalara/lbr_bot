import pandas as pd

from .rcs.main import main as rcsparser
from .rbe.main import main as rbeparser
from src.utils.task_index import main as task_index


def rcs(x):
    y=[]
    if isinstance(x, pd.DataFrame):
        task_index_ = int(task_index(task='parser', lbrtype='rcs')) + 1
        y = x.apply(lambda x: rcsparser(x, task_index_), axis=1).to_list()
    else:
        print('error at rcs parser: input is not a DataFrame')
    return y

def rbe(x):
    y=[]
    if isinstance(x, pd.DataFrame):
        task_index_ = int(task_index(task='parser', lbrtype='rbe')) + 1
        y = x.apply(lambda x: rbeparser(x, task_index_), axis=1).to_list()
    else:
        print('error at rcs parser: input is not a DataFrame')
    return y