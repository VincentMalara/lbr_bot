import pandas as pd


def main(Mongo):
    changed = Mongo.find({"changed_RCS_number": 'old_one'})
    raw = Mongo.find_from_RCSlist(RCS=changed['Replaced by'].to_list())
    tobechanged = pd.DataFrame()
    tobechanged['RCS'] = changed['Replaced by']
    tobechanged['old RCS'] = changed['RCS']
    raw = raw.merge(tobechanged, on='RCS', how='left')
    raw['task_index'] = raw['task_index']+1
    raw['changed_RCS_number'] = 'new_one'
    Mongo.insert(raw)
    Mongo.drop_duplicates(colsel='task_index', coldup='RCS')
    print('Changed RCS Updated')