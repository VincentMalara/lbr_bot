from src.html_parsers.rbe.parser import main as parser
from src.mongo.main import rcs_input_checker


def main(RCS=None, mongo='', mongoparsed='', onlynew=True):
    task_index = mongoparsed.get_index_max() + 1
    dict_ = {'status':'scraped'}
    if RCS is not None:
        list_, dict_rcs = rcs_input_checker(RCS=RCS, fct_name='rbe.parser')
        dict_ = {**dict_, **dict_rcs}

    RCSDF = mongo.find(dict_)

    if onlynew:
        alreadydone = mongoparsed.find_from_RCSlist(RCSDF)
        if 'RCS' in alreadydone.columns:
            if alreadydone.shape[0] > 0:
                dict_on = {"RCS": {'$nin': alreadydone['RCS'].to_list()}}
                dict_ = {**dict_, **dict_on}
                RCSDF = mongo.find(dict_)
    else:
        if RCS is None:#--> repars all in ths case
            print("All RBE will be reparsed")
            mongoparsed.delete()
            task_index = -1

    if RCSDF.shape[0] > 0:
        RCSparsed = RCSDF.apply(lambda x: parser(x, task_index), axis=1).to_list()
        mongoparsed.insert(RCSparsed)
        mongoparsed.drop_duplicates(colsel='task_index', coldup='RCS')
    else:
        print("error in rbe parser : empty dataframe")


if __name__ == '__main__':
    main()