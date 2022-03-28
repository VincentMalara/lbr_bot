from src.mongo.main import rcs_input_checker
from src.pdf_parsers.financials.parser import main as parser


def main(RCS=None, mongo='', mongoparsed='', onlynew=True):
    task_index = mongoparsed.get_index_max() + 1
    dict_ = {'Type_de_depot': {'$regex': "Comptes annuels .eCDF"}}
    if RCS is not None:
        list_, dict_rcs, status, msg = rcs_input_checker(RCS=RCS) #, fct_name='financials.parser')
        dict_ = {**dict_, **dict_rcs}

    RCSDF = mongo.find(dict_)

    if onlynew:
        alreadydone = mongoparsed.find_from_RCSlist(RCSDF)
        if 'N_depot' in alreadydone.columns:
            if alreadydone.shape[0] > 0:
                dict_on = {"N_depot": {'$nin': alreadydone['N_depot'].to_list()}}
                dict_ = {**dict_, **dict_on}
                RCSDF = mongo.find(dict_)
    else: #--> reparse all in ths case
        print("All bilan will be reparsed")
        mongoparsed.delete(dict_rcs)
        task_index = -1

    if RCSDF.shape[0] > 0:
        RCSparsed = RCSDF.apply(lambda x: parser(x, task_index), axis=1).to_list()
        mongoparsed.insert(RCSparsed)
        mongoparsed.drop_duplicates(colsel='task_index', coldup='N_depot')
        N = len(RCSparsed)
    else:
        print("error in pdf parser financials : empty dataframe")

    return N


if __name__ == '__main__':
    main()