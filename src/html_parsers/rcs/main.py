from src.html_parsers.rcs.parser import main as parser
from .manage_changed_RCS import main as manage_changed_RCS

def main(RCS=None, mongo='', mongoparsed=''):
    if RCS is None:
        RCSDF = mongo.find()
    else:
        RCSDF = mongo.find_from_RCSlist(RCS)

    RCSparsed = RCSDF.apply(parser, axis=1).to_list()


    mongoparsed.insert(RCSparsed)
    mongoparsed.drop_duplicates(colsel='task_index', coldup='RCS')
    manage_changed_RCS(mongoparsed)

if __name__ == '__main__':
    main()