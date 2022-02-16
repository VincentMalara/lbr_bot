from src.html_parsers.rbe.parser import main as parser

def main(RCS=None, mongo='', mongoparsed=''):
    if RCS is None:
        RCSDF = mongo.find()
    else:
        RCSDF = mongo.find_from_RCSlist(RCS)

    RCSparsed = RCSDF.apply(parser, axis=1).to_list()


    mongoparsed.insert(RCSparsed)
    mongoparsed.drop_duplicates(colsel='task_index', coldup='RCS')

if __name__ == '__main__':
    main()