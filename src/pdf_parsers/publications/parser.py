import pandas as pd

from src.pdf_parsers.publications.utils import *


def main(row, task_index=-1):
    dictout = {}
    if isinstance(row, pd.core.series.Series):
        row = dict(row)
    if isinstance(row, dict):
        #text = ' '.join([item for item in row['file']['content'].split('\n') if item])

        file = row['file']


        try:
            dictout['splitted_file'] = [item for item in file['content'].split('\n') if item]
            test = True
        except:
            dictout['splitted_file'] = []
            test = False

        dictout['depot'] = row['depot']
        dictout['Detail'] = row['Detail']
        dictout['Date'] = row['Date']

        if test:
            dictout = split_and_parse(dictout)

        for key in ['RCS', 'N_depot','Type_de_depot']: #'extraction_date',
            dictout[key] = row[key]

        dictout['task_index'] = task_index
        del dictout['splitted_file']

    else:
        print(f"error ar publications.parser: input is not a dict")

    return dictout