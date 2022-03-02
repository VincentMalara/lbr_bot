import pandas as pd

from src.pdf_parsers.publications.utils import *


def main(row, task_index=-1):
    dictout = {}
    if isinstance(row, pd.core.series.Series):
        row = dict(row)
    if isinstance(row, dict):
        text = ' '.join([item for item in row['file']['content'].split('\n') if item])
        print(text)
        1/0


        for key in ['RCS', 'N_depot', 'depot', 'extraction_date', 'Type_de_depot', 'Detail', 'Date']:
            dictout[key] = row[key]

        dictout['task_index'] = task_index

    else:
        print(f"error ar publications.parser: input is not a dict")

    return dictout