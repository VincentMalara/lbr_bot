import pandas as pd

from src.pdf_parsers.financials.utils import *


def main(row, task_index=-1):
    dictout = {}
    if isinstance(row, pd.core.series.Series):
        row = dict(row)
    if isinstance(row, dict):
        text = ' '.join([item for item in row['file']['content'].split('\n') if item])

        for key in ['RCS', 'N_depot', 'depot', 'extraction_date', 'Type_de_depot', 'Detail']:
            dictout[key] = row[key]

        data = regex_loop(text)  # numbers from bilan
        infobilan = check_bilan_new(data)  # analysis of bilan's type

        dictout['type compte resultat'] = infobilan['cr_type']
        dictout['type bilan'] = infobilan['bilan_type']
        dictout['year'] = get_year(row['Detail'])
        dictout['correction'] = get_correction(row['Type_de_depot'])
        dictout['task_index'] = task_index

        dictout = {**dictout, **fonction_cafe(dictout),
                 **data}  # merg bilan dictionnary and output od fonction_cafe which is also a dictionay
    else:
        print(f"error ar finnacials.parser: input is not a dict")

    return dictout