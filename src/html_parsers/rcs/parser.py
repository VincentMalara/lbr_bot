from datetime import datetime
import pandas as pd

import bs4 as beautifulsoup

from configs import settings
from src.html_parsers.utils import clean_spaces, translator, get_pdflink
from ..utils import replaced_RCS



def main(row, task_index=-1):
    dictout = {}
    if isinstance(row, pd.core.series.Series):
        row = dict(row)
    if isinstance(row, dict):
        dictout['RCS'] = row['RCS']
        dictout['ToRescrap'] = False
        dictout['extraction_date'] = row['extraction_date']
        dictout['parsing_date'] = datetime.today().strftime("%d/%m/%Y")
        dictout['parser_version'] = settings.parser_version
        dictout['task_index'] = task_index

        if 'info' in row.keys():
            dictout['exists'] = True
            infos = str(row['info'])
            Content = beautifulsoup.BeautifulSoup(infos.replace('<br/>', ' '),features="html.parser")

            try:
                compdetails = Content.find_all('div', {"id": "companyDetails"})[0]
                for cd in compdetails.find_all('li'):
                    details = cd.get_text().strip().split('\n')
                    if len(details) > 1:
                        dictout[details[0].strip()] = ' '.join(details[1].split()).strip()
                    else:
                        pass
            except:
                dictout['ToRescrap'] = True

            if Content.find_all('div', {"id": "succursales"}):
                AAA = Content.find_all('div', {"id": "succursales"})[0]
                succs = []
                for succursale in AAA.find_all('ul'):
                    succ = {}
                    succ['name'] = succursale.find('h3').get_text()
                    for i in succursale.find_all('li'):
                        AA = i.get_text().strip().split('\n')
                        if len(AA) > 1:
                            if succ['name'] in AA:
                                pass
                            else:
                                succ[AA[0].strip()] = ' '.join(AA[1].split()).strip()
                        else:
                            pass
                    succs.append(succ)
                dictout['succursales'] = succs

            try:
                withininfoout = Content.find_all('div', {"class": "withInfoOut"})[0]
            except Exception:
                withininfoout = []

            h1red = ""
            h1 = ""

            try:
                h1red_ = withininfoout.find_all('span', {"class": "h1Red"})[0]
            except Exception:
                h1red_ = ""
            if h1red_:
                h1red = h1red_.get_text().translate(translator)
                h1red = ' '.join(h1red.split()).strip()
                if h1red:
                    dictout['company status'] = h1red

            try:
                h1_ = withininfoout.find_all('h1')[0]
            except Exception:
                h1_ = ""
            if h1_:
                h1 = h1_.get_text().translate(translator)
                if h1red:
                    h1 = h1.replace(h1red, "")
                h1 = h1.replace(row['RCS'], "")
                h1 = h1.replace(",", "")
                h1 = ' '.join(h1.split()).strip()
                if h1:
                    dictout['company name'] = h1

            depot = {}
            depots = []

            try:
                tableau = Content.find_all('tbody')[0]
                test_tableau = True
            except Exception:
                test_tableau = False

            if test_tableau:
                for ligne in tableau.find_all('tr'):
                    elements = ligne.find_all('td')
                    # print(elements[0].get_text())
                    if len(elements) > 4:
                        depot = {}
                        depot['N_depot'] = clean_spaces(elements[0].get_text())
                        depot['Date'] = clean_spaces(elements[1].get_text())
                        depot['Type_de_depot'] = clean_spaces(elements[2].get_text())
                        depot['Detail'] = clean_spaces(elements[3].get_text())
                        try:
                            depot['depot'] = get_pdflink(elements[4])
                        except Exception:
                            depot['depot'] = 'empty'
                            print(f"One empty dépots for RCS:  {str(row['RCS'])}s")
                        depots.append(depot)
                        # print(depots)
                if depot:
                    dictout['depots'] = depots
                else:
                    #logger.info(f"complete empty dépots for RCS:  {str(row['RCS'])}")
                    dictout['ToRescrap'] = False
                    #print(f"complete empty dépots for RCS:  {str(row['RCS'])}")
            else:
                #logger.info(f"complete empty dépots for RCS:  {str(row['RCS'])}, no <tr> found")
                print(f"complete empty dépots for RCS:  {str(row['RCS'])}, no <tr> found")
        else:
            dictout['exists'] = False

        if 'company name' in dictout.keys():
            if " Le numéro RCS saisi a été remplacé par le numéro RCS" in dictout['company name']:
                dictout['Replaced by'], dictout['company name'] = replaced_RCS(dictout['company name'])
                dictout['changed_RCS_number'] = 'old_one'
            else:
                dictout['changed_RCS_number'] = 'no'

    else:
        print(f"error ar rcs.parser: input is not a dict")

    #print(dictout)

    return dictout