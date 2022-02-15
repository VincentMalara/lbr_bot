from datetime import datetime
import pandas as pd

import bs4 as beautifulsoup

from src.html_parsers.utils import clean_spaces, translator, get_pdflink
from configs import settings
from src.utils.set_logger import main as set_logger

translator = str.maketrans({chr(10): '', chr(9): ''})
ckeck_loi2004="Personnes inscrites en application de l'article 1er paragraphe 7"
check_aucun_benef_eco="Aucun bénéficiaire effectif"
check_succurs="Succursale(s) luxembourgeoise(s) de "
check_marche="Informations relatives au marché réglementé"

logger = set_logger()


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
        if isinstance(row['info'], str):
            Content = beautifulsoup.BeautifulSoup(row['info'].replace('<br/>', ' '),features="html.parser")

            if "Vous n'avez pas accès aux formalités de dépôt électroniques." in Content.get_text():
                dictout['ToRescrap'] = True
                print("Vous n'avez pas accès aux formalités de dépôt électroniques.")
            else:
                dictout['ToRescrap'] = False
                for li in Content.find_all('li'):
                    li_text = li.get_text().strip().split('\n')
                    if len(li_text) > 1:
                        dictout[li_text[0].strip()] = ' '.join(li_text[1].split()).strip()
                    else:
                        pass

                div_text_base = ''
                for div in Content.find_all('div'):
                    if 'Date de la dernière déclaration' in div_text_base:
                        div_text = div.get_text()
                        dictout['Date de la dernière déclaration'] = div_text.strip()
                        break
                    div_text_base = div.get_text()


                UBOS = []
                for tbody in Content.find_all('tbody'):
                    dict_UBO = {}
                    for td in tbody.find_all('td'):
                        lines = td.get_text().replace('\xa0', '').strip().split('\n')
                        if len(lines) > 1:
                            dict_UBO[lines[0].strip()] = lines[1].strip()
                        else:
                            pass
                    UBOS.append(dict_UBO)

                addinfo = []
                try:
                    name = dictout['Dénomination(s) ou raison(s) sociale(s)']
                except:
                    name = ""

                dictout['Loi_2004'] = False
                dictout['aucun_RBE'] = False
                company_cote = False

                for h1 in Content.find_all('h1'):
                    txt = h1.get_text().translate(translator)
                    txt = ' '.join(txt.split()).strip()

                    check_add_ai = True

                    if 'Bénéficiaires effectifs' in txt:
                        check_add_ai = False
                    else:
                        pass

                    if name:
                        if name in txt:
                            check_add_ai = False
                        else:
                            pass
                    else:
                        pass

                    if ckeck_loi2004 in txt:
                        # print(txt)
                        dictout['Loi_2004'] = True
                        check_add_ai = False
                    else:
                        pass

                    if check_succurs in txt:
                        dictout['Succur_etran'] = txt.replace(check_succurs, "")
                        check_add_ai = False
                    else:
                        pass

                    if check_marche in txt: #check_marche="Informations relatives au marché réglementé"
                        company_cote = True
                        check_add_ai = False
                    else:
                        pass

                    if check_add_ai:
                        addinfo.append(txt)
                    else:
                        pass

                for ai in Content.find_all("div", {"class": "marginLeft10"}):
                    txt = ai.get_text().translate(translator)
                    txt = ' '.join(txt.split()).strip()
                    if check_aucun_benef_eco in txt:
                        # print(txt)
                        dictout['aucun_RBE'] = True
                        check_add_ai = False
                    else:
                        if not company_cote:
                            addinfo.append(txt)  # ai.get_text().replace('\xa0','').strip()
                        else:
                            pass

                if UBOS:
                    if company_cote:
                        dictout['Entreprise cotée'] = UBOS
                    else:
                        dictout['Benef Economiques'] = UBOS

                if addinfo:
                    dictout['addinfo'] = addinfo
                else:
                    pass
        else:
            print("row[info] not a string")
            dictout['ToRescrap'] = True
    else:
        print(f"error ar rbe.parser: input is not a dict")
    return dictout
