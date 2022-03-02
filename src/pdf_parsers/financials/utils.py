import re
from .dictionaries import *

def get_year(x):
    try:
        year = int(x.split('/')[-1].split(' ')[0])
    except Exception:
        year = ''
    return year


def get_correction(x):
    y = False
    if "rectificatif" in str.lower(x):
        y = True
    return y


def check_bilan_new(dictin):
    bilan_type = None
    CR_type = None
    keys = dictin.keys()

    if keys:
        for i in LIST_LABEL_BILAN_ABREGE:
            if i in keys:
                bilan_type = "abrege"
                break
        for i in LIST_LABEL_CR_ABREGE:
            if i in keys:
                CR_type = "abrege"
                break

    if bilan_type == "abrege":
        for i in LIST_LABEL_BILAN_COMPLET:
            if i in keys:
                bilan_type = "complet"
                break

    if CR_type == "abrege":
        for i in LIST_LABEL_CR_COMPLET:
            if i in keys:
                CR_type = "complet"
                break

    return {'bilan_type': bilan_type, 'cr_type': CR_type}


def regex_loop(text):
    dict = {}
    for i in range(400):
        courant = ''
        a = str(1101 + i * 2)
        b = str(101 + i * 2)
        c = str(101 + i * 2 + 1)
        if b not in ['201', '405']:
            regex = a + REGEXBLOC + b + REGEXBLOC2 + c
            bb = re.findall(regex, text)
            try:
                bb = re.sub(a, "", bb[0], count=1).strip()
                ll = len(c) * -1
                bb = bb[:ll]
                courant = re.split(b + " ", bb)[0]
                prec = re.split(b + " ", bb)[1]
            except Exception:
                pass
        else:
            try:
                regex = REGEXBLOC + b + REGEXBLOC2 + c
                bb = re.findall(regex, text)
                bb = bb[0].strip()
                ll = len(c) * -1
                bb = bb[:ll]
                courant = re.split(b + " ", bb)[0]
                prec = re.split(b + " ", bb)[1]
            except Exception:
                pass

        if courant:
            try:
                dict[b] = float(courant.replace(".", "").replace(",", "."))
            except:
                pass
    return dict


def fonction_cafe(dico):
    calculus_sheet = {}
    if dico['type bilan'] == 'abrege':
        if dico['type compte resultat'] == 'abrege':
            calculus_sheet = SC_BAB_CRAB
        elif dico['type compte resultat'] == 'complet':
            calculus_sheet = SC_BAB_CRC
        else:
            calculus_sheet = SC_BAB_ELSE
    elif dico['type bilan'] == 'complet':
        if dico['type compte resultat'] == 'abrege':
            calculus_sheet = SC_BC_CRAB
        elif dico['type compte resultat'] == 'complet':
            calculus_sheet = SC_BC_CRC
        else:
            calculus_sheet = SC_BC_ELSE
    result = {}
    for key, value in calculus_sheet.items():
        clean_value = value.replace(' ', '')
        if key == 'passif_circulant':
            if '405' in dico:
                if '301' in dico:
                    result[key] = dico['405'] - dico['301']
                else:
                    result[key] = dico['405']
            elif '201' in dico:
                if '301' in dico:
                    result[key] = dico['201'] - dico['301']
                else:
                    result[key] = dico['201']

        elif key == 'tresorerie_net':
            if 'fond_roulement' in result and 'besoin_fond_roulement' in result:
                result[key] = result['fond_roulement'] - result['besoin_fond_roulement']
            else:
                temp = value.replace('-', '+')
                temp = temp.replace('/', '+')
                temp = temp.replace('(', '')
                temp = temp.replace(')', '')
                temp = temp.replace(' ', '')
                temp = temp.split('+')
                if any(val in dico for val in temp):
                    operation = value
                    for val in temp:
                        if val in dico:
                            operation = operation.replace(' ' + val + ' ', str(dico[val]))
                        else:
                            operation = operation.replace(' ' + val + ' ', '0')

                    try:
                        result[key] = eval(operation)
                    except Exception:
                        pass

        elif any(operator in value for operator in ('+', '-', '/', '(')):
            temp = value.replace('-', '+')
            temp = temp.replace('/', '+')
            temp = temp.replace('(', '')
            temp = temp.replace(')', '')
            temp = temp.replace(' ', '')
            temp = temp.split('+')

            if any(val in dico for val in temp):
                operation = value
                for val in temp:
                    if val in dico:
                        operation = operation.replace(' ' + val + ' ', str(dico[val]))
                    else:
                        operation = operation.replace(' ' + val + ' ', '0')

                try:
                    result[key] = eval(operation)
                except Exception:
                    pass
            elif clean_value in dico:
                result[key] = dico[clean_value]

            if 'resultat_net_669' in result:
                if 'resultat_net_321' in result:
                    result['resultat_net'] = result['resultat_net_321']
                    del result['resultat_net_321'], result['resultat_net_669']
                else:
                    result['resultat_net'] = result['resultat_net_669']
                    del result['resultat_net_669']
            else:
                if 'resultat_net_321' in result:
                    result['resultat_net'] = result['resultat_net_321']
                    del result['resultat_net_321']

            for key_ in result.keys():
                if isinstance(result[key_], float):
                    result[key_] = round(result[key_], 2)

            # added ticket 361
            if 'captiaux_propres' in result:
                if result['captiaux_propres'] != 0:
                    if 'dettes' in result:
                        result['ratio_endettement'] = result['dettes'] / result['captiaux_propres']

    return result