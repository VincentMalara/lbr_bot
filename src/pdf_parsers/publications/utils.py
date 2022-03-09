import re
import numpy as np
#from src.PDF_downloader.history_builder import FILE_LABEL_LIST
from src.pdf_parsers.publications.dictionaries import *

import time #TO BE DEL



def split_and_parse(dict_):
    dict_ = getlang(dict_)
    dict_ = trad_first_page(dict_)
    dict_ = trad(dict_)
    dict_ = donnees_a_modifier(dict_)
    dict_ = main_splitter(dict_)
    dict_ = parse_splitted(dict_)
    return dict_

def trad_first_page(dict_):
    output=[]
    if dict_['lang'] == 'de':
        phase1 = True
        for word in dict_['splitted_file']:
            translated = word
            if phase1:
                if translated.find('Seite') != -1:
                    translated = translated.replace('Seite', 'Page')
                for trad in TRAD_SPLITERS.keys():
                    if word == trad:
                        translated = TRAD_SPLITERS[trad]
                        break
                    elif word.find(trad) != -1:
                        translated = translated.replace(trad, TRAD_SPLITERS[trad])
                        break
                if translated.find('Page 2 / ') != -1:
                    dict_['splitted_file_start'] = output
                    phase1 = False
            output.append(translated)
        dict_['splitted_file'] = output
    return dict_

def trad(dict_):
    output=[]
    if dict_['lang']=='de':
        for word in dict_['splitted_file']:
            translated = word
            if word in REPLACE_PHRASE.keys():
                translated = REPLACE_PHRASE[word]
            else:
                for trad in REPLACE_WORD.keys():
                    if word.find(trad) != -1:
                        translated = word.replace(trad, REPLACE_WORD[trad])
                        break
            output.append(translated)
        dict_['splitted_file'] = output
    return dict_


def getlang(dict_):
    lang = 'fr'
    regexfr = r"Page \d+ / \d+"
    regexde = r"Seite \d+ / \d+"

    if re.search(regexfr, (' - ').join(dict_['splitted_file'])):
        listfr = re.findall(regexfr, (' - ').join(dict_['splitted_file']))
    else:
        listfr = []

    if re.search(regexde, (' - ').join(dict_['splitted_file'])):
        listde = re.findall(regexde, (' - ').join(dict_['splitted_file']))
    else:
        listde = []

    if len(listfr) < len(listde):
        lang = 'de'

    dict_['lang'] = lang
    dict_['fr'] = listfr
    dict_['de'] = listde

    dict_['readable'] = True
    if len(listde)+len(listfr) == 0:
        dict_['readable'] = False

    return dict_

def get_list_of_modif(list_):
    output={}
    for label in list_personne:
        output[label] = {}
        output[label]['needed_full'] = 0

    if isinstance(list_, list):
        for depot in list_:
            if isinstance(depot, dict):
                #if depot['Type_de_depot'] in FILE_LABEL_LIST or True:
                for label in list_personne:
                    if isinstance(depot['Detail'], str):
                        for pmr in PERSON_MERGED_REVERSED[label]:
                            if depot['Detail'].find(pmr) != -1:
                                output[label]['needed_full'] +=1
                                break
    return output



def main_splitter(dict_):
    page2 = DICT_DONNEES_A_MODIF['page2']

    missing = []
    Detail = []

    for i in list_personne:
        if isinstance(dict_['Detail'], str):
            if dict_['Detail'].find(i) != -1:
                Detail.append(i)

    for i in Detail:
        if i not in dict_['donnees_a_modifier']:
            missing.append(i)

    dict_['missing'] = missing
    dict_['donnees_a_modifier'] = dict_['donnees_a_modifier'] + missing

    splitted_file = dict_['splitted_file']
    splitter = dict_['donnees_a_modifier']

    if len(splitter) > 0:
        n = 0
        start = False
        dictout = {}
        AA = []
        for i in splitted_file:
            if start:
                if n < len(splitter):
                    if i == splitter[n]:
                        if n > 0:
                            dictout[splitter[n - 1]] = AA
                        AA = []
                        n += 1
                    else:
                        AA.append(i)
                else:
                    AA.append(i)
            if page2 in i:
                start = True

        if AA:
            dictout[splitter[-1]] = AA

        dict_['re_splitted'] = dictout
    return dict_


def parse_splitted(dict_):
    # in case there is no numero 'numero rue' is parse instead of 'numero' and 'rue'
    NR = False
    if 're_splitted' in dict_.keys():
        if 'Siège social' in dict_['re_splitted'].keys():
            if 'Numéro Rue' in dict_['re_splitted']['Siège social']:
                LABEL_DICT['Siège social'] = ['Numéro Rue', 'Code postal', 'Localité']
                NR = True

    for label in LABEL_DICT.keys():
        dict_ = get_data_from_subdict(label, LABEL_DICT[label], dict_)

    if 'Durée' in dict_.keys():
        if 'Durée' in dict_['Durée'].keys():
            if dict_['Durée']['Durée'] == 'Illimitée':
                if 'Date de fin' in dict_['Durée'].keys():
                    del dict_['Durée']['Date de fin']

    if 'Capital social / Fonds social' in dict_.keys():
        if 'Pourcentage, le cas échéant' in dict_['Capital social / Fonds social'].keys():
            if 'Etat de libération' in dict_['Capital social / Fonds social'].keys():
                if 'Total' in dict_['Capital social / Fonds social']['Etat de libération']:
                    del dict_['Capital social / Fonds social']['Pourcentage, le cas échéant']

    regexcapit = r"\D"

    if 'Capital social / Fonds social' in dict_.keys():
        if 'Pourcentage, le cas échéant' in dict_['Capital social / Fonds social'].keys():
            if re.search(regexcapit, dict_['Capital social / Fonds social']['Pourcentage, le cas échéant'].replace(',', '').replace('.', '')):
                del dict_['Capital social / Fonds social']['Pourcentage, le cas échéant']



    if 'Dénomination ou raison sociale' in dict_.keys():
        if 'Le cas échéant, abréviation utilisée' in dict_['Dénomination ou raison sociale'].keys():
            if 'Page ' in dict_['Dénomination ou raison sociale']['Le cas échéant, abréviation utilisée']:
                del dict_['Dénomination ou raison sociale']['Le cas échéant, abréviation utilisée']

    #activité or objet social
    if 're_splitted' in dict_.keys():
        dict_ = parse_object_new(dict_)

    # associés
    if 're_splitted' in dict_.keys():
        dict_ = get_personne(dict_)

    if NR:
        dict_['Siège social']['Rue'] = dict_['Siège social']['Numéro Rue']
        dict_['Siège social']['Numéro'] = ''
        del dict_['Siège social']['Numéro Rue']

    #dict_ = rate_completion(dict_)

    return dict_


def get_personne(dict_):
    regex1 = DICT_PERSONNE_SPLITTERS['regex1'] # N NAME name page N ✔
    regex2 = DICT_PERSONNE_SPLITTERS['regex2'] # N
    regex3 = DICT_PERSONNE_SPLITTERS['regex3'] # Page n / N
    #page = DICT_PERSONNE_SPLITTERS['page']
    raye = DICT_PERSONNE_SPLITTERS['raye']
    modifie = DICT_PERSONNE_SPLITTERS['modifie']
    demission = DICT_PERSONNE_SPLITTERS['demission']

    #print('---')
    #print(dict_['depot'])
    #print(dict_['re_splitted'])
    for type_ in DICT_PERSONNE.keys():
        new_list = []
        modif_name_list = []
        pers_dict_list = []
        regex = DICT_PERSONNE[type_]['regex']
        label = DICT_PERSONNE[type_]['label']
        nouvel_pers = regex.replace('(\d+ )*', '').replace('\d+ ', '')
        #print(nouvel_pers)

        if 're_splitted' in dict_.keys():
            if label in dict_['re_splitted'].keys():
                previous = ""
                modray = True
                modraypage = False
                dict_[label] = {}
                re_splitted = dict_['re_splitted'][label]
                for count, i in enumerate(re_splitted):
                    #print('----')
                    #print(count)
                    #print(modraypage)
                    #print(modray)


                    if re.search(regex3, previous):
                        #print("regex3", re.search(regex3, previous).group())
                        modraypage= True

                    if re.search(regex, previous) and not modraypage: #buil dlist of new based on summary page
                        modray= False
                        new = i.split('page')[0].split('Page')[0].strip()
                        new = ' '.join([xx.capitalize() for xx in new.split(' ')])
                        new_list.append(new)
                        #print("new ", new)

                    if re.search(regex3, previous) and not modraypage:
                        #print("regex3 step2", re.search(regex3, previous).group())
                        modraypage = True

                    if modraypage:

                        if previous.lower().replace(' ','') in [x.lower().replace(' ','') for x in modif_name_list]:
                            # .replace(' ','') added in case of missing spaces in the summary page
                            aa = ' '.join([xx.capitalize() for xx in previous.split(' ')])

                            if raye in i:
                                #print("raye ", raye, ' : ', aa )
                                pers_dict_list.append({'name': aa.replace(',', '').strip(), 'status':'rayé','info':get_infos_from_pers(re_splitted[count:])})
                            elif modifie in i:
                                #print("modifie ", modifie, ' : ', aa )
                                pers_dict_list.append({'name': aa.replace(',', '').strip(), 'status':'modifié','info':get_infos_from_pers(re_splitted[count:])})
                            elif demission in i:
                                #print("demission ", demission, ' : ', aa )
                                pers_dict_list.append({'name': aa.replace(',', '').strip(), 'status':'démission','info':get_infos_from_pers(re_splitted[count:])})


                        if nouvel_pers.lower() in previous.lower():
                            #print(nouvel_pers.lower(), ' = ', previous.lower())
                            for new_ in new_list:
                                if i.lower() == new_.lower():
                                    pers_dict_list.append({'name': new_.replace(',', '').strip(), 'status':'nouveau','info':get_infos_from_pers(re_splitted[count:])})
                                    new_list.remove(new_) #add new listed in new_list to pers_dict_list, by parsing its page
                                    break


                    if modray and not modraypage:
                        x = re.search(regex1, ' '.join(i.split()))
                        if x:
                            #print("regex1", x.group())
                            aa = "@@" + x.group().split('page')[0].split('Page')[0].strip() #@@ added to be sure to replace only first occurence
                            if re.search(regex2, aa):
                                #print("regex2", re.search(regex2, aa).group())
                                initnumb = re.search(regex2, aa).group()
                                aa = ' '.join([xx.capitalize() for xx in aa.replace(initnumb, '').split(' ')])
                                #print('aa:', aa)
                                modif_name_list.append(aa.strip())
                                #print(modif_name_list)



                    previous = i

                #if len(output) > 0:
                #  dict_[label]['nouveau'] = output

                #if len(output2) > 0:
                #  dict_[label]['mod ou ray'] = output2

                if len(pers_dict_list) > 0:
                    dict_[label] = pers_dict_list

                #added EY sample 26/11/2021 : Merge similar position
                if dict_[label] == {}:
                    del dict_[label]
                else:
                    if label != PERSON_MERGED[label]:
                        dict_[PERSON_MERGED[label]] = dict_[label]
                        del dict_[label]
                #end modif


    return dict_



def get_infos_from_pers(list_):
    dictout = {}
    whiletest = False
    foundpersone = False
    count = 0
    while not whiletest:
        list_ = list_[1:]
        count += 1
        #shift list until first value is in ['Personne physique', "Personne morale luxembourgeoise", "Personne morale étrangère"]
        # to remove unwanted text before format is known
        #foundpersone is used to allow next steps
        if list_[0] in ['Personne physique', "Personne morale luxembourgeoise", "Personne morale étrangère"]:
            whiletest = True
            foundpersone = True
        if count > 10:
            whiletest = True
        #############

        INFOLIST_DICT = {'Personne physique':['Nom', 'Prénom(s)', "Date de naissance", "Lieu de naissance", "Pays de naissance"],
        "Personne morale luxembourgeoise":["N° d'immatriculation au RCS"],
        "Personne morale étrangère":["Pays", "Nom du registre", "N° d'immatriculation", "Dénomination ou raison sociale",
                        "Forme juridique étrangère"]}



    if foundpersone:
        dictout['type'] = list_[0]
        previous = ''
        if dictout['type'] in INFOLIST_DICT.keys():
            infolist =INFOLIST_DICT[dictout['type']]
            subtitlelist = list(SUBPERSOINFO_DICT.keys())
            for count, i in enumerate(list_):

                if previous in infolist:
                    if previous in dictout.keys(): #in case a title is seen a second time, stop the loop
                        break
                    else:
                        if i not in infolist: #if value is not in title list --> its the value of previous title
                            dictout[previous] = i

                elif len(subtitlelist) > 0:
                    if previous in subtitlelist:
                        subdictout = get_sub_info(list_[count:], previous)
                        if len(subdictout.keys())>0: #to avoid empty subdictout
                            dictout[previous] = subdictout
                        subtitlelist.remove(previous) #remove title from list once done to avoid doing it agin
                previous = i

        # added to remove "Date d'expiration du mandat" when value is JJ/MM/AAAA meaning that it's empty and
        # "jusqu'à l'assemblée générale qui se tiendra en l'année" has to be used instead
        if dictout['type'] =='Personne physique':
            if 'Durée du mandat' in dictout.keys():
                if "Date d'expiration du mandat" in dictout['Durée du mandat'].keys():
                    if 'JJ/MM/AAAA' in dictout['Durée du mandat']["Date d'expiration du mandat"]:
                        del dictout['Durée du mandat']["Date d'expiration du mandat"]
                    else:
                        if "jusqu'à l'assemblée générale qui se tiendra en l'année" in dictout['Durée du mandat'].keys():
                            del dictout['Durée du mandat']["jusqu'à l'assemblée générale qui se tiendra en l'année"]
        #----------------------------
    #print(dictout)
    return dictout

def get_sub_info(list_, previous_):#process information from admin/asso page
    dictout = {}
    #remove usefull 'ou' or 'oder' in the splitted text
    if previous_ == 'Durée du mandat':
        if 'ou' in list_:
            list_.remove('ou')
        if 'oder' in list_:
            list_.remove('oder')
    #SUBPERSOINFO_DICT is a dict giving the list of item to find depending on type of persone
    infolist = SUBPERSOINFO_DICT[previous_]
    previous = ''
    for i in list_:
        otherlist = list(SUBPERSOINFO_DICT.keys())
        otherlist.remove(previous_)
        if i in otherlist: #in case that due to missing info it takes the next title as value
            break
        else:
            if previous in infolist:
                if i not in infolist:
                    dictout[previous] = i
            previous = i

    #for Durée de mandat, in case value is "indeterminée", complete dictout can be deleted and redo with only this value
    #other information would be useless or wrong
    if previous_ == 'Durée du mandat':
        if 'Durée du mandat' in dictout.keys():
            if dictout['Durée du mandat'] == 'Indéterminée':
                del dictout
                dictout = {}
                dictout['Durée du mandat'] = 'Indéterminée'
    return dictout








def get_data_from_subdict(label, labellist, dict_):
    if 're_splitted' in dict_.keys():
        if label in dict_['re_splitted'].keys():
            previous = ""
            dict_[label] = {}
            n = 0
            for i in dict_['re_splitted'][label]:
                if previous == labellist[n]:
                    n += 1
                    dict_[label][previous] = i.strip()
                    if n == len(labellist):
                        break
                previous = i
    return dict_


def donnees_a_modifier(dict_):
    start = False
    end = False
    modif = []
    listout = []

    regex = DICT_DONNEES_A_MODIF['regex']
    startlist = DICT_DONNEES_A_MODIF['startlist']
    page2 = DICT_DONNEES_A_MODIF['page2']
    page = DICT_DONNEES_A_MODIF['page']

    for i in dict_['splitted_file']:
        if start and not end:
            listout.append(i)
            x = re.search(regex, i.replace('Page', 'page'))
            if x:
                modif.append(i.replace("✔", '').replace('Page', 'page').split(page)[0].strip())

        for phrase in startlist:
            if phrase in i and not start:
                start = True
                break

        if page2 in i:
            break

        dict_['donnees_a_modifier'] = modif
    return dict_



def parse_object_new(dict_):
    ODC = []
    objet_flag = False
    valid_file = False
    nn = 0
    objects = OBJECTS_DICTS
    Splitteds = dict_['re_splitted']

    for objet_ in objects.keys():
        nn += 1
        if objet_ in Splitteds.keys():
            objet = objet_
            objet_in = objects[objet]['indic']
            objet_end = objects[objet]['incomp']
            objet_end2 = "✔ " + objet_end
            valid_file = True
            dict_[objet] = {}
            break

    if valid_file:
        for i in dict_['re_splitted'][objet]:
            if i == objet_end2:
                dict_[objet]['Objet incomplet'] = True
                objet_flag = False

            if i == objet_end:
                dict_[objet]['Objet incomplet'] = False
                objet_flag = False

            if objet_flag and i != objet_in:
                for txt_ in i.split('.'):
                    txt = clean_object(txt_)
                    if txt:
                        ODC.append(txt.strip())
                test = True

            if i == objet_in:
                objet_flag = True

            if ODC:
                dict_[objet]['objet'] = '. '.join(ODC)

    return dict_


def clean_object(x):
    y = ""
    if x:
        x = x.strip()
        if len(x) > 0:
            try:
                r = re.compile(r'^-')
                x = r.sub("", x, 1)
                if x[-1] == ",":
                    y = x[:-2].strip()
                elif x[-1] == ";":
                    y = x[:-2].strip()
                else:
                    y = x.strip()
            except Exception:
                pass
    return y