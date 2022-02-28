import pandas as pd
from datetime import datetime
import os
import glob
from configs import settings
import urllib.request

#import tika
#tika.initVM()
#from tika import parser


ACCEPTED_TYPE = ['Modification', 'Comptes annuels (eCDF)', 'Comptes annuels',
                 'Modification non statutaire des mandataires', 'Statuts coordonnés', 'Mise à jour du dossier',
                 'Immatriculation', 'Modification - Changement de la forme juridique', 'Démission']


LOCAL_PATH_PDF = os.getcwd() + settings.temp

NDLSTEPS = 100

def clean_temp():
    files = glob.glob(os.path.join(LOCAL_PATH_PDF+'/*'))
    n=0
    for iteration, item in enumerate(files):
        os.remove(item)
        n = iteration
    return n

def check_temp_exist():
    print(LOCAL_PATH_PDF)
    if os.path.exists(LOCAL_PATH_PDF):
        print('temp does exist')
    else:
        path = os.path.join(LOCAL_PATH_PDF)
        os.mkdir(path)
        print(f"{LOCAL_PATH_PDF} created")
        print(os.path.exists(LOCAL_PATH_PDF))



def is_accepeted(x):
    return any(at in x for at in ACCEPTED_TYPE)


def get_pdfs_list_from_parsed_RCS(DF):
    DFout = pd.DataFrame()
    for index, row in DF.iterrows():
        if 'depots' in row.keys():
            publis = row['depots']
            RCS = row['RCS']
            if isinstance(publis, list):
                df = pd.DataFrame(publis)
                df['RCS'] = RCS
                DFout = pd.concat([DFout, df])

    DFout=DFout[DFout['Type_de_depot'].apply(is_accepeted)].reset_index(drop=True)
    return DFout


def build_pdfs_list(RCS=None, mongo_rcsparsed='', mongo_pdfs=''):
    if RCS is None:
        RCSDF = mongo_rcsparsed.find()
        already_done_pdfs = mongo_pdfs.find()
    else:
        RCSDF = mongo_rcsparsed.find_from_RCSlist(RCS)
        already_done_pdfs = mongo_pdfs.find_from_RCSlist(RCS)

    print('getting pdfs list')
    pdfs = get_pdfs_list_from_parsed_RCS(RCSDF)
    print(f'pdfs list done, {pdfs.shape[0]} pdfs found')

    if "N_depot" in pdfs.columns:
        if "N_depot" in already_done_pdfs.columns:
            if already_done_pdfs.shape[0] > 0:
                pdfs = pdfs[~pdfs['N_depot'].isin(already_done_pdfs['N_depot'])]

    return pdfs


def dl_pdfs_list(pdfs='', mongo_pdfs=''):
    dllist = []
    n=0
    n_downloaded = 0
    for index, row in pdfs.iterrows():
        if 'depot' in row.keys():
            n += 1
            output = row.to_dict()
            output['file'] = get_pdf_content(output['depot'])
            output['extraction_date'] = datetime.today().strftime("%d/%m/%Y")
            output['task_index'] = -1
            dllist.append(output)
            n_downloaded += 1

        if n > NDLSTEPS:
            n = 0
            clean_temp()
            mongo_pdfs.insert(dllist)
            dllist = []

    clean_temp()
    mongo_pdfs.insert(dllist)

    return n_downloaded


def get_pdf_content(x):
    if 'https://gd.lu' in x:
        dict_ = downloadfile(x, ext='.pdf')
    else:
        dict_={'file':''}
        print(f"{x} was not a valid url")
    return dict_['file']


def downloadfile(url, ext='.pdf'):
    path = os.path.join(LOCAL_PATH_PDF, (url.split('/')[-1] + ext))
    urllib.request.urlretrieve(url, path)
    #raw = parser.from_file(path)
    raw = " "
    return {'path': path, 'file': raw}