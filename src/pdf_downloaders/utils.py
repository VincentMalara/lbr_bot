from datetime import datetime
import glob
import os
import urllib.request

import pandas as pd
import tika
from tika import parser

from configs import settings

FILE_LABEL_LIST = ['Modification',
                   'Modification non statutaire des mandataires',
                   'Immatriculation',
                   'Démission',
                   'Modification - Changement de la forme juridique',
                   'Inscription - Succursale',
                   'Modification - succursale']


LOCAL_PATH_PDF = os.getcwd() + settings.temp
NDLSTEPS = 100


tika.initVM()


def is_valid_pdf(row, type_):
    depots = row['Type_de_depot']
    try:
        year = datetime.strptime(row['Date'], '%d/%m/%Y').year
    except:
        year = 2000
    y = False
    if type_ in ['all', 'publi']:
        if year >= 2014:
            for label in FILE_LABEL_LIST:
                if label in depots:
                    y = True
                    break
    if type_ in ['all', 'bilan']:
        if 'eCDF' in depots:
            y = True
    return y

def is_valid_bilan(row):
    y = is_valid_pdf(row, 'bilan')
    return y

def is_valid_publi(row):
    y = is_valid_pdf(row, 'publi')
    return y

def is_valid_all(row):
    y = is_valid_pdf(row, 'all')
    return y


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
    raw = parser.from_file(path)
    return {'path': path, 'file': raw}



class PdfDownloader:
    def __init__(self, RCS='', mongo_rcsparsed='', mongo_pdfs=''):
        if RCS is None:
            self.RCS = ''
        else:
            self.RCS = RCS
        self.mongo_rcsparsed = mongo_rcsparsed
        self.mongo_pdfs = mongo_pdfs
        self.pdfs = ''
        if self.RCS=='':
            self.DFrcsparsed = self.mongo_rcsparsed.find()
            self.already_done_pdfs = self.mongo_pdfs.find()
        else:
            self.DFrcsparsed = self.mongo_rcsparsed.find_from_RCSlist(self.RCS)
            self.already_done_pdfs = self.mongo_pdfs.find_from_RCSlist(self.RCS)

    def get_pdfs_list(self):
        print('get_pdfs_list')
        DFout = pd.DataFrame()
        for index, row in self.DFrcsparsed.iterrows():
            if 'depots' in row.keys():
                publis = row['depots']
                RCS = row['RCS']
                if isinstance(publis, list):
                    df = pd.DataFrame(publis)
                    df['RCS'] = RCS
                    DFout = pd.concat([DFout, df])
        self.pdfs = DFout[DFout.apply(is_valid_all, axis=1)].reset_index(drop=True)
        #print(self.pdfs)
        if "N_depot" in self.pdfs.columns: #remove all the existing depot already in the pdf collection
            if "N_depot" in self.already_done_pdfs.columns:
                if self.already_done_pdfs.shape[0] > 0:
                    self.pdfs = self.pdfs[~self.pdfs['N_depot'].isin(self.already_done_pdfs['N_depot'])]
        #print(self.pdfs)
        return self.pdfs

    def store_pdfs(self):
        print('store_pdfs')
        dllist = []
        n = 0
        n_downloaded = 0
        self.task_index = self.mongo_pdfs.get_index_max() + 1
        for index, row in self.pdfs.iterrows():
            if 'depot' in row.keys():
                n += 1
                output = row.to_dict()
                output['file'] = get_pdf_content(output['depot'])
                output['extraction_date'] = datetime.today().strftime("%d/%m/%Y")
                output['task_index'] = self.task_index
                dllist.append(output)
                n_downloaded += 1

            if n > NDLSTEPS:
                n = 0
                clean_temp()
                self.mongo_pdfs.insert(dllist)
                dllist = []

        clean_temp()
        self.mongo_pdfs.insert(dllist)
        print(f"{n_downloaded} files downloaded")

        return n_downloaded
