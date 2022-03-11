
import os.path as path

import pandas as pd


from src.utils_merger import *


from .utils.timer import performance_timer
from src.mongo.main import mongo
from src.pdf_parsers.publications.main import main as parser
#from src.pdf_parsers.publications.parser import main as parser


timer_main = performance_timer()

Mongorcs = mongo(ip='146.59.152.231', db='LBR_test', col='RCS')
Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')
Mongorbep = mongo(ip='146.59.152.231', db='LBR_test', col='RBE_parsed')
Mongoresa = mongo(ip='146.59.152.231', db='LBR_test', col='RESA_parsed')
Mongopdf= mongo(ip='146.59.152.231', db='LBR_test', col='all_pdfs')
Mongopubli = mongo(ip='146.59.152.231', db='LBR_test', col='publications')

'Code NACE (Information mise à jour mensuellement)'
RCSlist = Mongorcsp.get_RCSlist(dictin={"Forme juridique":{ '$regex' : '.*' + 'commandite spéciale' + '.*'},
                                        'Code NACE (Information mise à jour mensuellement)':{'$exists':False}})
print(len(RCSlist))

Mongorcs.set_to_be_updated(RCS=RCSlist)
'''
#find all the SCSP companies from RCS parsed
RCSlist = Mongorcsp.get_RCSlist(dictin={"Forme juridique":{ '$regex' : '.*' + 'commandite spéciale' + '.*'}})
print(len(RCSlist))

parser(RCS=RCSlist, mongo=Mongopdf, mongoparsed=Mongopubli,onlynew=False )
'''


'''
list_ = ['https://gd.lu/rcsl/2Q9MLZ','https://gd.lu/rcsl/7Qm7MM', 'https://gd.lu/rcsl/3dCg4g']
#list_ = ['https://gd.lu/rcsl/3dCg4g']


AA = Mongopdf.find({'depot':{'$in':list_}}).apply(lambda x: parser(x, -1), axis=1).to_list()

for a in AA:
    print('--')
    for label_ in [
                "Gérant/Administrateur", "Délégué à la gestion journalière", "Actionnaire/Associé",
                "Personne(s) chargée(s) du contrôle des comptes", "Société de gestion"]:
        try:
            print(a[label_])
        except:
            pass
'''