import os


# local vs server
if os.name == 'nt':
    executablepath = 'geckodriver.exe'
    mongo_ip =  '146.59.152.231' #'localhost' #
    headless = False
    NMAX = 10000
else:
    executablepath = 'geckodriver'
    mongo_ip = 'localhost' #'146.59.152.231'
    headless = True
    NMAX = 10000

#mongo
mongo_port = 27017
mongo_DB = 'LBR_test'
col_RCS = 'RCS'
col_RBE = 'RBE'
col_RESA = 'RESA'
col_RESAp = 'RESA_parsed'
col_RCSp = 'RCS_parsed'
col_RBEp = 'RBE_parsed'
col_pdfs = 'all_pdfs'
col_finan = 'financials' #bilans to ratio
col_publi = 'publications' #immat, modifications... to build admin/asso history

#LBR
URL_RCS = 'https://www.lbr.lu/mjrcs/jsp'
URL_RBE = 'https://www.lbr.lu/mjrcs-rbe/'
URL_LBR = 'https://www.lbr.lu/mjrcs-lbr/jsp/'
URL_RESA = 'https://www.lbr.lu/mjrcs-resa/jsp/'

#scrapper version
scraper_version = '3.00'
parser_version = '3.00'

#temp folder
temp = '/temp/'

#Rabbit
rabbit_user = 'rbuser2'
rabbit_psw = 'rabibit22z'
rabbit_param = ['51.91.62.122', 5672, '/'] #51.77.132.103
