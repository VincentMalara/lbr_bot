import os


if os.name == 'nt':
    executablepath = 'geckodriver.exe'
    mongo_ip = 'localhost'
    headless = False
else:
    executablepath = 'geckodriver'
    mongo_ip = '146.59.152.231'
    headless = True


mongo_port = 27017
mongo_DB = 'LBR_test'
col_RCS = 'RCS'
col_RBE = 'RBE'
col_RESA = 'RESA'
col_RCSp = 'RCS_parsed'
col_pdfs = 'all_pdfs'


URL_RCS = 'https://www.lbr.lu/mjrcs/jsp'
URL_RBE = 'https://www.lbr.lu/mjrcs-rbe/'
URL_LBR = 'https://www.lbr.lu/mjrcs-lbr/jsp/'
URL_RESA = 'https://www.lbr.lu/mjrcs-resa/jsp/'

scraper_version = '3.00'
parser_version = '3.00'

temp = '/temp/'

