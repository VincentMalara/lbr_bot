from datetime import datetime


from src.scrapers.resa.main import main as resa
from src.scrapers.main import main as scraper
from src.html_parsers.main import main as rcs_parser
from src.pdf_downloaders.main import main as pdf_downloader
from src.pdf_parsers.financials.main import main as financials_parser
from src.pdf_parsers.publications.main import main as publi_parser
from src.merger.main import main as merger
from src.rabbit.main import main as message
from src.generate_report.main import main as generate_report
from src.utils.RCS_spliter import main as rcs_spliter
from src.utils.timer import performance_timer
from src.mongo.main import mongo


TODAY = datetime.today().strftime("%d/%m/%Y")


def main():
    timer_main = performance_timer()

    Mongorcs = mongo(db='LBR_test', col='RCS')
    Mongorbe = mongo(db='LBR_test', col='RBE')
    Mongorcsp = mongo(db='LBR_test', col='RCS_parsed')
    Mongorbep = mongo(db='LBR_test', col='RBE_parsed')
    Mongoresa = mongo(db='LBR_test', col='RESA')
    Mongoresaparsed = mongo(db='LBR_test', col='RESA_parsed')
    Mongopdf = mongo(db='LBR_test', col='all_pdfs')
    Mongopubli = mongo(db='LBR_test', col='publications')
    Mongofinan = mongo(db='LBR_test', col='financials')
    DBstatus = mongo( db='LBR_test', col='DB_status')

    rcs_list = ['A39456', 'B100006', 'B100007', 'B100008', 'B100009', 'B106338', 'B120246', 'B1202461', 'B120247', 'B120248',
           'B14674', 'B14710', 'B14815', 'B15268', 'B15425', 'B15467', 'B15489', 'B15490', 'B15491', 'B15492', 'B15590',
           'B15846', 'B15904', 'B16286', 'B16336', 'B16468', 'B16607', 'B16677', 'B16768', 'B16844', 'B16854', 'B16855',
           'B16923', 'B16924', 'B17015', 'B17016', 'B17020', 'B17218', 'B17286', 'B17298', 'B17479', 'B182934', 'B215643',
           'B221018', 'B221019', 'B248373', 'B249879', 'B255380', 'B262114', 'B39099', 'B56047', 'E1879', 'E2226', 'E229',
           'E2818', 'E4589', 'E4738', 'E5555', 'E7052', 'E7905', 'E831','B256373']

    rcs_list = [ 'B17298', 'B17479', 'B182934', 'B215643','B221018', 'B221019', 'B248373', 'B249879', 'B255380',
                 'B262114', 'B39099', 'B56047', 'E1879', 'E2226', 'E229', 'E2818', 'E4589', 'E4738', 'E5555',
                 'E7052', 'E7905', 'E831','B256373']

    '''
    print("---- Updating from Resa ----")
    resa(Mongoresa=Mongoresa, Mongoresaparsed=Mongoresaparsed, Mongorcs=Mongorcs, nmonth=4)
    print(f"---- Updated from Resa at {str(timer_main.stop())}s ----")
    '''

    #to be del
    Mongorcs.set_to_be_updated(RCS=rcs_list)

    print('---- Scraping RCS ----')
    rcs_list = scraper(type_='RCS', mongo=Mongorcs, to_be_updated=True)
    print(f"---- RCS scraped at {str(timer_main.stop())}s ----")

    '''
    print('---- Scraping RBE ----')
    scraper(type_='RBE', rcs=rcs_list, mongo=Mongorbe, to_be_updated=False)
    print(f"---- RBE scraped at {str(timer_main.stop())}s ----")

    print('---- Parsing RCS ----')
    rcs_parser(rcs=rcs_list, type_='rcs', mongo=Mongorcs, mongoparsed=Mongorcsp, onlynew=False)
    print(f"---- RCS Parsed at {str(timer_main.stop())}s ----")

    print('---- Parsing RBE ----')
    rcs_parser(rcs=rcs_list, type_='rbe', mongo=Mongorbe, mongoparsed=Mongorbep, onlynew=False)
    print(f"---- RBE Parsed at {str(timer_main.stop())}s ----")

    RCS_splited_lists = rcs_spliter(rcs_list, 1000)
    print('---- Downloading pdfs ----')
    for i, sub_rcs_list in enumerate(RCS_splited_lists):
        print(f"    {i} on {len(RCS_splited_lists)}")
        pdf_downloader(RCS=sub_rcs_list,mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf)
        print(f"    Pdfs Downloaded")
    print(f"---- Pdfs Downloaded at {str(timer_main.stop())}s ----")

    print('---- Parsing publi & financials---')
    for i, sub_rcs_list in enumerate(RCS_splited_lists):
        print(f"{i} on {len(RCS_splited_lists)}")
        print('    Starting publi')
        N = publi_parser(RCS=sub_rcs_list, mongo=Mongopdf, mongoparsed=Mongopubli, onlynew=True)
        print(f'    Publi parsed : {N}')
        print('    Starting financials')
        N = financials_parser(RCS=sub_rcs_list, mongo=Mongopdf, mongoparsed=Mongofinan, onlynew=True)
        print(f"    Financials parsed : {N} in {str(timer_main.stop())}s")
    print(f"---- Pdfs parsed in {str(timer_main.stop())}s")
    '''



    print('---- Merging ----')
    RCS_output = merger(Mongorcs=Mongorcs, Mongorbe=Mongorbe, Mongorcsp=Mongorcsp, Mongorbep=Mongorbep,
                        Mongopdf=Mongopdf, Mongopubli=Mongopubli, Mongofinan=Mongofinan, rcs_list=rcs_list)
    print(f"---- Merging done at {str(timer_main.stop())}s ----")


    #tobedel
    #RCS_output.to_csv('update_03052022.csv', sep=';')

    print('---- Messaging ----')
    RCS_splited_lists = rcs_spliter(rcs_list, 1)
    print('---- Downloading pdfs ----')
    for i, sub_rcs_list in enumerate(RCS_splited_lists):
        #print(f"    {i} on {len(RCS_splited_lists)}")
        message(RCS_output[RCS_output['RCS'].isin(sub_rcs_list)], mongo_rcs=Mongorcs, date=TODAY)
    print(f"---- Messaging done at {str(timer_main.stop())}s ----")

    print('---- Dashboard data update ----')
    #generate_report(DBstatus=DBstatus, Mongorcsp=Mongorcsp, Mongorbep=Mongorbep, Mongofinan=Mongofinan,Mongopubli=Mongopubli, Merged=RCS_output)
    print(f"---- Dashboard data updated at {str(timer_main.stop())}s ----")


    Mongorcs.close()
    Mongorbe.close()
    Mongorcsp.close()
    Mongorbep.close()
    Mongoresa.close()
    Mongoresaparsed.close()
    Mongopdf.close()
    Mongopubli.close()
    Mongofinan.close()
    DBstatus.close()

    print(f"---- Completed in {str(timer_main.stop())}s ----")



if __name__ == '__main__':
    main()