from src.scrapers.resa.main import main as resa
from src.scrapers.main import main as scraper
from src.html_parsers.main import main as rcs_parser
from src.pdf_downloaders.main import main as pdf_downloader
from src.pdf_parsers.financials.main import main as financials_parser
from src.pdf_parsers.publications.main import main as publi_parser
from src.merger.main import main as merger
from src.generate_report.main import main as generate_report
from src.utils.RCS_spliter import main as rcs_spliter
from src.utils.timer import performance_timer
from src.mongo.main import mongo


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


    print("---- Updating from Resa ----")
    resa(Mongoresa=Mongoresa, Mongoresaparsed=Mongoresaparsed, Mongorcs=Mongorcs, nmonth=4)
    print(f"---- Updated from Resa at {str(timer_main.stop())}s ----")


    print('---- Scraping RCS ----')
    rcs_list = scraper(type_='RCS', mongo=Mongorcs, to_be_updated=True)
    print(f"---- RCS scraped at {str(timer_main.stop())}s ----")


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


    print('---- Merging ----')
    RCS_output = merger(Mongorcs=Mongorcs, Mongorbe=Mongorbe, Mongorcsp=Mongorcsp, Mongorbep=Mongorbep,
                        Mongopdf=Mongopdf, Mongopubli=Mongopubli, Mongofinan=Mongofinan, rcs_list=rcs_list)
    print(f"---- Merging done at {str(timer_main.stop())}s ----")


    #tobedel
    RCS_output.to_csv('update_03052022.csv', sep=';')

    print('---- Messaging ----')
    #message(RCS_output)
    print(f"---- Messaging done at {str(timer_main.stop())}s ----")

    print('---- Dashboard data update ----')
    generate_report(DBstatus=DBstatus, Mongorcsp=Mongorcsp, Mongorbep=Mongorbep, Mongofinan=Mongofinan,
                    Mongopubli=Mongopubli, Merged=RCS_output)
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