from datetime import datetime
import sys

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
from src.utils.set_logger import main as set_logger


logger = set_logger()

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

    daylist = ['15/05/2022','22/05/2022']
    rcs_list=Mongorcs.get_RCSlist({'extraction_date': {'$in':daylist}})



    #to be del
    #Mongorcs.set_to_be_updated(RCS=rcs_list)

    print('---- Scraping RCS ----')
    logger.info('---- Scraping RCS ----')
    try:
        rcs_list = scraper(type_='RCS', mongo=Mongorcs, to_be_updated=True)
        print(f"---- RCS scraped at {str(timer_main.stop())}s ----")
        logger.info(f"---- RCS scraped at {str(timer_main.stop())}s ----")
    except Exception as e:
        print(f'error at main.scraper RCS: {e}')
        logger.error(f'error at main.scraper RCS: {e}')
        logger.info('bot has been stopped')
        sys.exit()


    print('---- Scraping RBE ----')
    logger.info('---- Scraping RBE ----')
    try:
        scraper(type_='RBE', rcs=rcs_list, mongo=Mongorbe, to_be_updated=False)
        print(f"---- RBE scraped at {str(timer_main.stop())}s ----")
        logger.info(f"---- RBE scraped at {str(timer_main.stop())}s ----")
    except Exception as e:
        print(f'error at main.scraper RBE: {e}')
        logger.error(f'error at main.scraper RBE: {e}')
        logger.info('bot has been stopped')
        sys.exit()

    print('---- Parsing RCS ----')
    logger.info('---- Parsing RCS ----')
    try:
        rcs_parser(rcs=rcs_list, type_='rcs', mongo=Mongorcs, mongoparsed=Mongorcsp, onlynew=False)
        print(f"---- RCS Parsed at {str(timer_main.stop())}s ----")
        logger.info(f"---- RCS Parsed at {str(timer_main.stop())}s ----")
    except Exception as e:
        print(f'error at main.parser RCS: {e}')
        logger.error(f'error at main.parser RCS: {e}')
        logger.info('bot has been stopped')
        sys.exit()

    print('---- Parsing RBE ----')
    logger.info('---- Parsing RBE ----')
    try:
        rcs_parser(rcs=rcs_list, type_='rbe', mongo=Mongorbe, mongoparsed=Mongorbep, onlynew=False)
        print(f"---- RBE Parsed at {str(timer_main.stop())}s ----")
        logger.info(f"---- RBE Parsed at {str(timer_main.stop())}s ----")
    except Exception as e:
        print(f'error at main.parser RBE: {e}')
        logger.error(f'error at main.parser RBE: {e}')
        logger.info('bot has been stopped')
        sys.exit()

    RCS_splited_lists = rcs_spliter(rcs_list, 1000)
    print('---- Downloading pdfs ----')
    logger.info('---- Downloading pdfs ----')
    try:
        for i, sub_rcs_list in enumerate(RCS_splited_lists):
            print(f"    {i} on {len(RCS_splited_lists)}")
            logger.info(f"    {i} on {len(RCS_splited_lists)}")
            pdf_downloader(RCS=sub_rcs_list, mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf)
            print(f"    Pdfs Downloaded")
            logger.info(f"    Pdfs Downloaded")
        print(f"---- Pdfs Downloaded at {str(timer_main.stop())}s ----")
        logger.info(f"---- Pdfs Downloaded at {str(timer_main.stop())}s ----")
    except Exception as e:
        print(f'error at main.pdf_downloader: {e}')
        logger.error(f'error at main.pdf_downloader: {e}')
        logger.info('bot has been stopped')
        sys.exit()


    print('---- Parsing publi & financials---')
    logger.info('---- Parsing publi & financials---')
    try:
        for i, sub_rcs_list in enumerate(RCS_splited_lists):
            print(f"{i} on {len(RCS_splited_lists)}")
            logger.info(f"{i} on {len(RCS_splited_lists)}")
            print('    Starting publi')
            logger.info('    Starting publi')
            N = publi_parser(RCS=sub_rcs_list, mongo=Mongopdf, mongoparsed=Mongopubli, onlynew=True)
            print(f'    Publi parsed : {N}')
            logger.info(f'    Publi parsed : {N}')
            print('    Starting financials')
            logger.info('    Starting financials')
            N = financials_parser(RCS=sub_rcs_list, mongo=Mongopdf, mongoparsed=Mongofinan, onlynew=True)
            print(f"    Financials parsed : {N} in {str(timer_main.stop())}s")
            logger.info(f"    Financials parsed : {N} in {str(timer_main.stop())}s")
        print(f"---- Pdfs parsed in {str(timer_main.stop())}s")
        logger.info(f"---- Pdfs parsed in {str(timer_main.stop())}s")
    except Exception as e:
        print(f'error at main.parsers: {e}')
        logger.error(f'error at main.parsers: {e}')
        logger.info('bot has been stopped')
        sys.exit()



    #rcs_list = Mongorcs.get_RCSlist({'task_index':42})

    #print(len(rcs_list))

    print('---- Merging ----')
    logger.info('---- Merging ----')
    try:
        RCS_output = merger(Mongorcs=Mongorcs, Mongorbe=Mongorbe, Mongorcsp=Mongorcsp, Mongorbep=Mongorbep,
                            Mongopdf=Mongopdf, Mongopubli=Mongopubli, Mongofinan=Mongofinan, rcs_list=rcs_list)
        print(f"---- Merging done at {str(timer_main.stop())}s ----")
        logger.info(f"---- Merging done at {str(timer_main.stop())}s ----")
    except Exception as e:
        print(f'error at main.merger: {e}')
        logger.error(f'error at main.merger: {e}')
        logger.info('bot has been stopped')
        sys.exit()

    #tobedel
    #RCS_output.to_csv(f'backup_{datetime.today().strftime("%d%m%Y")}.csv', sep=';')

    print('---- Messaging ----')
    logger.info('---- Messaging ----')
    try:
        RCS_splited_lists = rcs_spliter(rcs_list, 1)
        for i, sub_rcs_list in enumerate(RCS_splited_lists):
            #print(f"    {i} on {len(RCS_splited_lists)}")
            message(RCS_output[RCS_output['RCS'].isin(sub_rcs_list)], mongo_rcs=Mongorcs, date=TODAY)
        print(f"---- Messaging done at {str(timer_main.stop())}s ----")
        logger.info(f"---- Messaging done at {str(timer_main.stop())}s ----")
    except Exception as e:
        print(f'error at main.message: {e}')
        logger.error(f'error at main.message: {e}')
        logger.info('bot has been stopped')
        sys.exit()


    print('---- Dashboard data update ----')
    logger.info('---- Dashboard data update ----')
    try:
        generate_report(DBstatus=DBstatus, Mongorcsp=Mongorcsp, Mongorbep=Mongorbep, Mongofinan=Mongofinan,Mongopubli=Mongopubli, Merged=RCS_output)
        print(f"---- Dashboard data updated at {str(timer_main.stop())}s ----")
        logger.info(f"---- Dashboard data updated at {str(timer_main.stop())}s ----")
    except Exception as e:
        print(f'error at main.generate_report: {e}')
        logger.error(f'error at main.generate_report: {e}')
        logger.info('bot has been stopped')
        sys.exit()

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
    logger.info(f"---- Completed in {str(timer_main.stop())}s ----")



if __name__ == '__main__':
    main()