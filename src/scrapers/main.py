import sys


from configs import settings
from src.utils.handle_RCS_list import main as rcs_input_checker
from src.utils.set_logger import main as set_logger


logger = set_logger()


def main(type_='RCS', rcs=None, mongo=None, to_be_updated=False): #if to_be_updated, RCS has to be empty
    if type_ == 'RCS':
        from src.scrapers.rcs.scraper import Rcs as Scraper
        print('run scraper in RCS mode')
        logger.info('run scraper in RCS mode')
    elif type_ == 'RBE':
        from src.scrapers.rbe.scraper import Rbe as Scraper
        print('run scraper in RBE mode')
        logger.info('run scraper in RBE mode')
    elif type_ == 'resa':
        print('run scraper in resa mode')
        logger.info('run scraper in resa mode')
    else:
        print(f"error at srcaper.main : {type_} is not known, for scraper type")
        logger.error(f"error at srcaper.main : {type_} is not known, for scraper type")
        sys.exit()

    run_scraper = False

    if to_be_updated and rcs is None:
        if mongo is not None:
            rcs = mongo.get_RCSlist({'status': 'to_be_updated'})
            if len(rcs) > 0:
                run_scraper = True
            else:
                run_scraper = False
                print(f"info at scraper.main {type_}: nothing to scrap, to_be_updated list si empty")
                logger.info(f"info at scraper.main {type_}: nothing to scrap, to_be_updated list si empty")
        else:
            print(f"missing mongo collection in scraper.main {type_} in to_be_updated mode")
            logger.error(f"missing mongo collection in scraper.main {type_} in to_be_updated mode")
            sys.exit()
    else:
        rcs, dict_, status, msg = rcs_input_checker(RCS=rcs)
        if len(rcs)>0 and status:
            run_scraper = True
        else:
            print(f"info scraper.main {type_}, message of rcs_input_checker : {message}")
            logger.error(f"info scraper.main {type_}, message of rcs_input_checker : {message}")

    if run_scraper:
        n = 0
        test = False
        print(f"info scraper.main {type_}: {len(rcs)} RCS will be scraped")
        logger.info(f"info scraper.main {type_}: {len(rcs)} RCS will be scraped")
        while not test:
            print(f"info scraper.main {type_}: trial to connect : {n}")
            logger.info(f"info scraper.main {type_}: trial to connect : {n}")
            scraper = Scraper(headless=settings.headless, Mongo=mongo)
            scraper.launch()
            test = scraper.status
            n += 1
            if n > 5:
                print(f"info scraper.main {type_}: trial stopped at {n}")
                logger.error(f"info scraper.main {type_}: trial stopped at {n}")
                test = True

        scraper.scrap_list(rcs)
        scraper.quit()
    return rcs

if __name__ == '__main__':
    main()