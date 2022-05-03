import sys


from configs import settings
from src.utils.handle_RCS_list import main as rcs_input_checker


def main(type_='RCS', rcs=None, mongo=None, to_be_updated=False): #if to_be_updated, RCS has to be empty
    if type_ == 'RCS':
        from src.scrapers.rcs.scraper import Rcs as Scraper
        print('run in RCS mode')
    elif type_ == 'RBE':
        from src.scrapers.rbe.scraper import Rbe as Scraper
        print('run in RBE mode')
    elif type_ == 'resa':
        print('run in resa mode')
    else:
        print(f"{type_} is not known, for scraper type")
        sys.exit()

    run_scraper = False

    if to_be_updated and rcs is None:
        if mongo is not None:
            rcs = mongo.get_RCSlist({'status': 'to_be_updated'})
            if len(rcs) > 0:
                run_scraper = True
            else:
                run_scraper = False
                print(f"{type_} scraper: nothing to scrap, to_be_updated list si empty")
        else:
            print("missing mongo collection in rcs scraper in to_be_updated mode")
    else:
        rcs, dict_, status, msg = rcs_input_checker(RCS=rcs)
        if len(rcs)>0 and status:
            run_scraper = True
        else:
            print(msg)

    if run_scraper:
        n = 0
        test = False
        print(f"{len(rcs)} RCS will be scraped")
        while not test:
            print('---------')
            print(n)
            scraper = Scraper(headless=settings.headless, Mongo=mongo)
            scraper.launch()
            test = scraper.status
            n += 1
            if n > 5:
                test = True

        scraper.scrap_list(rcs)
        scraper.quit()
    return rcs

if __name__ == '__main__':
    main()