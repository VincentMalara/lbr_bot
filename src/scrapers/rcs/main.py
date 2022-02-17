from src.scrapers.rcs.scraper import Rcs
from configs import settings


def main(rcs, mongo):
    n=0
    test = False
    while not test:
        print('---------')
        print(n)
        scraper = Rcs(headless=settings.headless, Mongo=mongo)
        scraper.launch()
        test = scraper.status
        n += 1
        if n > 5:
            test = True
    scraper.scrap_list(rcs)
    scraper.quit()

if __name__ == '__main__':
    main()