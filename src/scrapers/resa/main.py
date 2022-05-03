from src.scrapers.resa.utils import Resa, generate_last_n_month


def main(Mongoresa, Mongoresaparsed, Mongorcs, nmonth=4):
    monthlist = generate_last_n_month(nmonth)

    n=0
    test = False
    while not test and n<5:
        scraper = Resa(mongoRESA=Mongoresa, mongoRESAparsed=Mongoresaparsed, mongorcs=Mongorcs)
        scraper.launch()
        test = scraper.status
        n += 1

    for month in monthlist:
        scraper.scrap_month(year=month.split('-')[0], month=month.split('-')[1])
        scraper.push_pages_to_mongo()
        scraper.extract_xmls()
        scraper.set_rcs_to_be_updated()

    scraper.quit()

if __name__ == '__main__':
    main()













