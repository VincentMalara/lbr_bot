from src.scrapers.scraper import scraper


class resa(scraper):
    def __init__(self,  **kwargs):
        self.type = 'resa'
        scraper.__init__(self,  **kwargs)