from src.scrapers.connection import connection


class resa(connection):
    def __init__(self,  **kwargs):
        self.type = 'resa'
        connection.__init__(self,  **kwargs)