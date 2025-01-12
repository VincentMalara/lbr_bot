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
from src.utils.set_logger import main as set_logger


logger = set_logger()


class Mongos():
    def __init__(self):
        self.Mongorcs = ''
        self.Mongorbe = ''
        self.Mongorcsp = ''
        self.Mongorbep = ''
        self.Mongoresa = ''
        self.Mongoresaparsed = ''
        self.Mongopdf = ''
        self.Mongopubli = ''
        self.Mongofinan = ''
        self.DBstatus = ''

    def close_all(self):
        self.Mongorcs.close()
        self.Mongorbe.close()
        self.Mongorcsp.close()
        self.Mongorbep.close()
        self.Mongoresa.close()
        self.Mongoresaparsed.close()
        self.Mongopdf.close()
        self.Mongopubli.close()
        self.Mongofinan.close()
        self.DBstatus.close()


def run_step(mongos, name, funct, timer, output, rcs_list, n, TODAY):
    print(f"---- processing {name} ----")
    logger.info(f"---- processing {name} ----")
    try:
        if n>0:
            RCS_splited_lists = rcs_spliter(rcs_list, n)
            for i, sub_rcs_list in enumerate(RCS_splited_lists):
                rcs_list = sub_rcs_list
                output = eval(funct)
                print(f'splited step {name} : {i}')
        else:
            output = eval(funct)
        print(f"---- {name} done at {str(timer.stop())}s ----")
        logger.info(f"---- {name} done at {str(timer.stop())}s ----")
        status = True
    except Exception as e:
        print(f'error at main.{name}: {e}')
        logger.exception(f'error at main.{name}: {e}')
        logger.info('bot has been stopped')
        status = False

    print('output')
    print(output)
    print('status')
    print(status)

    return status, output
