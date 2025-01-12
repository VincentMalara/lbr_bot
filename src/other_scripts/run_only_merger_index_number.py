from datetime import datetime
import sys

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
    DBstatus = mongo(db='LBR_test', col='DB_status')


    rcs_list = Mongorcs.get_RCSlist({'task_index':52})

    print(len(rcs_list))


    print('---- Merging ----')
    logger.info('---- Merging ----')

    RCS_output = merger(Mongorcs=Mongorcs, Mongorbe=Mongorbe, Mongorcsp=Mongorcsp, Mongorbep=Mongorbep,
                            Mongopdf=Mongopdf, Mongopubli=Mongopubli, Mongofinan=Mongofinan, rcs_list=rcs_list)
    print(f"---- Merging done at {str(timer_main.stop())}s ----")
    logger.info(f"---- Merging done at {str(timer_main.stop())}s ----")

    # tobedel
    # RCS_output.to_csv(f'backup_{datetime.today().strftime("%d%m%Y")}.csv', sep=';')

    print('---- Messaging ----')
    logger.info('---- Messaging ----')

    RCS_splited_lists = rcs_spliter(rcs_list, 1)
    for i, sub_rcs_list in enumerate(RCS_splited_lists):
        # print(f"    {i} on {len(RCS_splited_lists)}")
        message(RCS_output[RCS_output['RCS'].isin(sub_rcs_list)], mongo_rcs=Mongorcs, date=TODAY)
    print(f"---- Messaging done at {str(timer_main.stop())}s ----")
    logger.info(f"---- Messaging done at {str(timer_main.stop())}s ----")


    print('---- Dashboard data update ----')
    logger.info('---- Dashboard data update ----')

    generate_report(DBstatus=DBstatus, Mongorcsp=Mongorcsp, Mongorbep=Mongorbep, Mongofinan=Mongofinan,
                        Mongopubli=Mongopubli, Merged=RCS_output)
    print(f"---- Dashboard data updated at {str(timer_main.stop())}s ----")
    logger.info(f"---- Dashboard data updated at {str(timer_main.stop())}s ----")


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