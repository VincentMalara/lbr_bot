from datetime import datetime
import sys


from src.utils.timer import performance_timer
from src.mongo.main import mongo
from src.utils.set_logger import main as set_logger
from src.updaters.daily_updater.utils import run_step, Mongos


logger = set_logger()

TODAY = datetime.today().strftime("%d/%m/%Y")


def main():
    timer_main = performance_timer()

    mongos = Mongos()
    mongos.Mongorcs = mongo(db='LBR_test', col='RCS')
    mongos.Mongorbe = mongo(db='LBR_test', col='RBE')
    mongos.Mongorcsp = mongo(db='LBR_test', col='RCS_parsed')
    mongos.Mongorbep = mongo(db='LBR_test', col='RBE_parsed')
    mongos.Mongoresa = mongo(db='LBR_test', col='RESA')
    mongos.Mongoresaparsed = mongo(db='LBR_test', col='RESA_parsed')
    mongos.Mongopdf = mongo(db='LBR_test', col='all_pdfs')
    mongos.Mongopubli = mongo(db='LBR_test', col='publications')
    mongos.Mongofinan = mongo(db='LBR_test', col='financials')
    mongos.DBstatus = mongo( db='LBR_test', col='DB_status')


    step_dict = {
        'resa': {'funct': "resa(Mongoresa=mongos.Mongoresa, Mongoresaparsed=mongos.Mongoresaparsed, Mongorcs=mongos.Mongorcs, nmonth=4)",
                 'n': 0},
        'rcs': {'funct': "scraper(rcs=rcs_list, type_='RCS', mongo=mongos.Mongorcs, to_be_updated=False)", 'n': 0},
        'rbe': {'funct': "scraper(rcs=rcs_list, type_='RBE', mongo=mongos.Mongorbe, to_be_updated=False)",'n': 0},
        'rcs_parsed': {'funct': "rcs_parser(rcs=rcs_list, type_='rcs', mongo=mongos.Mongorcs, mongoparsed=mongos.Mongorcsp, onlynew=False)",
                       'n': 0},
        'rbe_parsed': {'funct': "rcs_parser(rcs=rcs_list, type_='rbe', mongo=mongos.Mongorbe, mongoparsed=mongos.Mongorbep, onlynew=False)",
                       'n': 0},
        'pdf_downloader': {'funct': "pdf_downloader(RCS=rcs_list, mongo_rcsparsed=mongos.Mongorcsp, mongo_pdfs=mongos.Mongopdf)",
                           'n': 1000},
        'publi_parser': {'funct': "publi_parser(RCS=rcs_list, mongo=mongos.Mongopdf, mongoparsed=mongos.Mongopubli, onlynew=True)",
                         'n': 1000},
        'financials_parser': {'funct': "financials_parser(RCS=rcs_list, mongo=mongos.Mongopdf, mongoparsed=mongos.Mongofinan, onlynew=True)",
                              'n': 1000},
        'merger': {'funct': "merger(Mongorcs=mongos.Mongorcs, Mongorbe=mongos.Mongorbe, Mongorcsp=mongos.Mongorcsp, Mongorbep=mongos.Mongorbep, Mongopdf=mongos.Mongopdf, Mongopubli=mongos.Mongopubli, Mongofinan=mongos.Mongofinan, rcs_list=rcs_list)",
                   'n': 0},
        'message': {'funct': "message(message=output, mongo_rcs=mongos.Mongorcs, date=TODAY)",
                    'n': 0},
        #'generate_report': {'funct': "generate_report(DBstatus=mongos.DBstatus, Mongorcsp=mongos.Mongorcsp, Mongorbep=mongos.Mongorbep, Mongofinan=mongos.Mongofinan,Mongopubli=mongos.Mongopubli, Merged=output)",
         #                   'n': 0}
    }

    output = ''
    #running first resa
    rcs_list = []
    funct = step_dict['resa']['funct']
    nsplit = step_dict['resa']['n']
    status, output = run_step(mongos, 'resa', funct, timer_main, output, rcs_list, nsplit, TODAY)
    rcs_list = mongos.Mongorcs.get_RCSlist({'status': 'to_be_updated'})
    print(rcs_list)
    #rcs_list = ["B157402"]#"B180661", "B2642"]
    #then loop over the other
    if len(rcs_list) > 0:
        for key in step_dict.keys():
            if key != 'resa':
                name = key
                funct = step_dict[key]['funct']
                nsplit = step_dict[key]['n']
                n=0
                status = False
                while not status:
                    n += 1
                    status, output = run_step(mongos, name, funct, timer_main, output, rcs_list, nsplit, TODAY)
                    if n > 1:
                        mongos.Mongorcs.set_to_be_updated(RCS=rcs_list)
                        print(f'bot has been stop at step {name}, rcs list has been reset to "to_be_updated"')
                        logger.error(f'bot has been stop at step {name}, rcs list has been reset to "to_be_updated"')
                        sys.exit()
    else:
        print('to be updated list is empty')
        logger.info('to be updated list is empty')

    mongos.close_all()

    print(f"---- Completed in {str(timer_main.stop())}s ----")
    logger.info(f"---- Completed in {str(timer_main.stop())}s ----")



if __name__ == '__main__':
    main()