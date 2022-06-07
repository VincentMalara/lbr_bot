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
        'merger': {'funct': "merger(Mongorcs=mongos.Mongorcs, Mongorbe=mongos.Mongorbe, Mongorcsp=mongos.Mongorcsp, Mongorbep=mongos.Mongorbep, Mongopdf=mongos.Mongopdf, Mongopubli=mongos.Mongopubli, Mongofinan=mongos.Mongofinan, rcs_list=rcs_list)",
                   'n': 0},

        'generate_report': {'funct': "generate_report(DBstatus=mongos.DBstatus, Mongorcsp=mongos.Mongorcsp, Mongorbep=mongos.Mongorbep, Mongofinan=mongos.Mongofinan,Mongopubli=mongos.Mongopubli, Merged=output)",
                            'n': 0}
    }

    output = ''

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
                    if not status and name == 'message':
                        print(output)
                    if n > 1:
                        mongos.Mongorcs.set_to_be_updated(RCS=rcs_list)
                        print(f'bot has been stop at step {name}, rcs list has been reset to "to_be_updated"')
                        logger.error(f'bot has been stop at step {name}, rcs list has been reset to "to_be_updated"')
                        break
    else:
        print('to be updated list is empty')
        logger.info('to be updated list is empty')

    mongos.close_all()

    print(f"---- Completed in {str(timer_main.stop())}s ----")
    logger.info(f"---- Completed in {str(timer_main.stop())}s ----")



if __name__ == '__main__':
    main()