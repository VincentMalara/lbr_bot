import pandas as pd

from src.rabbit.utils import Rabbit


def main(message,mongo_rcs=None, date=''):
    if isinstance(message, pd.DataFrame):
        #print('dataframe')
        for rcs in message['RCS'].to_list():
            print(rcs)
            message = message[message['RCS'].isin([rcs])].fillna('').to_json(orient="records") #to_dict('records')
            rabbit = Rabbit()
            rabbit.send_message(message, rcs_list=rcs)
            if mongo_rcs is not None:
                mongo_rcs.update(query={'RCS':rcs}, newdatas={'send_to_rabbit':date}) #query can take a df as input and use RCS column as a list
    else:
        print('input format not valid')



if __name__ == '__main__':
    main()