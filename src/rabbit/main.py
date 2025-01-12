import pandas as pd

from src.rabbit.utils import Rabbit


def main(message,mongo_rcs=None, date=''):
    status = ''
    if isinstance(message, pd.DataFrame):
        #print('dataframe')
        for rcs in message['RCS'].to_list():
            try:
                print(rcs)
                message = message[message['RCS'] == rcs].fillna('').to_json(orient="records") #to_dict('records')
                rabbit = Rabbit()
                rabbit.send_message(message, rcs_list=rcs)
                if mongo_rcs is not None:
                    mongo_rcs.update(query={'RCS':rcs}, newdatas={'send_to_rabbit': date, 'rabbit_status': 'success'}) #query can take a df as input and use RCS column as a list
                status = f"RCS {rcs} send successfully"
            except Exception as e:
                status = f"error while sending RCS {rcs} : {e}"
                mongo_rcs.update(query={'RCS': rcs}, newdatas={'send_to_rabbit': date, 'rabbit_status': 'failed'})
    else:
        status = 'input format not valid'
        print('input format not valid')
    return status



if __name__ == '__main__':
    main()