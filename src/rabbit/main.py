import pandas as pd

from src.rabbit.utils import Rabbit


def main(message,mongo_rcs=None, date=None):
    format = False
    if isinstance(message, list):
        #print('list')
        #print(message)
        format = False
    elif isinstance(message, dict):
        #print('dict')
        #message = [message]
        format = False
    elif isinstance(message, pd.DataFrame):
        #print('dataframe')
        df = message.copy()
        if 'RCS' in df.columns:
            rcs_list = df['RCS'].to_list()
        else:
            rcs_list = []
        message = message.fillna('').to_json(orient="records") #to_dict('records')
        format = True
    if format:
        rabbit = Rabbit()
        #print(message)
        rabbit.send_message(str(message), rcs_list = rcs_list)
        if mongo_rcs is not None and date is not None:
            mongo_rcs.update(query=df, newdatas={'send_to_rabbit':date}) #query can take a df as input and use RCS column as a list
    else:
        print('input format not valid')


if __name__ == '__main__':
    main()