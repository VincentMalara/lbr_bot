import pika
from configs.settings import *
from src.utils.set_logger import main as set_logger


logger = set_logger()

class Rabbit():
    def __init__(self):
        self.credentials = pika.PlainCredentials(rabbit_user, rabbit_psw)
        self.parameters = pika.ConnectionParameters(rabbit_param[0], rabbit_param[1],
                                                    rabbit_param[2], self.credentials)
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()

    def send_message(self, message, rcs_list=None):
        try:
            self.channel.queue_declare(queue='lbr_bot', durable=True)
            self.channel.basic_publish(exchange='',
                              routing_key='lbr_bot',
                              body=message)
            if rcs_list is not None:
                logger.info(f"info at rabbit.send {rcs_list} sent")
        except Exception as e:
            print(f"error at rabbit.send message: {e}")
            logger.error(f"error at rabbit.send message: {e}")
            if rcs_list is not None:
                logger.error(f"error at rabbit.send rcs list: {rcs_list}")

        #print(f"message sent: {message}")

    def close(self):
        self.connection.close()
