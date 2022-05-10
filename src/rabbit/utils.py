import pika
from configs.settings import *


class Rabbit():
    def __init__(self):
        self.credentials = pika.PlainCredentials(rabbit_user, rabbit_psw)
        self.parameters = pika.ConnectionParameters(rabbit_param[0], rabbit_param[1],
                                                    rabbit_param[2], self.credentials)
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()

    def send_message(self, message):
        self.channel.queue_declare(queue='lbr_bot', durable=True)

        self.channel.basic_publish(exchange='',
                          routing_key='lbr_bot',
                          body=message)
        #print(f"message sent: {message}")

    def close(self):
        self.connection.close()
