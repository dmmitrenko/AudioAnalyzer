import pika
import logging
from config import RabbitMQConfig
from message_handler import MessageHandler
from rabbitmq_dispatcher import RabbitMQDispatcher

class RabbitMQConsumer:
    def __init__(self, config, handler):
        self.config = config
        self.handler = handler
        self.connection = None
        self.channel = None

    def connect(self):
        try:
            credentials = pika.PlainCredentials(self.config.USERNAME, self.config.PASSWORD)
            parameters = pika.ConnectionParameters(host=self.config.HOST, port=self.config.PORT, credentials=credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            self.channel.exchange_declare(exchange=self.config.EXCHANGE_NAME, exchange_type='topic')
            self.channel.queue_declare(queue=self.config.QUEUE_NAME, durable=True)

            routing_key = "audio.processing.#"
            self.channel.queue_bind(exchange=self.config.EXCHANGE_NAME, queue=self.config.QUEUE_NAME, routing_key=routing_key)

            logging.info(f"Connected to RabbitMQ on {self.config.HOST}:{self.config.PORT}, bound to exchange '{self.config.EXCHANGE_NAME}' with routing key '{routing_key}'")
        
        except Exception as e:
            logging.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def consume(self):
        if not self.channel:
            raise Exception("RabbitMQ channel is not initialized. Call connect() first.")

        def callback(ch, method, properties, body):
            logging.info("Received a message")
            self.handler.process_message(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=self.config.QUEUE_NAME, on_message_callback=callback)

        logging.info("Starting consuming...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.info("Interrupted by user, stopping consumer...")
            self.close_connection()

    def close_connection(self):
        if self.connection:
            self.connection.close()
            logging.info("Connection to RabbitMQ closed.")