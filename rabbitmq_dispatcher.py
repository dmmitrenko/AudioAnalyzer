import pika
import json
import logging

class RabbitMQDispatcher:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.channel = None

    def connect(self):
        try:
            credentials = pika.PlainCredentials(self.config.USERNAME, self.config.PASSWORD)
            parameters = pika.ConnectionParameters(host=self.config.HOST, port=self.config.PORT, credentials=credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.config.EXCHANGE_NAME, exchange_type='topic')
            logging.info("Connected to RabbitMQ")
        except Exception as e:
            logging.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def send_message(self, routing_key, message):
        formatted_result = self.format_message(message)
        try:
            body = json.dumps(formatted_result)
            self.channel.basic_publish(
                exchange=self.config.EXCHANGE_NAME,
                routing_key=routing_key,
                body=body
            )
            logging.info(f"Message sent to exchange '{self.config.EXCHANGE_NAME}' with routing key '{routing_key}': {message}")
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
        finally:
            self.close_connection()

    @staticmethod
    def format_message(message):
        return {
            "Id": message["id"],
            "EmotionalTone": message["emotional_tone"],
            "Text": message["text"],
            "Location": message["location"],
            "Categories": message["categories"]
        }

    def close_connection(self):
        if self.connection:
            self.connection.close()
            logging.info("Dispatcher connection closed.")