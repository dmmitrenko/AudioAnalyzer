import logging
from consumer import RabbitMQConsumer
from config import RabbitMQConfig
from rabbitmq_dispatcher import RabbitMQDispatcher
from message_handler import MessageHandler


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = RabbitMQConfig()
    dispatcher = RabbitMQDispatcher(config)
    handler = MessageHandler(dispatcher)
    consumer = RabbitMQConsumer(config, handler)

    try:
        consumer.connect()
        consumer.consume()
    except Exception as e:
        logging.error(f"Failed to run consumer: {e}")