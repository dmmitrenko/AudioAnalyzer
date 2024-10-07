import os

class RabbitMQConfig:
    HOST = os.getenv('RABBITMQ_HOST', 'localhost')
    PORT = int(os.getenv('RABBITMQ_PORT', 5672))
    QUEUE_NAME = os.getenv('RABBITMQ_QUEUE_NAME', 'audio_processing_queue')
    USERNAME = os.getenv('RABBITMQ_USERNAME', 'guest')
    PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
    EXCHANGE_NAME = os.getenv('RABBITMQ_EXCHANGE_NAME', 'audio_exchange')