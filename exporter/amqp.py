from dataclasses import dataclass


@dataclass
class QueueConfig:
    name: str
    routing_key: str
    exchange: str
    exchange_type: str


@dataclass
class PublishConfig:
    exchange: str
    routing_key: str
    retry: bool
    retry_policy: dict


@dataclass
class AmqpConnConfig:
    host: str
    port: int

    def broker_url(self):
        return f'amqp://{self.host}:{str(self.port)}'
