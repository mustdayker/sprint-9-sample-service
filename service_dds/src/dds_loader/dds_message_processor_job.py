import json
from logging import Logger
from typing import List, Dict
from datetime import datetime

from lib.kafka_connect import KafkaConsumer, KafkaProducer

from dds_loader.repository import DdsRepository
import time
class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")


        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg['payload']
            self._dds_repository.test_insert(
                msg["object_id"],
                msg["object_type"],
                json.dumps(order))

            # user_id = order["user"]["id"]
            # user = self._redis.get(user_id)
            # user_name = user["name"]
            # user_login = user["login"]

            # restaurant_id = order['restaurant']['id']
            # restaurant = self._redis.get(restaurant_id)
            # restaurant_name = restaurant["name"]

            dst_msg = {
                "object_id": msg["object_id"],
                "status": "status",
                "payload": order
            }

            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
