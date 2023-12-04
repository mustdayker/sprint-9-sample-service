import json
from logging import Logger
from typing import List, Dict
from datetime import datetime
from uuid import UUID
from lib.kafka_connect import KafkaConsumer

from cdm_loader.repository import CdmRepository
import time

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger
                 ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
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

            # вставляем данные в PostgreSQL
            self._cdm_repository.test_insert(
                msg["object_id"],
                msg["status"],
                json.dumps(order))

            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")