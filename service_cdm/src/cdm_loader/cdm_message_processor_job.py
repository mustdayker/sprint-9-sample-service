import json
from logging import Logger
from typing import List, Dict
from datetime import datetime
from uuid import UUID, uuid5
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

            # ______ Загрузка cdm.user_product_counters ______
            for product in order['products']:
                self._cdm_repository.cdm_user_product_counters_insert(
                    self._uuid_gen(order["user"]["id"]),
                    self._uuid_gen(product["id"]),
                    product["name"],
                    product["quantity"]
                    )

            # ______ Загрузка cdm.user_category_counters ______
            for product in order['products']:
                self._cdm_repository.cdm_user_category_counters_insert(
                    self._uuid_gen(order["user"]["id"]),
                    self._uuid_gen(product["category"]),
                    product["category"],
                    product["quantity"]
                    )


            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    # Функция для генерации uuid
    def _uuid_gen(self, keygen):
        return uuid5(UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307'), keygen)