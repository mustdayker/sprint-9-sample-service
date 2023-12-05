import json
from logging import Logger
from typing import List, Dict
from datetime import datetime
from uuid import UUID, uuid5
from lib.kafka_connect import KafkaConsumer, KafkaProducer


from dds_loader.repository import DdsRepository
import time

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger
                 ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        # Цикл обработки сообщений
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg['payload']

            load_src_var = "orders-system-kafka"

# __________ ЗАГРУЗКА ДАННЫХ POSTGESQL __________

# ______________________ ХАБЫ ______________________

            # h.1 ______ Загрузка dds.h_user ______
            self._dds_repository.dds_h_user_insert(
                self._uuid_gen(order["user"]["id"]),
                order["user"]["id"],
                datetime.utcnow(),
                load_src_var
                )


            # h.2 ______ Загрузка dds.h_product ______
            for product in order['products']:
                self._dds_repository.dds_h_product_insert(
                    self._uuid_gen(product["id"]),
                    product["id"],
                    datetime.utcnow(),
                    load_src_var
                    )


            # h.3 ______ Загрузка dds.h_category ______
            for product in order['products']:
                self._dds_repository.dds_h_category_insert(
                    self._uuid_gen(product["category"]),
                    product["category"],
                    datetime.utcnow(),
                    load_src_var
                    )

            # h.4 ______ Загрузка dds.h_restaurant ______
            self._dds_repository.dds_h_restaurant_insert(
                self._uuid_gen(order["restaurant"]["id"]),
                order["restaurant"]["id"],
                datetime.utcnow(),
                load_src_var
                )


            # h.5 ______ Загрузка dds.h_order ______
            self._dds_repository.dds_h_order_insert(
                self._uuid_gen(str(order["id"])),
                order["id"],
                order["date"],
                datetime.utcnow(),
                load_src_var
                )


# ______________________ ЛИНКИ ______________________

            # l.1 ______ Загрузка dds.l_order_product ______
            for product in order['products']:
                self._dds_repository.dds_l_order_product_insert(
                    self._uuid_gen(f'{order["id"]}{product["id"]}'),
                    self._uuid_gen(str(order["id"])),
                    self._uuid_gen(product["id"]),
                    datetime.utcnow(),
                    load_src_var
                    )

            # l.2 ______ Загрузка dds.l_product_restaurant ______
            for product in order['products']:
                self._dds_repository.dds_l_product_restaurant_insert(
                    self._uuid_gen(f'{product["id"]}{order["restaurant"]["id"]}'),
                    self._uuid_gen(product["id"]),
                    self._uuid_gen(order["restaurant"]["id"]),
                    datetime.utcnow(),
                    load_src_var
                    )


            # l.3 ______ Загрузка dds.l_product_category ______
            for product in order['products']:
                self._dds_repository.dds_l_product_category_insert(
                    self._uuid_gen(f'{product["id"]}{product["category"]}'),
                    self._uuid_gen(product["id"]),
                    self._uuid_gen(product["category"]),
                    datetime.utcnow(),
                    load_src_var
                    )


            # l.4 ______ Загрузка dds.l_order_user ______
            self._dds_repository.dds_l_order_user_insert(
                self._uuid_gen(f'{order["id"]}{order["user"]["id"]}'),
                self._uuid_gen(str(order["id"])),
                self._uuid_gen(order["user"]["id"]),
                datetime.utcnow(),
                load_src_var
                )


# ______________________ САТЕЛЛИТЫ ______________________

            # s.1 ______ Загрузка dds.s_user_names ______
            self._dds_repository.dds_s_user_names_insert(
                self._uuid_gen(order["user"]["id"]),
                order["user"]["name"],
                order["user"]["login"],
                datetime.utcnow(),
                load_src_var,
                self._uuid_gen(order["user"]["name"])
                )


            # s.2 ______ Загрузка dds.s_product_names ______
            for product in order['products']:
                self._dds_repository.dds_s_product_names_insert(
                    self._uuid_gen(product["id"]),
                    product["name"],
                    datetime.utcnow(),
                    load_src_var,
                    self._uuid_gen(product["name"])
                    )

            # s.3 ______ Загрузка dds.s_restaurant_names ______
            self._dds_repository.dds_s_restaurant_names_insert(
                self._uuid_gen(order["restaurant"]["id"]),
                order["restaurant"]["name"],
                datetime.utcnow(),
                load_src_var,
                self._uuid_gen(order["restaurant"]["name"])
                )


            # s.4 ______ Загрузка dds.s_order_cost ______
            self._dds_repository.dds_s_order_cost_insert(
                self._uuid_gen(str(order["id"])),
                order["cost"],
                order["payment"],
                datetime.utcnow(),
                load_src_var,
                self._uuid_gen(str(order["cost"]))
                )


            # s.5 ______ Загрузка dds.s_order_status ______
            self._dds_repository.dds_s_order_status_insert(
                self._uuid_gen(str(order["id"])),
                order["status"],
                datetime.utcnow(),
                load_src_var,
                self._uuid_gen(order["status"])
                )


            # # Создание сообщения для топика dds-service-orders
            # dst_msg = {
            #     "object_id": order["user"]["id"],
            #     "object_type": "cdm_counter",
            #     "payload": {
            #         "category_counter": self._category_counter(order['products']),
            #         "products_counter": self._product_counter(order['products'])
            #     }
            # }
            
            # Создание сообщения для топика dds-service-orders
            dst_msg = {
                "object_id": order["user"]["id"],
                "object_type": "order",
                "payload": {
                    "user": order['user'],
                    "products": order['products']
                }
            }



            # Отправка сообщения в топик dds-service-orders
            self._producer.produce(dst_msg)
            
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    # Функция для генерации uuid
    def _uuid_gen(self, keygen):
        return uuid5(UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307'), keygen)
    
    # # Функция для подсчета категорий
    # def _category_counter(self, cat_array):
    #     df = pd.DataFrame(cat_array)[['category', 'quantity']]
    #     return df.groupby(['category']).sum().reset_index().to_dict('records')
    
    # # Функция для подсчета продуктов
    # def _product_counter(self, prod_array):
    #     df = pd.DataFrame(prod_array)[['id', 'name', 'quantity']]
    #     return df.groupby(['id', 'name']).sum().reset_index().to_dict('records')