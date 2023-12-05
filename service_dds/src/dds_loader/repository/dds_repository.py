import uuid
from uuid import UUID, uuid5
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel




class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db



# ______________________ ХАБЫ ______________________

    # h.1 ______ Загрузка dds.h_user ______ 
    def dds_h_user_insert(self,
                          h_user_pk: uuid,
                          user_id: str,
                          load_dt: datetime,
                          load_src: str
                    ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_user(
                        h_user_pk,
                        user_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_user_pk)s,
                        %(user_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_user_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt
                    ;
                    """,
                    {
                        'h_user_pk': h_user_pk,
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    # h.2 ______ Загрузка dds.h_product ______ 
    def dds_h_product_insert(self,
                          h_product_pk: uuid,
                          product_id: str,
                          load_dt: datetime,
                          load_src: str
                    ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_product(
                        h_product_pk,
                        product_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_product_pk)s,
                        %(product_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_product_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt
                    ;
                    """,
                    {
                        'h_product_pk': h_product_pk,
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    # h.3 ______ Загрузка dds.h_category ______ 
    def dds_h_category_insert(self,
                          h_category_pk: uuid,
                          category_name: str,
                          load_dt: datetime,
                          load_src: str
                    ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_category(
                        h_category_pk,
                        category_name,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_category_pk)s,
                        %(category_name)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_category_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt
                    ;
                    """,
                    {
                        'h_category_pk': h_category_pk,
                        'category_name': category_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    # h.4 ______ Загрузка dds.h_restaurant ______ 
    def dds_h_restaurant_insert(self,
                          h_restaurant_pk: uuid,
                          restaurant_id: str,
                          load_dt: datetime,
                          load_src: str
                    ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_restaurant(
                        h_restaurant_pk,
                        restaurant_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_restaurant_pk)s,
                        %(restaurant_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_restaurant_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt
                    ;
                    """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'restaurant_id': restaurant_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    # h.5 ______ Загрузка dds.h_order ______ 
    def dds_h_order_insert(self,
                          h_order_pk: uuid,
                          order_id: int,
                          order_dt: datetime,
                          load_dt: datetime,
                          load_src: str
                    ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_order(
                        h_order_pk,
                        order_id,
                        order_dt,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_order_pk)s,
                        %(order_id)s,
                        %(order_dt)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_order_pk) DO UPDATE
                    SET
                        order_dt = EXCLUDED.order_dt,
                        load_dt = EXCLUDED.load_dt

                    ;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


# ______________________ ЛИНКИ ______________________


    # l.1 ______ Загрузка dds.l_order_product ______ 
    def dds_l_order_product_insert(self,
                                   hk_order_product_pk: uuid,
                                   h_order_pk: uuid,
                                   h_product_pk: uuid,
                                   load_dt: datetime,
                                   load_src: str
                                   ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_product(
                        hk_order_product_pk,
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_order_product_pk)s,
                        %(h_order_pk)s,
                        %(h_product_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_order_product_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt
                    ;
                    """,
                    {
                        'hk_order_product_pk': hk_order_product_pk,
                        'h_order_pk': h_order_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    # l.2 ______ Загрузка dds.l_product_restaurant ______ 
    def dds_l_product_restaurant_insert(self,
                                   hk_product_restaurant_pk: uuid,
                                   h_product_pk: uuid,
                                   h_restaurant_pk: uuid,
                                   load_dt: datetime,
                                   load_src: str
                                   ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_restaurant(
                        hk_product_restaurant_pk,
                        h_product_pk,
                        h_restaurant_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_product_restaurant_pk)s,
                        %(h_product_pk)s,
                        %(h_restaurant_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt
                    ;
                    """,
                    {
                        'hk_product_restaurant_pk': hk_product_restaurant_pk,
                        'h_product_pk': h_product_pk,
                        'h_restaurant_pk': h_restaurant_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    # l.3 ______ Загрузка dds.l_product_category ______ 
    def dds_l_product_category_insert(self,
                                   hk_product_category_pk: uuid,
                                   h_product_pk: uuid,
                                   h_category_pk: uuid,
                                   load_dt: datetime,
                                   load_src: str
                                   ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_category(
                        hk_product_category_pk,
                        h_product_pk,
                        h_category_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_product_category_pk)s,
                        %(h_product_pk)s,
                        %(h_category_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_product_category_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt
                    ;
                    """,
                    {
                        'hk_product_category_pk': hk_product_category_pk,
                        'h_product_pk': h_product_pk,
                        'h_category_pk': h_category_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )



    # l.4 ______ Загрузка dds.l_order_user ______ 
    def dds_l_order_user_insert(self,
                                   hk_order_user_pk: uuid,
                                   h_order_pk: uuid,
                                   h_user_pk: uuid,
                                   load_dt: datetime,
                                   load_src: str
                                   ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_user(
                        hk_order_user_pk,
                        h_order_pk,
                        h_user_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_order_user_pk)s,
                        %(h_order_pk)s,
                        %(h_user_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_order_user_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt
                    ;
                    """,
                    {
                        'hk_order_user_pk': hk_order_user_pk,
                        'h_order_pk': h_order_pk,
                        'h_user_pk': h_user_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


# ______________________ САТЕЛЛИТЫ ______________________


    # s.1 ______ Загрузка dds.s_user_names ______ 
    def dds_s_user_names_insert(self,
                                   h_user_pk: uuid,
                                   username: str,
                                   userlogin: str,
                                   load_dt: datetime,
                                   load_src: str,
                                   hk_user_names_hashdiff: uuid
                                   ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_user_names(
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src,
                        hk_user_names_hashdiff
                    )
                    VALUES(
                        %(h_user_pk)s,
                        %(username)s,
                        %(userlogin)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_user_names_hashdiff)s
                    )
                    ON CONFLICT (h_user_pk, load_dt) DO UPDATE
                    SET
                        username = EXCLUDED.username,
                        userlogin = EXCLUDED.userlogin,
                        hk_user_names_hashdiff = EXCLUDED.hk_user_names_hashdiff
                    ;
                    """,
                    {
                        'h_user_pk': h_user_pk,
                        'username': username,
                        'userlogin': userlogin,
                        'load_dt': load_dt,
                        'load_src': load_src,
                        'hk_user_names_hashdiff': hk_user_names_hashdiff
                    }
                )



    # s.2 ______ Загрузка dds.s_product_names ______ 
    def dds_s_product_names_insert(self,
                                   h_product_pk: uuid,
                                   name: str,
                                   load_dt: datetime,
                                   load_src: str,
                                   hk_product_names_hashdiff: uuid
                                   ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_product_names(
                        h_product_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_product_names_hashdiff
                    )
                    VALUES(
                        %(h_product_pk)s,
                        %(name)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_product_names_hashdiff)s
                    )
                    ON CONFLICT (h_product_pk, load_dt) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        hk_product_names_hashdiff = EXCLUDED.hk_product_names_hashdiff
                    ;
                    """,
                    {
                        'h_product_pk': h_product_pk,
                        'name': name,
                        'load_dt': load_dt,
                        'load_src': load_src,
                        'hk_product_names_hashdiff': hk_product_names_hashdiff
                    }
                )





    # s.3 ______ Загрузка dds.s_restaurant_names ______ 
    def dds_s_restaurant_names_insert(self,
                                   h_restaurant_pk: uuid,
                                   name: str,
                                   load_dt: datetime,
                                   load_src: str,
                                   hk_restaurant_names_hashdiff: uuid
                                   ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_restaurant_names(
                        h_restaurant_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_restaurant_names_hashdiff
                    )
                    VALUES(
                        %(h_restaurant_pk)s,
                        %(name)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_restaurant_names_hashdiff)s
                    )
                    ON CONFLICT (h_restaurant_pk, load_dt) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        hk_restaurant_names_hashdiff = EXCLUDED.hk_restaurant_names_hashdiff
                    ;
                    """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'name': name,
                        'load_dt': load_dt,
                        'load_src': load_src,
                        'hk_restaurant_names_hashdiff': hk_restaurant_names_hashdiff
                    }
                )




    # s.4 ______ Загрузка dds.s_order_cost ______ 
    def dds_s_order_cost_insert(self,
                                   h_order_pk: uuid,
                                   cost: float,
                                   payment: float,
                                   load_dt: datetime,
                                   load_src: str,
                                   hk_order_cost_hashdiff: uuid
                                   ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_cost(
                        h_order_pk,
                        cost,
                        payment,
                        load_dt,
                        load_src,
                        hk_order_cost_hashdiff
                    )
                    VALUES(
                        %(h_order_pk)s,
                        %(cost)s,
                        %(payment)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_order_cost_hashdiff)s
                    )
                    ON CONFLICT (h_order_pk, load_dt) DO UPDATE
                    SET
                        cost = EXCLUDED.cost,
                        payment = EXCLUDED.payment,
                        hk_order_cost_hashdiff = EXCLUDED.hk_order_cost_hashdiff
                    ;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'cost': cost,
                        'payment': payment,
                        'load_dt': load_dt,
                        'load_src': load_src,
                        'hk_order_cost_hashdiff': hk_order_cost_hashdiff
                    }
                )




    # s.5 ______ Загрузка dds.s_order_status ______ 
    def dds_s_order_status_insert(self,
                                   h_order_pk: uuid,
                                   status: str,
                                   load_dt: datetime,
                                   load_src: str,
                                   hk_order_status_hashdiff: uuid
                                   ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_status(
                        h_order_pk,
                        status,
                        load_dt,
                        load_src,
                        hk_order_status_hashdiff
                    )
                    VALUES(
                        %(h_order_pk)s,
                        %(status)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_order_status_hashdiff)s
                    )
                    ON CONFLICT (h_order_pk, load_dt) DO UPDATE
                    SET
                        status = EXCLUDED.status,
                        hk_order_status_hashdiff = EXCLUDED.hk_order_status_hashdiff
                    ;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'status': status,
                        'load_dt': load_dt,
                        'load_src': load_src,
                        'hk_order_status_hashdiff': hk_order_status_hashdiff
                    }
                )






