import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel




class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def test_insert(self,
                    object_id: int,
                    status: str,
                    payload: str
                    ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.test_cdm(
                        object_id,
                        status,
                        payload
                    )
                    VALUES(
                        %(object_id)s,
                        %(status)s,
                        %(payload)s
                    )
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        status = EXCLUDED.status,
                        payload = EXCLUDED.payload
                    ;
                    """,
                    {
                        'object_id': object_id,
                        'status': status,
                        'payload': payload
                    }
                )
