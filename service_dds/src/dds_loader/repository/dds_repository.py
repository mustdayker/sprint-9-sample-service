import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel




class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def test_insert(self,
                    object_id: int,
                    object_type: str,
                    payload: str
                    ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.test_dds(
                        object_id,
                        object_type,
                        payload
                    )
                    VALUES(
                        %(object_id)s,
                        %(object_type)s,
                        %(payload)s
                    )
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_type = EXCLUDED.object_type,
                        payload = EXCLUDED.payload
                    ;
                    """,
                    {
                        'object_id': object_id,
                        'object_type': object_type,
                        'payload': payload
                    }
                )