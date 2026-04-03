from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
from databricks import sql

DEFAULT_SERVER_HOSTNAME = "bolt-incentives.cloud.databricks.com"
DEFAULT_HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"


def _read_access_token() -> Optional[str]:
    token = os.environ.get("DATABRICKS_TOKEN")
    if token and token.strip():
        return token.strip()

    token_path = os.path.expanduser("~/.databricks_token")
    if os.path.exists(token_path):
        with open(token_path, "r", encoding="utf-8") as f:
            token_from_file = f.read().strip()
        return token_from_file or None

    return None


@dataclass
class DBX:
    server_hostname: str = DEFAULT_SERVER_HOSTNAME
    http_path: str = DEFAULT_HTTP_PATH
    access_token: Optional[str] = None

    _conn: Any = None

    def __enter__(self) -> "DBX":
        connect_args: dict[str, Any] = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
        }

        token = self.access_token or _read_access_token()
        if token:
            connect_args["access_token"] = token
        else:
            connect_args["auth_type"] = "databricks-oauth"

        self._conn = sql.connect(**connect_args)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def query(self, query: str) -> pd.DataFrame:
        if self._conn is None:
            raise RuntimeError("DBX connection not open. Use `with DBX() as dbx:`.")

        with self._conn.cursor() as cur:
            cur.execute(query)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

        return pd.DataFrame(rows, columns=cols)

