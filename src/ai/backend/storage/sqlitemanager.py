import sqlite3
from typing import Any, Mapping

class DBManager:
    endpoint: str
    port: str
    user: str
    password: str
    session: Any

    def __init__(
        self,
        endpoint: str,
        port: str,
        user: str,
        password: str,
    ) -> None:

        self.endpoint = endpoint
        self.port = port
        self.user = user
        self.password = password

    async def aclose(self) -> None:
        await self.session.close()

    def select_query(self, query):
        pass

    def insert_query(self, query):
        pass

    def delete_query(self, query):
        pass

