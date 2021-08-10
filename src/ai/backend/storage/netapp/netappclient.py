from __future__ import annotations

from typing import Any, AsyncGenerator, Mapping

import aiohttp
from yarl import URL
import asyncio
import json


class NetAppClient:

    endpoint: URL
    user: str
    password: str
    _session: aiohttp.ClientSession
    svm: str
    volume_name: str

    def __init__(
        self,
        endpoint: str,
        user: str,
        password: str,
        svm: str,
        volume_name: str
    ) -> None:
        self.endpoint = URL(endpoint)
        self.user = user
        self.password = password
        self.svm = svm
        self.volume_name = volume_name
        self._session = aiohttp.ClientSession()

    async def aclose(self) -> None:
        await self._session.close()

    async def get_list_volumes(self) -> AsyncGenerator[Mapping[str, Any], None]:

        async with self._session.get(
            (self.endpoint / "api/storage/volumes"),
            auth=aiohttp.BasicAuth("admin", "Netapp1!"),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            await self._session.close()
            return data["records"]

    async def get_volume_metrics(
        self, uuid: str
    ) -> AsyncGenerator[Mapping[str, Any], None]:

        async with self._session.get(
            (self.endpoint / "api/storage/volumes" / uuid),
            auth=aiohttp.BasicAuth("admin", "Netapp1!"),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()

        await self._session.close()
        return data

    async def create_qtree(self, name):

        payload = {
                    "svm": {
                        "name": self.svm
                    },
                    "volume": {
                        "name": self.volume_name
                    },
                    "name": name,
                    "security_style": "unix",
                    "unix_permissions": 777,
                    "export_policy": {
                        "name": "default"
                    }
                }

        headers = {"content-type": "application/json", "accept": "application/hal+json"}
        async with self._session.post(
            (self.endpoint / "api/storage/qtrees"),
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers, raise_for_status=True, data=json.dumps(payload)
        ) as resp:
            tmp = await resp.json()
            print(tmp)
            await self._session.close()
            return tmp
