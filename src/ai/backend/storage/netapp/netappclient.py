from __future__ import annotations

from typing import Any, AsyncGenerator, Mapping

import aiohttp
from yarl import URL


class NetAppClient:

    endpoint: URL
    user: str
    password: str
    _session: aiohttp.ClientSession

    def __init__(
        self,
        endpoint: str,
        user: str,
        password: str,
    ) -> None:
        self.endpoint = URL(endpoint)
        self.user = user
        self.password = password
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
