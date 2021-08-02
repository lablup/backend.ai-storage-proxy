from __future__ import annotations

import aiohttp
from yarl import URL


class QuotaManager:
    endpoint: URL
    user: str
    password: str
    _session: aiohttp.ClientSession
    volume_op: str

    def __init__(self, endpoint: str, user: str, password: str, volume_op: str) -> None:
        self.endpoint = URL(endpoint)
        self.user = user
        self.password = password
        self._session = aiohttp.ClientSession()
        self.volume_op = volume_op

    def list_quotarule(self):
        qr_api_url = "https://{}/api/storage/quota/rules".format(self.endpoint)

        async with self._session.get(
            qr_api_url,
            auth=aiohttp.BasicAuth("admin", "Netapp1!"),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            await self._session.close()
            if "error" in data:
                return None
            return data

    async def create_quotarule(self):
        pass

    async def patch_quotarule(self):
        pass

    async def celete_quotarule(self):
        pass
