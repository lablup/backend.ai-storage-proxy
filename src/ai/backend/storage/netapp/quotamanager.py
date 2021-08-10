from __future__ import annotations

from typing import Dict

import aiohttp
from yarl import URL


class QuotaManager:
    endpoint: URL
    user: str
    password: str
    _session: aiohttp.ClientSession

    def __init__(self, endpoint: str, user: str, password: str) -> None:
        self.endpoint = URL(endpoint)
        self.user = user
        self.password = password
        self._session = aiohttp.ClientSession()

    async def aclose(self) -> None:
        await self._session.close()

    async def list_quotarule(self):
        qr_api_url = URL("https://{}/api/storage/quota/rules".format(self.endpoint))

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

    async def create_quotarule(
        self,
        volume_uuid,
        svm_name: str,
        volume_name: str,
        quota_type: str,
        qtree_name: str,
        spahali: int,
        spasoli: int,
        fihali: int,
        fisoli: int,
    ) -> None:

        api_url: URL = URL("https://{}/api/storage/quota/rules".format(self.endpoint))
        dataobj: Dict = {}

        dataobj["svm"] = {"name": svm_name}
        dataobj["volume"] = {"name": volume_name}

        if quota_type == "qtree":
            dataobj["qtree"] = {"name": qtree_name}
            dataobj["type"] = "tree"

        if quota_type == "users":
            dataobj["type"] = "user"
            dataobj["user_mapping"] = False
            dataobj["users"] = []

        if quota_type == "group":
            dataobj["type"] = "group"
            dataobj["group"] = {}
        dataobj["space"] = {"hard_limit": spahali, "soft_limit": spasoli}
        dataobj["files"] = {"hard_limit": fihali, "soft_limit": fisoli}

        async with self._session.post(
            api_url,
            auth=aiohttp.BasicAuth("admin", "Netapp1!"),
            json=dataobj,
            ssl=False,
            raise_for_status=True,
        ) as resp:
            await resp.json()
            await self._session.close()

    async def update_quotarule(
        self, quota_uuid: str, spahali: int, spasoli: int, fihali: int, fisoli: int
    ) -> None:
        api_url: URL = URL(
            "https://{}/api/storage/quota/rules/{}".format(self.endpoint, quota_uuid)
        )
        dataobj: Dict = {}
        dataobj["space"] = {"hard_limit": spahali, "soft_limit": spasoli}
        dataobj["files"] = {"hard_limit": fihali, "soft_limit": fisoli}
        async with self._session.patch(
            api_url,
            auth=aiohttp.BasicAuth("admin", "Netapp1!"),
            json=dataobj,
            ssl=False,
            verify=False,
            raise_for_status=True,
        ) as resp:
            await resp.json()
            await self._session.close()

    async def delete_quotarule(self, quota_uuid: str):
        quota_uuid = input("Enter the UUID of the Quota to be deleted :- ")
        api_url: URL = URL(
            "https://{}/api/storage/quota/rules/{}".format(self.endpoint, quota_uuid)
        )
        async with self._session.delete(
            api_url,
            auth=aiohttp.BasicAuth("admin", "Netapp1!"),
            ssl=False,
            verify=False,
            raise_for_status=True,
        ) as resp:
            await resp.json()
            await self._session.close()

    async def show_qtree(self):
        raise NotImplementedError

    async def show_quotarule(self):
        raise NotImplementedError

    async def patch_quotarule(self):
        raise NotImplementedError

    async def celete_quotarule(self):
        raise NotImplementedError

    async def show_svm(self):
        raise NotImplementedError
