from __future__ import annotations

from typing import Any, Mapping

import aiohttp
from aiohttp.client_reqrep import ClientResponse
from yarl import URL


class QuotaManager:
    endpoint: str
    user: str
    password: str
    _session: aiohttp.ClientSession

    def __init__(
        self, endpoint: str, user: str, password: str, svm: str, volume_name: str
    ) -> None:

        self.endpoint = endpoint
        self.user = user
        self.password = password
        self._session = aiohttp.ClientSession()
        self.svm = svm
        self.volume_name = volume_name
        self._session = aiohttp.ClientSession()

    async def aclose(self) -> None:
        await self._session.close()

    async def list_quotarules(self):
        qr_api_url = URL("https://{}/api/storage/quota/rules".format(self.endpoint))
        async with self._session.get(
            qr_api_url,
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=False,
        ) as resp:

            data = await resp.json()
            await self._session.close()

        rules = [rule for rule in data["uuid"]]
        self.rules = rules
        return rules

    async def list_all_qtrees_with_quotas(self) -> Mapping[str, Any]:
        rules = self.list_quotarules()

        qtrees = {}

        for rule in rules:
            qr_api_url = URL(
                "https://{}/api/storage/quota/rules/{}".format(self.endpoint, rule)
            )

            async with self._session.get(
                qr_api_url,
                auth=aiohttp.BasicAuth(self.user, self.password),
                ssl=False,
                raise_for_status=False,
            ) as resp:
                data = await resp.json()
                qtree_uuid = data["uuid"]
                qtree_name = data["qtree"]["name"]
                qtrees[qtree_uuid] = qtree_name
        self.qtrees = qtrees
        return qtrees

    async def get_quota(self, rule_uuid) -> Mapping[str, Any]:
        qr_api_url = URL(
            "https://{}/api/storage/quota/rules/{}".format(self.endpoint, rule_uuid)
        )

        async with self._session.get(
            qr_api_url,
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=False,
        ) as resp:
            data = await resp.json()
            spaces = data["space"]
        return spaces

    async def create_quotarule_qtree(
        self, qtree_name: str, spahali: int, spasoli: int, fihali: int, fisoli: int
    ) -> Mapping[str, Any]:
        api_url = "https://{}/api/storage/quota/rules".format(self.endpoint)

        dataobj = {
            "svm": {"name": self.svm},
            "volume": {"name": self.volume_name},
            "type": "tree",
            "space": {"hard_limit": spahali, "soft_limit": spasoli},
            "files": {"hard_limit": fihali, "soft_limit": fisoli},
            "qtree": {"name": qtree_name},
        }

        headers = {"content-type": "application/json", "accept": "application/hal+json"}

        async with self._session.post(
            api_url,
            auth=aiohttp.BasicAuth(self.user, self.password),
            headers=headers,
            json=dataobj,
            ssl=False,
            raise_for_status=True,
        ) as resp:

            msg = await resp.json()
        return msg

    async def update_quotarule_qtree(
        self,
        qtree_name: str,
        spahali: int,
        spasoli: int,
        fihali: int,
        fisoli: int,
        rule_uuid,
    ) -> ClientResponse:
        api_url = "https://{}/api/storage/quota/rules/{}".format(
            self.endpoint, rule_uuid
        )

        dataobj = {
            "svm": {"name": self.svm},
            "volume": {"name": self.volume_name},
            "type": "tree",
            "space": {"hard_limit": spahali, "soft_limit": spasoli},
            "files": {"hard_limit": fihali, "soft_limit": fisoli},
            "qtree": {"name": qtree_name},
        }

        headers = {"content-type": "application/json", "accept": "application/hal+json"}

        async with self._session.patch(
            api_url,
            auth=aiohttp.BasicAuth(self.user, self.password),
            headers=headers,
            json=dataobj,
            ssl=False,
            raise_for_status=True,
        ) as resp:
            return await resp.json()

    async def delete_quotarule_qtree(self, rule_uuid) -> ClientResponse:
        api_url = "https://{}/api/storage/quota/rules/{}".format(
            self.endpoint, rule_uuid
        )

        headers = {"content-type": "application/json", "accept": "application/hal+json"}

        async with self._session.delete(
            api_url,
            auth=aiohttp.BasicAuth(self.user, self.password),
            headers=headers,
            ssl=False,
            raise_for_status=True,
        ) as resp:
            return await resp.json()
