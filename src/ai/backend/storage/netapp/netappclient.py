from __future__ import annotations

import json
from typing import Any, AsyncGenerator, Mapping

import aiohttp


class NetAppClient:

    endpoint: str
    user: str
    password: str
    _session: aiohttp.ClientSession
    svm: str
    volume_name: str

    def __init__(
        self, endpoint: str, user: str, password: str, svm: str, volume_name: str
    ) -> None:
        self.endpoint = endpoint
        self.user = user
        self.password = password
        self.svm = svm
        self.volume_name = volume_name
        self._session = aiohttp.ClientSession()

    async def aclose(self) -> None:
        await self._session.close()

    async def get_list_volumes(self) -> AsyncGenerator[Mapping[str, Any], None]:

        async with self._session.get(
            ("https://{}/api/storage/volumes".format(self.endpoint)),
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            await self._session.close()
        return data["records"]

    async def get_volume_uuid_by_name(self):
        async with self._session.get(
            (
                "https://{}/api/storage/volumes?name={}".format(
                    self.endpoint, self.volume_name
                )
            ),
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            await self._session.close()
        return data["uuid"]

    async def get_volume_info(self) -> AsyncGenerator[Mapping[str, Any], None]:

        async with self._session.get(
            "https://{}api/storage/volumes/{}".format(
                self.endpoint, self.get_volume_uuid_by_name()
            ),
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            await self._session.close()
        return data

    async def get_qtree_name_by_id(self, qtree_id):
        api_url = "https://{}/api/storage/qtrees?id={}".format(self.endpoint, qtree_id)
        async with self._session.get(
            api_url,
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            await self._session.close()
        return data["name"]

    async def get_qtree_id_by_name(self, qtree_name):

        api_url = "https://{}/api/storage/qtrees?name={}".format(
            self.endpoint, qtree_name
        )
        async with self._session.get(
            api_url,
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            await self._session.close()
        return data["id"]

    async def get_qtree_path(self, qtree_id):
        async with self._session.get(
            (
                self.endpoint
                / "/api/storage/qtrees/{}/{}".format(self.volume_uuid, self.qtree_id)
            ),
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            await self._session.close()
        return data["path"]

    async def create_qtree(self, name):

        payload = {
            "svm": {"name": self.svm},
            "volume": {"name": self.volume_name},
            "name": name,
            "security_style": "unix",
            "unix_permissions": 777,
            "export_policy": {"name": "default"},
        }

        headers = {"content-type": "application/json", "accept": "application/hal+json"}

        async with self._session.post(
            (self.endpoint / "api/storage/qtrees"),
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
            data=json.dumps(payload),
        ) as resp:
            await resp.json()
            await self._session.close()
        return resp

    async def update_qtree(
        self, qtree_id, qtree_name, security_style, unix_permission, export_policy_name
    ):

        payload = {
            "svm": {"name": self.svm},
            "volume": {"name": self.volume_name},
            "name": qtree_name,
            "security_style": security_style,
            "unix_permissions": unix_permission,
            "export_policy": {"name": export_policy_name},
        }

        headers = {"content-type": "application/json", "accept": "application/hal+json"}
        async with self._session.patch(
            (
                self.endpoint
                / "api/storage/qtrees{}/{}".format(self.volume_uuid, qtree_id)
            ),
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
            data=json.dumps(payload),
        ) as resp:
            tmp = await resp.json()
            await self._session.close()
        return tmp

    async def delete_qtree(self, qtree_id):

        headers = {"content-type": "application/json", "accept": "application/hal+json"}
        async with self._session.delete(
            (
                self.endpoint
                / "api/storage/qtrees{}/{}".format(self.volume_uuid, qtree_id)
            ),
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
        ) as resp:
            await resp.json()
            await self._session.close()
        return resp

    async def get_qtree_info(self, qtree_id):
        async with self._session.get(
            (
                self.endpoint
                / "/api/storage/qtrees/{}/{}".format(self.volume_uuid, qtree_id)
            ),
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            await self._session.close()
        return data
