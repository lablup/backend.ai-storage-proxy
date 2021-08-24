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

    async def get_metadata(self) -> Mapping[str, Any]:
        uuid = await self.get_volume_uuid_by_name()
        data = await self.get_volume_info(uuid)
        qos = await self.get_qos_by_id(uuid)
        return {
            "id": data["uuid"],
            "name": data["name"],
            "local_tier": data["aggregates"][0]["name"],
            "create_time": data["create_time"],
            "snapshot_policy": data["snapshot_policy"]["name"],
            "snapmirroring": str(data["snapmirror"]["is_protected"]),
            "style": data["style"],
            "state": data["state"],
            "svm_name": data["svm"]["name"],
            "svm_id": data["svm"]["uuid"],
            "qos": json.dumps(qos["policy"]) if qos else None,
        }

    async def get_usage(self) -> Mapping[str, Any]:
        uuid = await self.get_volume_uuid_by_name()
        data = await self.get_volume_info(uuid)
        return {
            "capacity_bytes": data["space"]["available"],
            "used_bytes": data["space"]["used"],
        }

    async def get_list_volumes(self) -> AsyncGenerator[Mapping[str, Any], None]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/volumes",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            # await self._session.close()
        return data["records"]

    async def get_volume_name_by_uuid(self, volume_uuid):
        async with self._session.get(
            f"{self.endpoint}/api/storage/volumes?uuid={volume_uuid}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            name = data["records"][0]["name"]
            # await self._session.close()
        return name

    async def get_volume_uuid_by_name(self):
        async with self._session.get(
            f"{self.endpoint}/api/storage/volumes?name={self.volume_name}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            uuid = data["records"][0]["uuid"]
            # await self._session.close()
        return uuid

    async def get_volume_info(
        self, volume_uuid
    ) -> AsyncGenerator[Mapping[str, Any], None]:
        volume_name = await self.get_volume_name_by_uuid(volume_uuid)
        async with self._session.get(
            f"{self.endpoint}/api/storage/volumes/{volume_uuid}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            # await self._session.close()
        return data

    async def get_qtree_name_by_id(self, qtree_id):
        async with self._session.get(
            f"{self.endpoint}/api/storage/qtrees?id={qtree_id}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            # await self._session.close()
        return data["name"]

    async def get_qtree_id_by_name(self, qtree_name):
        async with self._session.get(
            f"{self.endpoint}/api/storage/qtrees?name={qtree_name}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            # await self._session.close()
        return data["id"]

    async def get_qtree_path(self, qtree_id):
        async with self._session.get(
            f"{self.endpoint}/api/storage/qtrees/{self.volume_uuid}/{self.qtree_id}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            # await self._session.close()
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
            f"{self.endpoint}/api/storage/qtrees",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
            data=json.dumps(payload),
        ) as resp:
            await resp.json()
            # await self._session.close()
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
            f"{self.endpoint}/api/storage/qtrees{self.volume_uuid}/{qtree_id}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
            data=json.dumps(payload),
        ) as resp:
            tmp = await resp.json()
            # await self._session.close()
        return tmp

    async def delete_qtree(self, qtree_id):

        headers = {"content-type": "application/json", "accept": "application/hal+json"}
        async with self._session.delete(
            f"{self.endpoint}/api/storage/qtrees{self.volume_uuid}/{qtree_id}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
        ) as resp:
            await resp.json()
            # await self._session.close()
        return resp

    async def get_qtree_info(self, qtree_id):
        async with self._session.get(
            f"{self.endpoint}/api/storage/qtrees{self.volume_uuid}/{qtree_id}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            # await self._session.close()
        return data

    async def get_qos_by_id(
        self, volume_uuid
    ) -> AsyncGenerator[Mapping[str, Any], None]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/volumes/{volume_uuid}?fields=qos",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
        return data["qos"]
