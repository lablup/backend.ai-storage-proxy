from __future__ import annotations

import json
from typing import Any, Mapping

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
        volume_uuid = await self.get_volume_uuid_by_name()
        data = await self.get_volume_info(volume_uuid)
        qos = await self.get_qos_by_volume_id(volume_uuid)
        qos_policies = await self.get_qos_policies()
        qtree_metadata = await self.get_default_qtree_by_volume_id(volume_uuid)
        qtree = await self.get_qtree_info(qtree_metadata.get("id"))

        # mapping certain data for better explanation
        volume_qtree_cluster = {
            # ------ use volume info ------
            "id": data["uuid"],
            "local_tier": data["aggregates"][0]["name"],
            "create_time": data["create_time"],
            "snapshot_policy": data["snapshot_policy"]["name"],
            "snapmirroring": str(data["snapmirror"]["is_protected"]),
            "state": data["state"],
            "style": data["style"],
            "svm_name": data["svm"]["name"],
            "svm_id": data["svm"]["uuid"],
            # ------ use qtree info ------
            "name": qtree["name"],
            "path": qtree["path"],
            "security_style": qtree["security_style"],
            "export_policy": qtree["export_policy"]["name"],
            "timestamp": qtree["statistics"].get("timestamp"),  # last check time
        }
        # optional values to add in volume_qtree_cluster
        if qos:
            volume_qtree_cluster.update({
                "qos": json.dumps(qos["policy"])
            })
        if qos_policies:
            volume_qtree_cluster.update({
                "qos_policies": json.dumps(qos_policies)
            })
        return volume_qtree_cluster

    async def get_usage(self) -> Mapping[str, Any]:
        # volume specific usage check
        uuid = await self.get_volume_uuid_by_name()
        data = await self.get_volume_info(uuid)
        return {
            "capacity_bytes": data["space"]["available"],
            "used_bytes": data["space"]["used"],
        }

    async def get_list_volumes(self) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/volumes",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
        return data["records"]

    async def get_volume_name_by_uuid(self, volume_uuid) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/volumes?uuid={volume_uuid}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            name = data["records"][0]["name"]
        return name

    async def get_volume_uuid_by_name(self) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/volumes?name={self.volume_name}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            uuid = data["records"][0]["uuid"]
        return uuid

    async def get_volume_info(self, volume_uuid) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/volumes/{volume_uuid}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
        return data

    async def get_default_qtree_by_volume_id(self, volume_uuid) -> Mapping[str, Any]:
        qtrees = await self.list_qtrees_by_volume_id(volume_uuid)
        for qtree in qtrees:
            # skip the default qtree made by NetApp ONTAP internally
            # It will not be used in Backend.AI NetApp ONTAP Plugin
            if not qtree["name"]:
                continue
            else:
                return qtree

    async def get_qtree_name_by_id(self, qtree_id) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/qtrees?id={qtree_id}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
        return data["name"]

    async def get_qtree_id_by_name(self, qtree_name) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/qtrees?name={qtree_name}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            id = str(data["records"][0]["id"]) if data["num_records"] > 0 else ""
        return id

    async def get_qtree_path(self, qtree_id) -> Mapping[str, Any]:
        volume_uuid = await self.get_volume_uuid_by_name()
        async with self._session.get(
            f"{self.endpoint}/api/storage/qtrees/{volume_uuid}/{qtree_id}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
        return data["path"]

    async def list_qtrees_by_volume_id(self, volume_uuid) -> Mapping[str, Any]:
        if not volume_uuid:
            volume_uuid = await self.get_volume_uuid_by_name()
        async with self._session.get(
            f"{self.endpoint}/api/storage/qtrees/{volume_uuid}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
        return data["records"]

    # For now, Only Read / Update operation for qtree is available
    # in NetApp ONTAP Plugin of Backend.AI
    async def create_qtree(self, name) -> Mapping[str, Any]:

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
            return await resp.json()

    async def update_qtree(self, config) -> Mapping[str, Any]:
        payload = {
            "name": config.get("name"),
            "security_style": config.get("security_style"),
        }
        qtree_id = config["id"]
        volume_uuid = await self.get_volume_uuid_by_name()
        headers = {"content-type": "application/json", "accept": "application/hal+json"}
        async with self._session.patch(
            f"{self.endpoint}/api/storage/qtrees/{volume_uuid}/{qtree_id}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
            data=json.dumps(payload),
        ) as resp:
            return await resp.json()

    # For now, Only Read / Update operation for qtree is available
    # in NetApp ONTAP Plugin of Backend.AI
    async def delete_qtree(self, qtree_id) -> Mapping[str, Any]:
        volume_uuid = await self.get_volume_uuid_by_name()
        headers = {"content-type": "application/json", "accept": "application/hal+json"}
        async with self._session.delete(
            f"{self.endpoint}/api/storage/qtrees/{volume_uuid}/{qtree_id}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
        ) as resp:
            return await resp.json()

    async def get_qtree_info(self, qtree_id) -> Mapping[str, Any]:
        uuid = await self.get_volume_uuid_by_name()
        async with self._session.get(
            f"{self.endpoint}/api/storage/qtrees/{uuid}/{qtree_id}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
        return data

    async def get_qtree_config(self, qtree_id) -> Mapping[str, Any]:
        qtree = await self.get_qtree_info(qtree_id)
        qtree_config = {
            "qtree_name": qtree["name"],
            "security_style": qtree["security_style"],
            "export_policy": qtree["export_policy"]["name"],
        }
        return qtree_config

    async def update_qtree_config(self, config) -> Mapping[str, Any]:
        resp = await self.update_qtree(config)
        return resp

    async def get_qos_policies(self) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/qos/policies",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            qos_policies_metadata = data["records"]
            qos_policies = []
            for qos in qos_policies_metadata:
                policy = await self.get_qos_by_uuid(qos["uuid"])
                qos_policies.append(policy)
        return qos_policies

    async def get_qos_by_uuid(self, qos_uuid) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/qos/policies/{qos_uuid}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            fixed = data["fixed"]
            qos_policy = {
                "uuid": data["uuid"],
                "name": data["name"],
                "fixed": {
                    "max_throughput_iops": fixed.get("max_throughput_iops", 0),
                    "max_throughput_mbps": fixed.get("max_throughput_mbps", 0),
                    "min_throughput_iops": fixed.get("min_throughput_iops", 0),
                    "min_throughput_mbps": fixed.get("min_throughput_mbps", 0),
                    "capacity_shared": fixed["capacity_shared"],
                },
                "svm": data["svm"]
            }
            return qos_policy

    async def get_qos_by_volume_id(self, volume_uuid) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/volumes/{volume_uuid}?fields=qos",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
        return data["qos"]

    async def get_qos_by_qos_name(self, qos_name) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self.endpoint}/api/storage/qos/policies?name={qos_name}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            data = await resp.json()
            qos = {}
            if data["num_records"]:
                qos_metadata = data["records"][0]
                raw_qos = await self.get_qos_by_uuid(qos_metadata["uuid"])
                qos = {
                    "uuid": raw_qos["uuid"],
                    "name" : raw_qos["name"],
                    "svm" : raw_qos["svm"],
                    "fixed": raw_qos["fixed"]
                }
            else:
                resp = qos
            return qos

    async def create_qos(self, qos):
        payload = {
            "name": qos["name"],
            "fixed": qos["input"]["fixed"],
            "svm": qos["input"]["svm"]
        }

        headers = {"content-type": "application/json", "accept": "application/hal+json"}
        
        async with self._session.post(
            f"{self.endpoint}/api/storage/qos/policies",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
            data=json.dumps(payload),
        ) as resp:
            return await resp.json()

    async def update_qos(self, qos):

        payload = {
            "name": qos["input"]["name"],
            "fixed": qos["input"]["fixed"],
        }
        
        headers = {"content-type": "application/json", "accept": "application/hal+json"}

        async with self._session.patch(
            f"{self.endpoint}/api/storage/qos/policies/{qos['input']['uuid']}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
            data=json.dumps(payload),
        ) as resp:
            ic(resp)
            return await resp.json()

    async def delete_qos(self, qos_name):
        qos_metadata = await self.get_qos_by_qos_name(qos_name)
        
        async with self._session.delete(
            f"{self.endpoint}/api/storage/qos/policies/{qos_metadata['uuid']}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            raise_for_status=True,
        ) as resp:
            return await resp.json()

    async def update_volume_config(self, config):
        uuid = await self.get_volume_uuid_by_name()
        headers = {"content-type": "application/json", "accept": "application/hal+json"}
        async with self._session.patch(
            f"{self.endpoint}/api/storage/volumes/{uuid}",
            auth=aiohttp.BasicAuth(self.user, self.password),
            ssl=False,
            headers=headers,
            raise_for_status=True,
            data=json.dumps(config["input"]),
        ) as resp:
            return await resp.json()