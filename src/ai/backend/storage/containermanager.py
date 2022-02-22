from __future__ import annotations
from inspect import _Object

from typing import Any, List, Mapping

import aiodocker

class ContainerManager:

    contaner_list: Any

    def __init__(
        self,
        endpoint: str,
        user: str,
        password: str,
        container_list: Any,
    ) -> None:

        self.container_list = container_list

    async def aclose(self) -> None:
        await self._session.close()

    async def list_containers(self):
        pass

    async def list_filebrowser_containers(self):
        pass

    async def run_container(self, container_id):
        pass

    async def check_container_network_usage(self, container_id):
        pass

    async def get_container_by_id(self, container_id):
        return self.container

    async def get_container_status(self, container_id):
        pass

    async def edit_container_status(self, container_id):
        pass

    async def stop_container(self, contaier_id):
        pass

    async def delete_container(self, contaier_id):
        pass
