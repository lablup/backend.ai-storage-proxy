import aiodocker
from typing import Any, Mapping

class FileBrowserContainer:
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

    def get_container(self, container_id):
        pass

    def run_container(self, container_id):
        pass

    def stop_container(self, container_id):
        pass

    def get_network_usage(self, container_id):
        pass

    def check_idle_usage(self, container_id):
        pass

