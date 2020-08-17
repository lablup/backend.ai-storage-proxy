from __future__ import annotations

import time
from typing import (
    Any,
    AsyncGenerator,
    Mapping,
)

import aiohttp
from yarl import URL


class PurityClient:

    def __init__(self, endpoint: str, api_token: str) -> None:
        self.endpoint = URL(endpoint)
        self.api_token = api_token
        self.auth_token = None

    async def __aenter__(self) -> PurityClient:
        self._session = aiohttp.ClientSession()
        async with self._session.post(
            self.endpoint / 'api' / 'login',
            headers={'api-token': self.api_token},
            ssl=False,
            raise_for_status=True,
        ) as resp:
            self.auth_token = resp.headers['x-auth-token']
            _ = await resp.json()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self._session.close()

    async def get_nfs_metric(self, fs_name: str) -> AsyncGenerator[Mapping[str, Any], None]:
        if self.auth_token is None:
            raise RuntimeError('The auth token for Purity API is not initialized.')
        pagination_token = ''
        while True:
            async with self._session.get(
                self.endpoint / 'api' / '1.8' / 'file-systems' / 'performance',
                headers={'x-auth-token': self.auth_token},
                params={
                    'names': fs_name,
                    'protocol': 'NFS',
                    'items_returned': 10,
                    'token': pagination_token,
                },
                ssl=False,
                raise_for_status=True,
            ) as resp:
                data = await resp.json()
                for item in data['items']:
                    yield item
                pagination_token = data['pagination_info']['continuation_token']
                if pagination_token is None:
                    break
