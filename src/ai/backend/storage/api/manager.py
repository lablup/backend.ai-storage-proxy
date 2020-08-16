"""
Manager-facing API
"""

import logging
from typing import (
    List,
)

from aiohttp import web
import jwt
import trafaret as t

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter

from ..context import Context
from ..types import VFolderCreationOptions
from ..utils import check_params, log_manager_api_entry

log = BraceStyleAdapter(logging.getLogger(__name__))


async def get_status(request: web.Request) -> web.Response:
    async with check_params(request, None) as params:
        await log_manager_api_entry(log, 'get_status', params)
        return web.json_response({
            'status': 'ok',
        })


async def get_volumes(request: web.Request) -> web.Response:

    async def _get_caps(ctx: Context, volume_name: str) -> List[str]:
        async with ctx.get_volume(volume_name) as volume:
            return [*await volume.get_capabilities()]

    async with check_params(request, None) as params:
        await log_manager_api_entry(log, 'get_volumes', params)
        ctx: Context = request.app['ctx']
        volumes = ctx.list_volumes()
        return web.json_response({
            'volumes': [
                {
                    'name': name,
                    'backend': info.backend,
                    'path': str(info.path),
                    'fsprefix': str(info.fsprefix),
                    'capabilities': await _get_caps(ctx, name),
                }
                for name, info in volumes.items()
            ],
        })


async def create_vfolder(request: web.Request) -> web.Response:
    async with check_params(request, t.Dict({
        t.Key('volume'): t.String(),
        t.Key('vfid'): t.UUID(),
        t.Key('options', default=None): t.Null | VFolderCreationOptions.as_trafaret(),
    })) as params:
        await log_manager_api_entry(log, 'create_vfolder', params)
        ctx: Context = request.app['ctx']
        async with ctx.get_volume(params['volume']) as volume:
            await volume.create_vfolder(params['vfid'], params['options'])
            return web.Response(status=204)


async def delete_vfolder(request: web.Request) -> web.Response:
    async with check_params(request, t.Dict({
        t.Key('volume'): t.String(),
        t.Key('vfid'): t.UUID(),
    })) as params:
        await log_manager_api_entry(log, 'delete_vfolder', params)
        ctx: Context = request.app['ctx']
        async with ctx.get_volume(params['volume']) as volume:
            await volume.delete_vfolder(params['vfid'])
            return web.Response(status=204)


async def clone_vfolder(request: web.Request) -> web.Response:
    async with check_params(request, t.Dict({
        t.Key('volume'): t.String(),
        t.Key('src_vfid'): t.UUID(),
        t.Key('new_vfid'): t.UUID(),
    })) as params:
        await log_manager_api_entry(log, 'clone_vfolder', params)
        ctx: Context = request.app['ctx']
        async with ctx.get_volume(params['volume']) as volume:
            await volume.clone_vfolder(params['src_vfid'], params['new_vfid'])
            return web.Response(status=204)


async def get_performance_metric(request: web.Request) -> web.Response:
    async with check_params(request, t.Dict({
        t.Key('volume'): t.String(),
    })) as params:
        await log_manager_api_entry(log, 'get_performance_metric', params)
        return web.json_response({
            'status': 'ok',
        })


async def get_metadata(request: web.Request) -> web.Response:
    async with check_params(request, t.Dict({
        t.Key('volume'): t.String(),
        t.Key('vfid'): t.UUID(),
    })) as params:
        await log_manager_api_entry(log, 'get_metadata', params)
        return web.json_response({
            'status': 'ok',
        })


async def set_metadata(request: web.Request) -> web.Response:
    async with check_params(request, t.Dict({
        t.Key('volume'): t.String(),
        t.Key('vfid'): t.UUID(),
        t.Key('payload'): t.Bytes(),
    })) as params:
        await log_manager_api_entry(log, 'set_metadata', params)
        return web.json_response({
            'status': 'ok',
        })


async def create_download_session(request: web.Request) -> web.Response:
    async with check_params(request, t.Dict({
        t.Key('volume'): t.String(),
        t.Key('vfid'): t.UUID(),
        t.Key('relpath'): tx.PurePath(relative_only=True),
    })) as params:
        await log_manager_api_entry(log, 'create_download_session', params)
        return web.json_response({
            'status': 'ok',
            'token': '<JWT>',
        })


async def create_upload_session(request: web.Request) -> web.Response:
    async with check_params(request, t.Dict({
        t.Key('volume'): t.String(),
        t.Key('vfid'): t.UUID(),
        t.Key('relpath'): tx.PurePath(relative_only=True),
    })) as params:
        await log_manager_api_entry(log, 'create_upload_session', params)
        return web.json_response({
            'status': 'ok',
            'token': '<JWT>',
        })


async def delete_files(request: web.Request) -> web.Response:
    async with check_params(request, t.Dict({
        t.Key('volume'): t.String(),
        t.Key('vfid'): t.UUID(),
        t.Key('relpaths'): t.List(tx.PurePath(relative_only=True)),
    })) as params:
        await log_manager_api_entry(log, 'delete_files', params)
        return web.json_response({
            'status': 'ok',
        })


async def init_manager_app(ctx: Context) -> web.Application:
    app = web.Application()
    app['ctx'] = ctx
    app.router.add_route('GET', '/', get_status)
    app.router.add_route('GET', '/volumes', get_volumes)
    app.router.add_route('POST', '/folder/create', create_vfolder)
    app.router.add_route('POST', '/folder/delete', delete_vfolder)
    app.router.add_route('POST', '/folder/clone', clone_vfolder)
    app.router.add_route('GET', '/volume/performance-metric', get_performance_metric)
    app.router.add_route('GET', '/folder/metadata', get_metadata)
    app.router.add_route('POST', '/folder/metadata', set_metadata)
    app.router.add_route('POST', '/folder/file/download', create_download_session)
    app.router.add_route('POST', '/folder/file/upload', create_upload_session)
    app.router.add_route('POST', '/folder/file/delete', create_upload_session)
    return app
