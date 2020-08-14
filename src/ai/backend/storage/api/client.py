"""
Client-facing API
"""

import asyncio
import logging
from pathlib import Path
from typing import (
    Final,
)

from aiohttp import web
import aiohttp_cors
import jwt

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.utils import AsyncFileWriter

from ..context import Context
from ..exception import InvalidAPIParameters

log = BraceStyleAdapter(logging.getLogger(__name__))

DEFAULT_CHUNK_SIZE: Final = 256 * 1024  # 256 KiB
DEFAULT_INFLIGHT_CHUNKS: Final = 8

# TODO: migrate vfolder operations here


async def download(request: web.Request) -> web.StreamResponse:
    # accept JWT token and reply using web.FileRespnose
    pass


async def upload(request: web.Request) -> web.StreamResponse:
    # accept JWT token and implement tus.io server-side protocol
    pass


async def tus_check_session(request: web.Request) -> web.Response:
    try:
        secret = request.app['config']['manager']['secret']
        token = request.match_info['session']
        params = jwt.decode(token, secret, algorithms=['HS256'])
    except jwt.PyJWTError:
        log.exception('jwt error while parsing "{}"', token)
        raise InvalidAPIParameters(msg="Could not validate the upload session token.")

    headers = await tus_session_headers(request, params)
    return web.Response(headers=headers)


async def tus_upload_part(request: web.Request) -> web.Response:
    try:
        secret = request.app['config']['manager']['secret']
        token = request.match_info['session']
        params = jwt.decode(token, secret, algorithms=['HS256'])
    except jwt.PyJWTError:
        log.exception('jwt error while parsing "{}"', token)
        raise InvalidAPIParameters(msg="Could not validate the upload session token.")

    headers = await tus_session_headers(request, params)

    folder_path = (request.app['VFOLDER_MOUNT'] / params['host'] /
                   request.app['VFOLDER_FSPREFIX'] / params['folder'])
    upload_base = folder_path / ".upload"
    target_filename = upload_base / params['session_id']

    async with AsyncFileWriter(
            loop=asyncio.get_running_loop(),
            target_filename=target_filename,
            access_mode='ab',
            max_chunks=DEFAULT_INFLIGHT_CHUNKS) as writer:
        while not request.content.at_eof():
            chunk = await request.content.read(DEFAULT_CHUNK_SIZE)
            await writer.write(chunk)

    fs = Path(target_filename).stat().st_size
    if fs >= int(params['size']):
        target_path = folder_path / params['path']
        Path(target_filename).rename(target_path)
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: upload_base.rmdir())
        except OSError:
            pass

    headers['Upload-Offset'] = str(fs)
    return web.Response(status=204, headers=headers)


async def tus_options(request: web.Request) -> web.Response:
    headers = {}
    headers["Access-Control-Allow-Origin"] = "*"
    headers["Access-Control-Allow-Headers"] = \
        "Tus-Resumable, Upload-Length, Upload-Metadata, Upload-Offset, Content-Type"
    headers["Access-Control-Expose-Headers"] = \
        "Tus-Resumable, Upload-Length, Upload-Metadata, Upload-Offset, Content-Type"
    headers["Access-Control-Allow-Methods"] = "*"
    headers["Tus-Resumable"] = "1.0.0"
    headers["Tus-Version"] = "1.0.0"
    headers["Tus-Max-Size"] = "107374182400"  # 100G TODO: move to settings
    headers["X-Content-Type-Options"] = "nosniff"
    return web.Response(headers=headers)


async def tus_session_headers(request: web.Request, params) -> web.Response:
    folder_path = (request.app['VFOLDER_MOUNT'] / params['host'] /
                   request.app['VFOLDER_FSPREFIX'] / params['folder'])
    upload_base = folder_path / ".upload"
    base_file = upload_base / params['session_id']
    if not Path(base_file).exists():
        raise web.HTTPNotFound()
    headers = {}
    headers["Access-Control-Allow-Origin"] = "*"
    headers["Access-Control-Allow-Headers"] = \
        "Tus-Resumable, Upload-Length, Upload-Metadata, Upload-Offset, Content-Type"
    headers["Access-Control-Expose-Headers"] = \
        "Tus-Resumable, Upload-Length, Upload-Metadata, Upload-Offset, Content-Type"
    headers["Access-Control-Allow-Methods"] = "*"
    headers["Cache-Control"] = "no-store"
    headers["Tus-Resumable"] = "1.0.0"
    headers['Upload-Offset'] = str(Path(base_file).stat().st_size)
    headers['Upload-Length'] = str(params['size'])
    return headers


async def init_client_app(ctx: Context) -> web.Application:
    app = web.Application()
    app['ctx'] = ctx
    default_cors_options = {
        '*': aiohttp_cors.ResourceOptions(
            allow_credentials=False,
            expose_headers="*", allow_headers="*"),
    }
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    cors.add(add_route('POST', '/download', download))
    cors.add(add_route('POST', '/upload', upload))
    add_route('OPTIONS', r'/upload/{session}', tus_options)
    add_route('HEAD',    r'/upload/{session}', tus_check_session)
    add_route('PATCH',   r'/upload/{session}', tus_upload_part)
    return app
