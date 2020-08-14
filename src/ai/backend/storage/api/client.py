"""
Client-facing API
"""

import asyncio

from aiohttp import web

from ..context import Context


# TODO: migrate vfolder operations here


async def download(request: web.Request) -> web.StreamResponse:
    # accept JWT token and reply using web.FileRespnose
    pass


async def upload(request: web.Request) -> web.StreamResponse:
    # accept JWT token and implement tus.io server-side protocol
    pass


async def init_client_app(ctx: Context) -> web.Application:
    app = web.Application()
    app['ctx'] = ctx
    app.router.add_route('POST', '/download', download)
    app.router.add_route('POST', '/upload', upload)
    return app
