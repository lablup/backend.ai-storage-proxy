from contextlib import asynccontextmanager as actxmgr
from datetime import datetime
import logging
import json
from typing import (
    Any,
    Union,
)

from aiohttp import web
import trafaret as t


def fstime2datetime(t: Union[float, int]) -> datetime:
    return datetime.fromtimestamp(t)


@actxmgr
async def check_params(
    request: web.Request,
    checker: t.Trafaret,
    *,
    auth_required: bool = True,
) -> Any:
    body = await request.json()
    try:
        yield checker.check(body)
    except t.DataError as e:
        raise web.HTTPBadRequest(text=json.dumps({
            'type': 'https://api.backend.ai/probs/storage/invalid-api-params',
            'title': 'Invalid API parameters',
            'data': e.as_dict(),
        }), content_type='application/problem+json')
    except NotImplementedError:
        raise web.HTTPBadRequest(text=json.dumps({
            'type': 'https://api.backend.ai/probs/storage/unsupported-operation',
            'title': 'Unsupported operation by the storage backend',
        }), content_type='application/problem+json')


async def log_api_entry(log: logging.Logger, name: str, params: Any) -> None:
    if 'src_vfid' in params and 'dst_vfid' in params:
        log.info(
            "{}(h:{}, f:{} -> dst_f:{})",
            name,
            params['vfhost'],
            params['src_vfid'],
            params['dst_vfid'],
        )
    elif 'relpaths' in params:
        log.info(
            "{}(h:{}, f:{}, p*:{})",
            name,
            params['vfhost'],
            params['vfid'],
            str(params['relpaths'][0]) + '...',
        )
    elif 'relpath' in params:
        log.info(
            "{}(h:{}, f:{}, p:{})",
            name,
            params['vfhost'],
            params['vfid'],
            params['relpath'],
        )
    elif 'vfid' in params:
        log.info(
            "{}(h:{}, f:{})",
            name,
            params['vfhost'],
            params['vfid'],
        )
    elif 'vfhost' in params:
        log.info(
            "{}(h:{})",
            name,
            params['vfhost'],
        )
    else:
        log.info(
            "{}()",
            name,
        )
