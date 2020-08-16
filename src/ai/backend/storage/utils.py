from contextlib import asynccontextmanager as actxmgr
from datetime import datetime
import logging
import json
from typing import (
    Any,
    Optional,
    Union,
)

from aiohttp import web
import trafaret as t

from ai.backend.common.logging import BraceStyleAdapter


def fstime2datetime(t: Union[float, int]) -> datetime:
    return datetime.fromtimestamp(t)


@actxmgr
async def check_params(
    request: web.Request,
    checker: Optional[t.Trafaret],
    *,
    auth_required: bool = True,
) -> Any:
    if checker is None:
        if request.has_body:
            raise web.HTTPBadRequest(text=json.dumps({
                'type': 'https://api.backend.ai/probs/storage/malformed-request',
                'title': 'Malformed request (request body should be empty)',
            }), content_type='application/problem+json')
    else:
        body = await request.json()
    try:
        if checker is None:
            yield None
        else:
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


async def log_api_entry(
    log: Union[logging.Logger, BraceStyleAdapter],
    name: str,
    params: Any,
) -> None:
    if params is not None:
        if 'src_vfid' in params and 'dst_vfid' in params:
            log.info(
                "ManagerAPI::{}(h:{}, f:{} -> dst_f:{})",
                name.upper(),
                params['volume'],
                params['src_vfid'],
                params['dst_vfid'],
            )
        elif 'relpaths' in params:
            log.info(
                "ManagerAPI::{}(h:{}, f:{}, p*:{})",
                name.upper(),
                params['volume'],
                params['vfid'],
                str(params['relpaths'][0]) + '...',
            )
        elif 'relpath' in params:
            log.info(
                "ManagerAPI::{}(h:{}, f:{}, p:{})",
                name.upper(),
                params['volume'],
                params['vfid'],
                params['relpath'],
            )
        elif 'vfid' in params:
            log.info(
                "ManagerAPI::{}(h:{}, f:{})",
                name.upper(),
                params['volume'],
                params['vfid'],
            )
        elif 'volume' in params:
            log.info(
                "ManagerAPI::{}(h:{})",
                name.upper(),
                params['volume'],
            )
        return
    log.info(
        "ManagerAPI::{}()",
        name.upper(),
    )
