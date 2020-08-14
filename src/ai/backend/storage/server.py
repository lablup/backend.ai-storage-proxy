import asyncio
from ipaddress import _BaseAddress as BaseIPAddress
import logging
import os
from pathlib import Path
from pprint import pformat, pprint
from setproctitle import setproctitle
import ssl
import sys
from typing import (
    Any,
    AsyncIterator,
    Sequence,
)

from aiohttp import web
import aiotools
import click
import trafaret as t

from ai.backend.common import config
from ai.backend.common.etcd import AsyncEtcd, ConfigScopes
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common import validators as tx

from . import __version__ as VERSION
from .context import Context
from .api.client import init_client_app
from .api.manager import init_manager_app

log = BraceStyleAdapter(logging.getLogger('ai.backend.storage.server'))


@aiotools.server
async def server_main(
    loop: asyncio.AbstractEventLoop,
    pidx: int,
    _args: Sequence[Any],
) -> AsyncIterator[None]:
    config = _args[0]

    etcd_credentials = None
    if config['etcd']['user']:
        etcd_credentials = {
            'user': config['etcd']['user'],
            'password': config['etcd']['password'],
        }
    scope_prefix_map = {
        ConfigScopes.GLOBAL: "",
        ConfigScopes.NODE: f"nodes/storage/{config['node_id']}",
    }
    etcd = AsyncEtcd(config['etcd']['addr'],
                     config['etcd']['namespace'],
                     scope_prefix_map,
                     credentials=etcd_credentials)
    ctx = Context(pid=os.getpid(), etcd=etcd)
    client_api_app = init_client_app(ctx)
    manager_api_app = init_manager_app(ctx)

    ssl_ctx = None
    if config['api']['client']['ssl-enabled']:
        client_ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        client_ssl_ctx.load_cert_chain(
            str(config['api']['client']['ssl-cert']),
            str(config['api']['client']['ssl-privkey']),
        )
    if config['api']['manager']['ssl-enabled']:
        manager_ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        manager_ssl_ctx.load_cert_chain(
            str(config['api']['manager']['ssl-cert']),
            str(config['api']['manager']['ssl-privkey']),
        )
    client_api_runner = web.AppRunner(client_api_app)
    manager_api_runner = web.AppRunner(manager_api_app)
    await client_api_runner.setup()
    await manager_api_runner.setup()
    client_service_addr = config['api']['client']['service-addr']
    manager_service_addr = config['api']['manager']['service-addr']
    client_api_site = web.TCPSite(
        client_api_runner,
        str(client_service_addr.host),
        client_service_addr.port,
        backlog=1024,
        reuse_port=True,
        ssl_context=ssl_ctx,
    )
    manager_api_site = web.TCPSite(
        manager_api_runner,
        str(manager_service_addr.host),
        manager_service_addr.port,
        backlog=1024,
        reuse_port=True,
        ssl_context=ssl_ctx,
    )
    await client_api_site.start()
    await manager_api_site.start()
    try:
        yield
    finally:
        log.info('Shutting down...')
        await manager_api_runner.cleanup()
        await client_api_runner.cleanup()


@click.group(invoke_without_command=True)
@click.option('-f', '--config-path', '--config', type=Path, default=None,
              help='The config file path. (default: ./storage.toml and /etc/backend.ai/storage.toml)')
@click.option('--debug', is_flag=True,
              help='Enable the debug mode and override the global log level to DEBUG.')
@click.pass_context
def main(cli_ctx, config_path, debug):
    storage_config_iv = t.Dict({
        t.Key('etcd'): t.Dict({
            t.Key('namespace'): t.String,
            t.Key('addr'): tx.HostPortPair(allow_blank_host=False)
            # TODO: password
        }).allow_extra('*'),
        t.Key('logging'): t.Any,  # checked in ai.backend.common.logging
        t.Key('api'): t.Dict({
            t.Key('client'): t.Dict({
                t.Key('service-addr'): tx.HostPortPair(allow_blank_host=True),
                t.Key('ssl-enabled'): t.ToBool,
                t.Key('ssl-cert'): tx.Path,
                t.Key('ssl-privkey'): tx.Path,
            }),
            t.Key('manager'): t.Dict({
                t.Key('service-addr'): tx.HostPortPair(allow_blank_host=True),
                t.Key('ssl-enabled'): t.ToBool,
                t.Key('ssl-cert'): tx.Path,
                t.Key('ssl-privkey'): tx.Path,
            }),
        }),
        t.Key('storage'): t.List(t.Dict({
            t.Key('mode'): t.Enum('xfs', 'btrfs'),
            t.Key('path'): t.String,
            t.Key('user-uid'): t.Int,
            t.Key('user-gid'): t.Int,
        })),
    }).allow_extra('*')

    # Determine where to read configuration.
    raw_cfg, cfg_src_path = config.read_from_file(config_path, 'storage')

    config.override_with_env(raw_cfg, ('etcd', 'namespace'), 'BACKEND_NAMESPACE')
    config.override_with_env(raw_cfg, ('etcd', 'addr'), 'BACKEND_ETCD_ADDR')
    config.override_with_env(raw_cfg, ('etcd', 'user'), 'BACKEND_ETCD_USER')
    config.override_with_env(raw_cfg, ('etcd', 'password'), 'BACKEND_ETCD_PASSWORD')
    config.override_with_env(raw_cfg, ('agent', 'rpc-listen-addr', 'host'),
                             'BACKEND_AGENT_HOST_OVERRIDE')
    config.override_with_env(raw_cfg, ('agent', 'rpc-listen-addr', 'port'),
                             'BACKEND_AGENT_PORT')

    if debug:
        config.override_key(raw_cfg, ('debug', 'enabled'), True)
        config.override_key(raw_cfg, ('logging', 'level'), 'DEBUG')
        config.override_key(raw_cfg, ('logging', 'pkg-ns', 'ai.backend'), 'DEBUG')

    try:
        cfg = config.check(raw_cfg, storage_config_iv)
        cfg['_src'] = cfg_src_path
    except config.ConfigurationError as e:
        print('ConfigurationError: Validation of agent configuration has failed:', file=sys.stderr)
        print(pformat(e.invalid_data), file=sys.stderr)
        raise click.Abort()

    rpc_host = cfg['agent']['rpc-listen-addr'].host
    if (isinstance(rpc_host, BaseIPAddress) and
        (rpc_host.is_unspecified or rpc_host.is_link_local)):
        print('ConfigurationError: '
              'Cannot use link-local or unspecified IP address as the RPC listening host.',
              file=sys.stderr)
        raise click.Abort()

    if os.getuid() != 0:
        print('Storage agent can only be run as root', file=sys.stderr)
        raise click.Abort()

    if cli_ctx.invoked_subcommand is None:
        setproctitle('backend.ai: storage-proxy')
        logger = Logger(cfg['logging'])
        with logger:
            log.info('Backend.AI Storage Proxy', VERSION)

            log_config = logging.getLogger('ai.backend.agent.config')
            if debug:
                log_config.debug('debug mode enabled.')

            if 'debug' in cfg and cfg['debug']['enabled']:
                print('== Storage proxy configuration ==')
                pprint(cfg)

            aiotools.start_server(server_main, num_workers=1,
                                    use_threading=True, args=(cfg, ))
            log.info('exit.')
    return 0


if __name__ == "__main__":
    sys.exit(main())
