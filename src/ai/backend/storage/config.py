import os

import trafaret as t

from ai.backend.common import validators as tx
from ai.backend.common.config import etcd_config_iv
from ai.backend.common.logging import logging_config_iv

_max_cpu_count = os.cpu_count()


local_config_iv = t.Dict({
    t.Key('storage-proxy'): t.Dict({
        t.Key('node-id'): t.String,
        t.Key('num-proc', default=_max_cpu_count): t.Int[1:_max_cpu_count],
        t.Key('pid-file', default=os.devnull): tx.Path(type='file',
                                                       allow_nonexisting=True,
                                                       allow_devnull=True),
        t.Key('event-loop', default='asyncio'): t.Enum('asyncio', 'uvloop'),
    }),
    t.Key('logging'): logging_config_iv,
    t.Key('api'): t.Dict({
        t.Key('client'): t.Dict({
            t.Key('service-addr'): tx.HostPortPair(allow_blank_host=True),
            t.Key('ssl-enabled'): t.ToBool,
            t.Key('ssl-cert', default=None): t.Null | tx.Path(type='file'),
            t.Key('ssl-privkey', default=None): t.Null | tx.Path(type='file'),
        }),
        t.Key('manager'): t.Dict({
            t.Key('service-addr'): tx.HostPortPair(allow_blank_host=True),
            t.Key('ssl-enabled'): t.ToBool,
            t.Key('ssl-cert', default=None): t.Null | tx.Path(type='file'),
            t.Key('ssl-privkey', default=None): t.Null | tx.Path(type='file'),
        }),
    }),
    t.Key('storage'): t.Mapping(
        t.String,  # storage name
        t.Dict({
            t.Key('backend'): t.String,
            t.Key('path'): t.String,
            t.Key('options', default=None): t.Null | t.Mapping(t.String, t.Any),
        }),
    ),
    t.Key('debug'): t.Dict({
        t.Key('enabled', default=False): t.ToBool,
    }).allow_extra('*'),
}).merge(etcd_config_iv).allow_extra('*')