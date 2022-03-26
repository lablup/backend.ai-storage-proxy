import os
from pathlib import Path

import trafaret as t

from ai.backend.common import validators as tx
from ai.backend.common.config import etcd_config_iv
from ai.backend.common.logging import logging_config_iv

from .types import VolumeInfo

_max_cpu_count = os.cpu_count()
_file_perm = (Path(__file__).parent / "server.py").stat()


local_config_iv = (
    t.Dict(
        {
            t.Key("storage-proxy"): t.Dict(
                {
                    t.Key("node-id"): t.String,
                    t.Key("num-proc", default=_max_cpu_count): t.Int[1:_max_cpu_count],
                    t.Key("pid-file", default=os.devnull): tx.Path(
                        type="file",
                        allow_nonexisting=True,
                        allow_devnull=True,
                    ),
                    t.Key("event-loop", default="asyncio"): t.Enum("asyncio", "uvloop"),
                    t.Key("scandir-limit", default=1000): t.Int[0:],
                    t.Key("max-upload-size", default="100g"): tx.BinarySize,
                    t.Key("secret"): t.String,  # used to generate JWT tokens
                    t.Key("session-expire"): tx.TimeDuration,
                    t.Key("user", default=None): tx.UserID(
                        default_uid=_file_perm.st_uid,
                    ),
                    t.Key("group", default=None): tx.GroupID(
                        default_gid=_file_perm.st_gid,
                    ),
                },
            ),
            t.Key("filebrowser"): t.Dict(
                {
                    t.Key("image"): t.String,
                    t.Key("service-ip"): t.IP,
                    t.Key("max-cpu", default=1): t.Int[1:_max_cpu_count],
                    t.Key("max-mem", default="1g"): tx.BinarySize,
                    t.Key("max-containers", default=32): t.Int[1:],
                    t.Key("user", default=None): tx.UserID(
                        default_uid=_file_perm.st_uid,
                    ),
                    t.Key("group", default=None): tx.GroupID(
                        default_gid=_file_perm.st_gid,
                    ),
                    t.Key("settings_path", default=None): tx.Path(type="dir"),
                    t.Key("service_port", default=None): t.Int,
                    t.Key("mount_path", default=None): tx.Path(type="dir"),
                    t.Key("max-containers", default=None): t.Int,
                    t.Key("db-path", default=None): tx.Path(
                        type="file",
                        allow_nonexisting=True,
                        allow_devnull=True,
                    ),
                    t.Key("period", default=30): t.Int,
                    t.Key("freq", default=1): t.Int,
                    t.Key("idle_timeout", default=300): t.Int,
                },
            ),
            t.Key("logging"): logging_config_iv,
            t.Key("api"): t.Dict(
                {
                    t.Key("client"): t.Dict(
                        {
                            t.Key("service-addr"): tx.HostPortPair(
                                allow_blank_host=True,
                            ),
                            t.Key("ssl-enabled"): t.ToBool,
                            t.Key("ssl-cert", default=None): t.Null
                            | tx.Path(type="file"),
                            t.Key("ssl-privkey", default=None): t.Null
                            | tx.Path(type="file"),
                        },
                    ),
                    t.Key("manager"): t.Dict(
                        {
                            t.Key("service-addr"): tx.HostPortPair(
                                allow_blank_host=True,
                            ),
                            t.Key("ssl-enabled"): t.ToBool,
                            t.Key("ssl-cert", default=None): t.Null
                            | tx.Path(type="file"),
                            t.Key("ssl-privkey", default=None): t.Null
                            | tx.Path(type="file"),
                            t.Key("secret"): t.String,  # used to authenticate managers
                        },
                    ),
                },
            ),
            t.Key("volume"): t.Mapping(
                t.String,
                VolumeInfo.as_trafaret(),  # volume name -> details
            ),
            t.Key("debug"): t.Dict(
                {
                    t.Key("enabled", default=False): t.ToBool,
                },
            ).allow_extra("*"),
        },
    )
    .merge(etcd_config_iv)
    .allow_extra("*")
)
