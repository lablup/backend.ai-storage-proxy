from __future__ import annotations

from datetime import datetime
import enum
from pathlib import Path, PurePath
from typing import (
    Any,
    Final,
    Mapping,
    Optional,
)

import attr
import trafaret as t

from ai.backend.common import validators as tx


class Sentinel(enum.Enum):
    token = 0


SENTINEL: Final = Sentinel.token


@attr.s(auto_attribs=True, slots=True, frozen=True)
class FSPerfMetric:
    iops_read: int
    iops_write: int
    iobytes_read: int
    iobytes_write: int


@attr.s(auto_attribs=True, slots=True, frozen=True)
class FSUsage:
    capacity_bytes: int
    used_bytes: int


@attr.s(auto_attribs=True, slots=True, frozen=True)
class VolumeInfo:
    backend: str
    path: Path
    fsprefix: Optional[PurePath]
    options: Optional[Mapping[str, Any]]

    @classmethod
    def as_trafaret(cls) -> t.Trafaret:
        return t.Dict({
            t.Key('backend'): t.String,
            t.Key('path'): tx.Path(type='dir'),
            t.Key('fsprefix', default='.'): tx.PurePath(relative_only=True),
            t.Key('options', default=None): t.Null | t.Mapping(t.String, t.Any),
        })


@attr.s(auto_attribs=True, slots=True, frozen=True)
class VFolderCreationOptions:
    quota: int

    @classmethod
    def as_trafaret(cls) -> t.Trafaret:
        return t.Dict({
            t.Key('quota', default=0): t.ToInt[0:],
        })


@attr.s(auto_attribs=True, slots=True, frozen=True)
class VFolderUsage:
    file_count: int
    used_bytes: int


@attr.s(auto_attribs=True, slots=True, frozen=True)
class Stat:
    size: int
    owner: str
    mode: int
    modified: datetime
    created: datetime


class DirEntryType(enum.Enum):
    FILE = 0
    DIRECTORY = 1
    SYMLINK = 2


@attr.s(auto_attribs=True, slots=True, frozen=True)
class DirEntry:
    name: str
    path: Path
    type: DirEntryType
    stat: Stat
