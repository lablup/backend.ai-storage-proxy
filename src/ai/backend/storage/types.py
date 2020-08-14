from datetime import datetime
import enum
from pathlib import Path

import attr


@attr.s(auto_attribs=True, slots=True)
class FSPerfMetric:
    iops_read: int
    iops_write: int
    iobytes_read: int
    iobytes_write: int


@attr.s(auto_attribs=True, slots=True)
class FSUsage:
    capacity_bytes: int
    used_bytes: int


@attr.s(auto_attribs=True, slots=True)
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
