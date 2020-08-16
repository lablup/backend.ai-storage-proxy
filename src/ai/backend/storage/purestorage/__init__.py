from __future__ import annotations

import asyncio
import json
from pathlib import Path, PurePosixPath
from typing import (
    AsyncIterator,
    FrozenSet,
    Sequence,
)
from uuid import UUID

from ..abc import CAP_VFOLDER, CAP_METRIC, CAP_FAST_SCAN
from ..vfs import BaseVolume
from ..types import (
    FSPerfMetric,
    VFolderUsage,
    DirEntry,
    DirEntryType,
    Stat,
)
from ..utils import fstime2datetime


class FlashBladeVolume(BaseVolume):

    async def init(self) -> None:
        available = True
        try:
            proc = await asyncio.create_subprocess_exec(
                b'pdu', b'--version',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError:
            available = False
        else:
            try:
                stdout, stderr = await proc.communicate()
                if b'RapidFile Toolkit' not in stdout or proc.returncode != 0:
                    available = False
            finally:
                await proc.wait()
        if not available:
            raise RuntimeError(
                "PureStorage RapidFile Toolkit is not installed. "
                "You cannot use the PureStorage backend for the stroage proxy.")

    async def get_capabilities(self) -> FrozenSet[str]:
        return frozenset([
            CAP_VFOLDER,
            CAP_METRIC,
            CAP_FAST_SCAN,
        ])

    async def clone_vfolder(self, src_vfid: UUID, new_vfid: UUID) -> None:
        # TODO: pcp -r -p <src_vfpath>/. <new_vfpath>
        raise NotImplementedError

    async def get_quota(self, vfid: UUID) -> int:
        raise NotImplementedError

    async def set_quota(self, vfid: UUID, size_bytes: int) -> None:
        raise NotImplementedError

    async def get_performance_metric(self) -> FSPerfMetric:
        # TODO: use FlashBlade API
        raise NotImplementedError

    async def get_usage(self, vfid: UUID, relpath: PurePosixPath = None) -> VFolderUsage:
        target_path = self._sanitize_vfpath(vfid, relpath)
        total_size = 0
        total_count = 0
        raw_target_path = bytes(target_path)
        # Measure the exact file sizes and bytes
        proc = await asyncio.create_subprocess_exec(
            b'pdu', b'-0', b'-b', b'-a', b'-s', raw_target_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert proc.stdout is not None
        try:
            # TODO: check slowdowns when there are millions of files
            while True:
                try:
                    line = await proc.stdout.readuntil(b'\0')
                    line = line.rstrip(b'\0')
                except asyncio.IncompleteReadError:
                    break
                size, name = line.split(maxsplit=1)
                if len(name) != len(raw_target_path) and name != raw_target_path:
                    total_size += int(size)
                    total_count += 1
        finally:
            await proc.wait()
        return VFolderUsage(file_count=total_count, used_bytes=total_size)

    # ------ vfolder internal operations -------

    def scandir(self, vfid: UUID, relpath: PurePosixPath) -> AsyncIterator[DirEntry]:
        target_path = self._sanitize_vfpath(vfid, relpath)
        raw_target_path = bytes(target_path)

        async def _aiter() -> AsyncIterator[DirEntry]:
            proc = await asyncio.create_subprocess_exec(
                b'pls', b'--json', raw_target_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            assert proc.stdout is not None
            try:
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    line = line.rstrip(b'\n')
                    item = json.loads(line)
                    item_path = Path(item['path'])
                    entry_type = DirEntryType.FILE
                    if item['filetype'] == 40000:
                        entry_type = DirEntryType.DIRECTORY
                    if item['filetype'] == 120000:
                        entry_type = DirEntryType.SYMLINK
                    yield DirEntry(
                        name=item_path.name,
                        path=item_path,
                        type=entry_type,
                        stat=Stat(
                            size=item['size'],
                            owner=str(item['uid']),
                            # The integer represents the octal number in decimal
                            # (e.g., 644 which actually means 0o644)
                            mode=int(str(item['mode']), 8),
                            modified=fstime2datetime(item['mtime']),
                            created=fstime2datetime(item['ctime']),
                        ),
                    )
            finally:
                await proc.wait()

        return _aiter()

    async def move_file(self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath) -> None:
        raise NotImplementedError

    async def copy_file(self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath) -> None:
        # TODO: pcp ...
        raise NotImplementedError

    async def delete_files(self, vfid: UUID, relpaths: Sequence[PurePosixPath]) -> None:
        target_paths = [bytes(self._sanitize_vfpath(vfid, p)) for p in relpaths]
        proc = await asyncio.create_subprocess_exec(
            b'prm', *target_paths,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError("'prm' command returned a non-zero exit code.")
