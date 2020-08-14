from __future__ import annotations

import asyncio
from pathlib import Path, PurePosixPath
import os
import shutil
from typing import (
    AsyncIterator,
    Optional,
    Sequence,
)
from uuid import UUID

import janus

from ..abc import AbstractVFolderHost
from ..types import (
    FSPerfMetric,
    FSUsage,
    VFolderUsage,
    DirEntry,
    DirEntryType,
    Stat,
)
from ..utils import fstime2datetime


class BaseVFolderHost(AbstractVFolderHost):

    def _mangle_vfpath(self, vfid: UUID) -> Path:
        prefix1 = vfid.hex[0:2]
        prefix2 = vfid.hex[2:4]
        rest = vfid.hex[4:]
        return Path(self.mount_path, prefix1, prefix2, rest)

    def _sanitize_vfpath(self, vfid: UUID, relpath: Optional[PurePosixPath]) -> Path:
        if relpath is None:
            relpath = PurePosixPath('.')
        vfpath = self._mangle_vfpath(vfid)
        target_path = (vfpath / relpath).resolve()
        try:
            target_path.relative_to(vfpath)
        except ValueError:
            raise PermissionError("cannot acess outside of the given vfolder")
        return target_path

    # ------ vfhost operations -------

    async def create_vfolder(self, vfid: UUID) -> None:
        vfpath = self._mangle_vfpath(vfid)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: vfpath.mkdir(0o755, parents=True, exist_ok=False))

    async def delete_vfolder(self, vfid: UUID) -> None:
        vfpath = self._mangle_vfpath(vfid)
        loop = asyncio.get_running_loop()

        def _delete_vfolder():
            shutil.rmtree(vfpath)
            # remove intermediate prefix directories if they become empty
            if not os.listdir(vfpath.parent):
                vfpath.parent.rmdir()
            if not os.listdir(vfpath.parent.parent):
                vfpath.parent.parent.rmdir()

        await loop.run_in_executor(None, _delete_vfolder)

    async def clone_vfolder(self, src_vfid: UUID, new_vfid: UUID) -> None:
        raise NotImplementedError

    async def put_metadata(self, vfid: UUID, payload: bytes) -> None:
        vfpath = self._mangle_vfpath(vfid)
        metadata_path = (vfpath / 'metadata.json')
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, metadata_path.write_bytes, payload)

    async def get_metadata(self, vfid: UUID) -> bytes:
        vfpath = self._mangle_vfpath(vfid)
        metadata_path = (vfpath / 'metadata.json')
        loop = asyncio.get_running_loop()
        try:
            stat = await loop.run_in_executor(None, metadata_path.stat)
            if stat.st_size > 10 * (2 ** 20):
                raise RuntimeError("Too large metadata (more than 10 MiB)")
            data = await loop.run_in_executor(None, metadata_path.read_bytes)
            return data
        except FileNotFoundError:
            return b''
        # Other IO errors should be bubbled up.

    async def get_quota(self, vfid: UUID) -> int:
        raise NotImplementedError

    async def set_quota(self, vfid: UUID, size_bytes: int) -> None:
        raise NotImplementedError

    async def get_performance_metric(self) -> FSPerfMetric:
        raise NotImplementedError

    async def get_fs_usage(self) -> FSUsage:
        loop = asyncio.get_running_loop()
        stat = await loop.run_in_executor(None, os.statvfs, self.mount_path)
        return FSUsage(
            capacity_bytes=stat.f_frsize * stat.f_blocks,
            used_bytes=stat.f_frsize * (stat.f_blocks - stat.f_bavail),
        )

    async def get_usage(self, vfid: UUID, relpath: PurePosixPath = None) -> VFolderUsage:
        target_path = self._sanitize_vfpath(vfid, relpath)
        total_size = 0
        total_count = 0

        def _calc_usage(target_path: os.PathLike) -> None:
            nonlocal total_size, total_count
            with os.scandir(target_path) as scanner:
                for entry in scanner:
                    if entry.is_dir():
                        _calc_usage(entry)
                        continue
                    if entry.is_file() or entry.is_symlink():
                        stat = entry.stat(follow_symlinks=False)
                        total_size += stat.st_size
                        total_count += 1

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _calc_usage, target_path)
        return VFolderUsage(file_count=total_count, used_bytes=total_size)

    # ------ vfolder internal operations -------

    def scandir(self, vfid: UUID, relpath: PurePosixPath) -> AsyncIterator[DirEntry]:
        target_path = self._sanitize_vfpath(vfid, relpath)
        q: janus.Queue[DirEntry] = janus.Queue()
        loop = asyncio.get_running_loop()

        def _scandir(q: janus._SyncQueueProxy[DirEntry]) -> None:
            with os.scandir(target_path) as scanner:
                for entry in scanner:
                    entry_type = DirEntryType.FILE
                    if entry.is_dir():
                        entry_type = DirEntryType.DIRECTORY
                    if entry.is_symlink():
                        entry_type = DirEntryType.SYMLINK
                    q.put(DirEntry(
                        name=entry.name,
                        path=Path(entry.path),
                        type=entry_type,
                        stat=Stat(
                            size=entry.stat().st_size,
                            owner=str(entry.stat().st_uid),
                            mode=entry.stat().st_mode,
                            modified=fstime2datetime(entry.stat().st_mtime),
                            created=fstime2datetime(entry.stat().st_ctime),
                        )
                    ))

        async def _aiter() -> AsyncIterator[DirEntry]:
            scan_task = asyncio.create_task(loop.run_in_executor(None, _scandir, q.sync_q))
            await asyncio.sleep(0)
            try:
                while True:
                    item = await q.async_q.get()
                    yield item
                    q.async_q.task_done()
            finally:
                await scan_task

        return _aiter()

    async def mkdir(self, vfid: UUID, relpath: PurePosixPath, *, parents: bool = False) -> None:
        target_path = self._sanitize_vfpath(vfid, relpath)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: target_path.mkdir(0o755, parents=parents, exist_ok=False),
        )

    async def rmdir(self, vfid: UUID, relpath: PurePosixPath, *, recursive: bool = False) -> None:
        target_path = self._sanitize_vfpath(vfid, relpath)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, target_path.rmdir)

    async def move_file(self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath) -> None:
        raise NotImplementedError

    async def copy_file(self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath) -> None:
        raise NotImplementedError

    async def add_file(self, vfid: UUID, relpath: PurePosixPath, payload: AsyncIterator[bytes]) -> None:
        target_path = self._sanitize_vfpath(vfid, relpath)
        q: janus.Queue[bytes] = janus.Queue()

        def _write(q: janus._SyncQueueProxy[bytes]) -> None:
            with open(target_path, 'wb') as f:
                while True:
                    buf = q.get()
                    try:
                        if not buf:
                            return
                        f.write(buf)
                    finally:
                        q.task_done()

        loop = asyncio.get_running_loop()
        write_task = asyncio.create_task(loop.run_in_executor(None, _write, q.sync_q))
        try:
            async for buf in payload:
                await q.async_q.put(buf)
            await q.async_q.put(b'')
            await q.async_q.join()
        finally:
            await write_task

    def read_file(
        self,
        vfid: UUID,
        relpath: PurePosixPath,
        *,
        chunk_size: int = 0,
    ) -> AsyncIterator[bytes]:
        target_path = self._sanitize_vfpath(vfid, relpath)
        q: janus.Queue[bytes] = janus.Queue()
        loop = asyncio.get_running_loop()

        def _read(q: janus._SyncQueueProxy[bytes]) -> None:
            with open(target_path, 'rb') as f:
                while True:
                    buf = f.read(chunk_size)
                    q.put(buf)
                    if not buf:
                        return

        async def _aiter() -> AsyncIterator[bytes]:
            nonlocal chunk_size
            if chunk_size == 0:
                # get the preferred io block size
                _vfs_stat = await loop.run_in_executor(None, os.statvfs, self.mount_path)
                chunk_size = _vfs_stat.f_bsize
            read_task = asyncio.create_task(loop.run_in_executor(None, _read, q.sync_q))
            await asyncio.sleep(0)
            try:
                while True:
                    buf = await q.async_q.get()
                    yield buf
                    q.async_q.task_done()
            finally:
                await read_task

        return _aiter()

    async def delete_files(self, vfid: UUID, relpaths: Sequence[PurePosixPath]) -> None:
        target_paths = [self._sanitize_vfpath(vfid, p) for p in relpaths]

        def _delete() -> None:
            for p in target_paths:
                p.unlink()

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _delete)
