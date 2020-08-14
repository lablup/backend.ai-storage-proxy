from __future__ import annotations

import asyncio
from pathlib import Path, PurePosixPath
import os
from typing import (
    AsyncIterator,
    Optional,
    Sequence,
)
from uuid import UUID

import janus

from ..vfs import BaseVFolderHost
from ..types import (
    FSPerfMetric,
    FSUsage,
    VFolderUsage,
)


class FlashBladeVFolderHost(BaseVFolderHost):

    async def init(self) -> None:
        proc = await asyncio.create_subprocess_exec(
            b'pdu', b'--version',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if b'RapidFile Toolkit' not in stdout or proc.returncode != 0:
            raise RuntimeError(
                "PureStorage RapidFile Toolkit is not installed. "
                "You cannot use the PureStorage backend for the stroage proxy.")

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
            stderr=asyncio.subprocess.PIPE,
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

    def scandir(self, vfid: UUID, relpath: PurePosixPath) -> AsyncIterator[os.DirEntry]:
        # TODO: pls --json <vf-internal-path>
        target_path = self._sanitize_vfpath(vfid, relpath)
        q: janus.Queue[os.DirEntry] = janus.Queue()
        loop = asyncio.get_running_loop()

        def _scandir(q: janus._SyncQueueProxy[os.DirEntry]) -> None:
            with os.scandir(target_path) as scanner:
                for entry in scanner:
                    q.put(entry)

        async def _aiter() -> AsyncIterator[os.DirEntry]:
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
