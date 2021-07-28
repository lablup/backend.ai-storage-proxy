from __future__ import annotations

import asyncio
import os
import secrets
import shutil
from pathlib import Path, PurePosixPath
from typing import AsyncIterator, Sequence, Union
from uuid import UUID

import janus

from ai.backend.common.types import BinarySize

from ..exception import ExecutionError, InvalidAPIParameters
from ..types import (
    SENTINEL,
    DirEntry,
    DirEntryType,
    Sentinel,
    Stat,
    VFolderCreationOptions,
    VFolderUsage,
)
from ..utils import fstime2datetime
from ..vfs import BaseVolume


async def read_file(loop: asyncio.AbstractEventLoop, filename: str) -> str:
    def _read():
        with open(filename, "r") as fr:
            return fr.read()

    return await loop.run_in_executor(None, lambda: _read())


async def write_file(
    loop: asyncio.AbstractEventLoop, filename: str, contents: str, perm="w"
):
    def _write():
        with open(filename, perm) as fw:
            fw.write(contents)

    await loop.run_in_executor(None, lambda: _write())


async def run(cmd: str) -> str:
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await proc.communicate()
    if err:
        raise ExecutionError(err.decode())
    return out.decode()


class NetAppVolume(BaseVolume):
    async def init(self) -> None:

        available = True
        try:
            proc = await asyncio.create_subprocess_exec(
                b"nfsstat",
                b"-m",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError:
            available = False
        else:
            try:
                stdout, stderr = await proc.communicate()
                if b"NFS parameters" not in stdout or proc.returncode != 0:
                    available = False
            finally:
                await proc.wait()
        print(available, stdout, stderr)
        if not available:
            raise RuntimeError("NetApp volume is not available or not mounted.")

    async def create_vfolder(
        self, vfid: UUID, options: VFolderCreationOptions = None
    ) -> None:
        vfpath = self.mangle_vfpath(vfid)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: vfpath.mkdir(0o755, parents=True, exist_ok=False)
        )

    async def delete_vfolder(self, vfid: UUID) -> None:
        vfpath = self.mangle_vfpath(vfid)
        loop = asyncio.get_running_loop()

        def _delete_vfolder():
            shutil.rmtree(vfpath)
            # remove intermediate prefix directories if they become empty
            if not os.listdir(vfpath.parent):
                vfpath.parent.rmdir()
            if not os.listdir(vfpath.parent.parent):
                vfpath.parent.parent.rmdir()

        await loop.run_in_executor(None, _delete_vfolder)

    async def get_usage(
        self, vfid: UUID, relpath: PurePosixPath = None
    ) -> VFolderUsage:
        target_path = self.sanitize_vfpath(vfid, relpath)
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

    async def get_used_bytes(self, vfid: UUID) -> BinarySize:
        vfpath = self.mangle_vfpath(vfid)
        info = await run(f"du -hs {vfpath}")
        used_bytes, _ = info.split()
        return BinarySize.finite_from_str(used_bytes)

    # ------ vfolder internal operations -------

    def scandir(self, vfid: UUID, relpath: PurePosixPath) -> AsyncIterator[DirEntry]:
        target_path = self.sanitize_vfpath(vfid, relpath)
        q: janus.Queue[Union[Sentinel, DirEntry]] = janus.Queue()
        loop = asyncio.get_running_loop()

        def _scandir(q: janus._SyncQueueProxy[Union[Sentinel, DirEntry]]) -> None:
            count = 0
            limit = self.local_config["storage-proxy"]["scandir-limit"]
            try:
                with os.scandir(target_path) as scanner:
                    for entry in scanner:
                        symlink_target = ""
                        entry_type = DirEntryType.FILE
                        if entry.is_dir():
                            entry_type = DirEntryType.DIRECTORY
                        if entry.is_symlink():
                            entry_type = DirEntryType.SYMLINK
                            symlink_target = str(Path(entry).resolve())
                        entry_stat = entry.stat(follow_symlinks=False)
                        q.put(
                            DirEntry(
                                name=entry.name,
                                path=Path(entry.path),
                                type=entry_type,
                                stat=Stat(
                                    size=entry_stat.st_size,
                                    owner=str(entry_stat.st_uid),
                                    mode=entry_stat.st_mode,
                                    modified=fstime2datetime(entry_stat.st_mtime),
                                    created=fstime2datetime(entry_stat.st_ctime),
                                ),
                                symlink_target=symlink_target,
                            )
                        )
                        count += 1
                        if limit > 0 and count == limit:
                            break
            finally:
                q.put(SENTINEL)

        async def _scan_task(_scandir, q) -> None:
            await loop.run_in_executor(None, _scandir, q.sync_q)

        async def _aiter() -> AsyncIterator[DirEntry]:
            scan_task = asyncio.create_task(_scan_task(_scandir, q))
            await asyncio.sleep(0)
            try:
                while True:
                    item = await q.async_q.get()
                    if item is SENTINEL:
                        break
                    yield item
                    q.async_q.task_done()
            finally:
                await scan_task
                q.close()
                await q.wait_closed()

        return _aiter()

    async def mkdir(
        self,
        vfid: UUID,
        relpath: PurePosixPath,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        target_path = self.sanitize_vfpath(vfid, relpath)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: target_path.mkdir(0o755, parents=parents, exist_ok=exist_ok),
        )

    async def rmdir(
        self, vfid: UUID, relpath: PurePosixPath, *, recursive: bool = False
    ) -> None:
        target_path = self.sanitize_vfpath(vfid, relpath)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, target_path.rmdir)

    async def move_file(
        self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath
    ) -> None:
        src_path = self.sanitize_vfpath(vfid, src)
        if not src_path.is_file():
            raise InvalidAPIParameters(msg=f"source path {str(src_path)} is not a file")
        dst_path = self.sanitize_vfpath(vfid, dst)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: dst_path.parent.mkdir(parents=True, exist_ok=True)
        )
        await loop.run_in_executor(None, src_path.rename, dst_path)

    async def move_tree(
        self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath
    ) -> None:
        src_path = self.sanitize_vfpath(vfid, src)
        if not src_path.is_dir():
            raise InvalidAPIParameters(
                msg=f"source path {str(src_path)} is not a directory"
            )
        dst_path = self.sanitize_vfpath(vfid, dst)
        src_path = self.sanitize_vfpath(vfid, src)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: shutil.move(str(src_path), str(dst_path))
        )

    async def copy_file(
        self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath
    ) -> None:
        src_path = self.sanitize_vfpath(vfid, src)
        if not src_path.is_file():
            raise InvalidAPIParameters(msg=f"source path {str(src_path)} is not a file")
        dst_path = self.sanitize_vfpath(vfid, dst)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: dst_path.parent.mkdir(parents=True, exist_ok=True)
        )
        await loop.run_in_executor(
            None, lambda: shutil.copyfile(str(src_path), str(dst_path))
        )

    async def prepare_upload(self, vfid: UUID) -> str:
        vfpath = self.mangle_vfpath(vfid)
        session_id = secrets.token_hex(16)

        def _create_target():
            upload_base_path = vfpath / ".upload"
            upload_base_path.mkdir(exist_ok=True)
            upload_target_path = upload_base_path / session_id
            upload_target_path.touch()

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _create_target)
        return session_id

    async def add_file(
        self, vfid: UUID, relpath: PurePosixPath, payload: AsyncIterator[bytes]
    ) -> None:
        target_path = self.sanitize_vfpath(vfid, relpath)
        q: janus.Queue[bytes] = janus.Queue()

        def _write(q: janus._SyncQueueProxy[bytes]) -> None:
            with open(target_path, "wb") as f:
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
            await q.async_q.put(b"")
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
        target_path = self.sanitize_vfpath(vfid, relpath)
        q: janus.Queue[Union[bytes, Exception]] = janus.Queue()
        loop = asyncio.get_running_loop()

        def _read(
            q: janus._SyncQueueProxy[Union[bytes, Exception]],
            chunk_size: int,
        ) -> None:
            try:
                with open(target_path, "rb") as f:
                    while True:
                        buf = f.read(chunk_size)
                        if not buf:
                            return
                        q.put(buf)
            except Exception as e:
                q.put(e)
            finally:
                q.put(b"")

        async def _aiter() -> AsyncIterator[bytes]:
            nonlocal chunk_size
            if chunk_size == 0:
                # get the preferred io block size
                _vfs_stat = await loop.run_in_executor(
                    None,
                    os.statvfs,
                    self.mount_path,
                )
                chunk_size = _vfs_stat.f_bsize
            read_fut = loop.run_in_executor(None, _read, q.sync_q, chunk_size)
            await asyncio.sleep(0)
            try:
                while True:
                    buf = await q.async_q.get()
                    if isinstance(buf, Exception):
                        raise buf
                    yield buf
                    q.async_q.task_done()
                    if not buf:
                        return
            finally:
                await read_fut

        return _aiter()

    async def delete_files(
        self, vfid: UUID, relpaths: Sequence[PurePosixPath], recursive: bool = False
    ) -> None:
        target_paths = [self.sanitize_vfpath(vfid, p) for p in relpaths]

        def _delete() -> None:
            for p in target_paths:
                if p.is_dir() and recursive:
                    shutil.rmtree(p)
                else:
                    p.unlink()

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _delete)
