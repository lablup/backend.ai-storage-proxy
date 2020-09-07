import asyncio
from pathlib import Path, PurePosixPath
import shutil
import os
from typing import (
    Mapping,
    List,
    AsyncIterator
)
from uuid import UUID

from ..vfs import BaseVolume
from ..types import (
    DirEntry,
    VFolderUsage,
    VFolderCreationOptions
)
from ..exception import (
    ExecutionError,
    VFolderCreationError,
    VFolderNotFoundError,
    InvalidAPIParameters
)
from ai.backend.common.types import BinarySize


async def read_file(loop: asyncio.BaseEventLoop, filename: str) -> str:
    def _read():
        with open(filename, 'r') as fr:
            return fr.read()
    return await loop.run_in_executor(None, lambda: _read())


async def write_file(loop: asyncio.BaseEventLoop, filename: str, contents: str, perm='w'):
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


class XfsVolume(BaseVolume):
    loop: asyncio.BaseEventLoop

    async def init(self, uid, gid, loop=None) -> None:
        self.registry: Mapping[UUID, int] = {}
        self.project_id_pool: List[int] = []
        self.uid = uid
        self.gid = gid
        self.loop = loop or asyncio.get_running_loop()

        if os.path.isfile('/etc/projid'):
            raw_projid = await read_file(self.loop, '/etc/projid')
            for line in raw_projid.splitlines():
                proj_name, proj_id = line.split(':')[:2]
                self.project_id_pool.append(int(proj_id))
                self.registry[proj_name] = UUID(proj_name)
        else:
            await self.loop.run_in_executor(
                None, lambda: Path('/etc/projid').touch())

        if not os.path.isfile('/etc/projects'):
            await self.loop.run_in_executor(
                None, lambda: Path('/etc/projects').touch())

    # ----- volume opeartions -----
    async def create_vfolder(self, vfid: UUID, options: VFolderCreationOptions = None) -> None:
        if vfid in self.registry.keys():
            raise VFolderCreationError('VFolder id {} already exists'.format(str(vfid)))
        if options is None or options['quota'] == 0:
            raise VFolderCreationError('VFolder quota must be specified')

        project_id = -1
        # set project_id to the smallest integer not being used
        for i in range(len(self.project_id_pool) - 1):
            if self.project_id_pool[i] + 1 != self.project_id_pool[i + 1]:
                project_id = self.project_id_pool[i] + 1
                break
        if len(self.project_id_pool) == 0:
            project_id = 1
        if project_id == -1:
            project_id = self.project_id_pool[-1] + 1

        vfpath = self.mangle_vfpath(vfid)
        quota = options['quota']
        await self.loop.run_in_executor(
            None, lambda: vfpath.mkdir(0o755, parents=True, exist_ok=False))
        await self.loop.run_in_executor(
            None, lambda: os.chown(vfpath, self.uid, self.gid))

        await write_file(self.loop, '/etc/projects', f'{project_id}:{vfpath}\n', perm='a')
        await write_file(self.loop, '/etc/projid', f'{vfid}:{project_id}\n', perm='a')
        await run(f'sudo xfs_quota -x -c "project -s {vfid}" {self.mount_path}')
        await run(f'sudo xfs_quota -x -c "limit -p bhard={quota} {vfid}" {self.mount_path}')
        self.registry[vfid] = project_id
        self.project_id_pool += [project_id]
        self.project_id_pool.sort()

    async def delete_vfolder(self, vfid: UUID) -> None:
        if vfid not in self.registry.keys():
            raise VFolderNotFoundError('VFolder with id {} does not exist'.format(vfid))

        await run(f'sudo xfs_quota -x -c "limit -p bsoft=0 bhard=0 {vfid}" {self.mount_path}')

        raw_projects = await read_file(self.loop, '/etc/projects')
        raw_projid = await read_file(self.loop, '/etc/projid')
        new_projects = ''
        new_projid = ''
        for line in raw_projects.splitlines():
            if line.startswith(str(self.registry[vfid]) + ':'):
                continue
            new_projects += (line + '\n')
        for line in raw_projid.splitlines():
            if line.startswith(str(vfid) + ':'):
                continue
            new_projid += (line + '\n')
        await write_file(self.loop, '/etc/projects', new_projects)
        await write_file(self.loop, '/etc/projid', new_projid)

        vfpath = self.mangle_vfpath(vfid)

        def _delete_vfolder():
            shutil.rmtree(vfpath)
            if not os.listdir(vfpath.parent):
                vfpath.parent.rmdir()
            if not os.listdir(vfpath.parent.parent):
                vfpath.parent.parent.rmdir()

        await self.loop.run_in_executor(
            None, lambda: _delete_vfolder())
        self.project_id_pool.remove(self.registry[vfid])
        del self.registry[vfid]

    async def clone_vfolder(self, src_vfid: UUID, new_vifd: UUID) -> None:
        raise NotImplementedError

    async def get_quota(self, vfid: UUID) -> BinarySize:
        report = await run(f'sudo xfs_quota -x -c \'report -h\' {self.mount_path} | grep {str(vfid)[:-5]}')
        proj_name, _, _, quota, _, _ = report.split()
        if not str(vfid).startswith(proj_name):
            raise ExecutionError('vfid and project name does not match')
        return BinarySize.from_str(quota)

    async def set_quota(self, vfid: UUID, size_bytes: BinarySize) -> None:
        await run(f'sudo xfs_quota -x -c "limit -p bsoft=0 bhard={int(size_bytes)} {vfid}" {self.mount_path}')

    async def get_usage(self, vfid: UUID, relpath: PurePosixPath = None) -> VFolderUsage:
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
        info = await run(f'du -hs {vfpath}')
        used_bytes, _ = info.split()
        return BinarySize.from_str(used_bytes)

    # ----- vfolder internal operations -----

    def scandir(self, vfid: UUID, relpath: PurePosixPath) -> AsyncIterator[DirEntry]:
        raise NotImplementedError

    async def mkdir(self, vfid: UUID, relpath: PurePosixPath, *, parents: bool = False) -> None:
        target_path = self.sanitize_vfpath(vfid, relpath)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: target_path.mkdir(0o755, parents=parents, exist_ok=False))

    async def rmdir(self, vfid: UUID, relpath: PurePosixPath, *, recursive: bool = False) -> None:
        target_path = self.sanitize_vfpath(vfid, relpath)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, target_path.rmdir)

    async def move_file(self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath) -> None:
        src_path = self.sanitize_vfpath(vfid, src)
        if not src_path.is_file():
            raise InvalidAPIParameters(msg=f'source path {str(src_path)} is not a file')
        dst_path = self.sanitize_vfpath(vfid, dst)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: dst_path.parent.mkdir(parents=True, exist_ok=True))
        await loop.run_in_executor(None, lambda: src_path.rename(dst_path))

    async def copy_file(self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath) -> None:
        src_path = self.sanitize_vfpath(vfid, src)
        if not src_path.is_file():
            raise InvalidAPIParameters(msg=f'source path {str(src_path)} is not a file')
        dst_path = self.sanitize_vfpath(vfid, dst)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: dst_path.parent.mkdir(parents=True, exist_ok=True))
        await loop.run_in_executor(None, lambda: shutil.copyfile(str(src_path), str(dst_path)))

