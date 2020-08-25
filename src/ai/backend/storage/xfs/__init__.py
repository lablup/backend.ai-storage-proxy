import asyncio
from pathlib import Path, PurePosixPath
import shutil
import os
from typing import (
    FrozenSet
)
from uuid import UUID

from ..vfs import BaseVolume
from ..types import (
    VFolderUsage,
    VFolderCreationOptions
)
from ..exception import ExecutionError


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
        self.registry = {}
        self.project_id_pool = []
        self.uid = uid
        self.gid = gid
        self.loop = loop or asyncio.get_running_loop()

        if os.path.isfile('/etc/projid'):
            raw_projid = await read_file(self.loop, '/etc/projid')
            for line in raw_projid.splitlines():
                proj_name, proj_id = line.split(':')[:2]
                self.project_id_pool.append(int(proj_id))
        else:
            await self.loop.run_in_executor(
                None, lambda: Path('/etc/projid').touch())

        if not os.path.isfile('/etc/projects'):
            await self.loop.run_in_executor(
                None, lambda: Path('/etc/projects').touch())

    # ----- volume opeartions -----
    async def create_vfolder(self, vfid: UUID, options: VFolderCreationOptions = None) -> None:
        if vfid in self.registry.keys():
            return
        if options is None or options['bsize'] == '':
            return

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
        bsize = options['bsize']
        await self.loop.run_in_executor(
            None, lambda: vfpath.mkdir(0o755, parents=True, exist_ok=False))
        await self.loop.run_in_executor(
            None, lambda: os.chown(vfpath, self.uid, self.gid))

        await write_file(self.loop, '/etc/projects', f'{project_id}:{vfpath}\n', perm='a')
        await write_file(self.loop, '/etc/projid', f'{vfid}:{project_id}\n', perm='a')
        await run(f'sudo xfs_quota -x -c "project -s {vfid}" {self.mount_path}')
        await run(f'sudo xfs_quota -x -c "limit -p bhard={bsize} {vfid}" {self.mount_path}')
        self.registry[vfid] = project_id
        self.project_id_pool += [project_id]
        self.project_id_pool.sort()

    async def delete_vfolder(self, vfid: UUID) -> None:
        if vfid not in self.registry.keys():
            return

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

    async def get_quota(self, vfid: UUID) -> int:
        report = await run(f'sudo xfs_quota -x -c report {self.mount_path} | grep {str(vfid)[:-5]}')
        proj_id, _, _, quota, _, _ = report.split()
        if not str(vfid).startswith(proj_id):
            raise ExecutionError('vfid and project id does not match in get_quota')
        return int(quota)

    async def set_quota(self, vfid: UUID, block_size: str) -> None:
        await run(f'sudo xfs_quota -x -c "limit -p bsoft=0 bhard={block_size} {vfid}" {self.mount_path}')

    async def get_usage(self, vfid: UUID, relpath: PurePosixPath = None) -> VFolderUsage:
        pass

