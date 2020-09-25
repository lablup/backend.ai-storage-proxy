import asyncio
import logging
from pathlib import Path
import shutil
import os

from ai.backend.common.logging import BraceStyleAdapter

from ..server import AbstractVolumeAgent, run

log = BraceStyleAdapter(logging.getLogger('ai.backend.storage.server'))


async def read_file(loop: asyncio.BaseEventLoop, filename: str) -> str:
    content = ''

    def _read():
        nonlocal content
        with open(filename, 'r') as fr:
            content = fr.read()

    await loop.run_in_executor(None, _read())
    return content


async def write_file(loop: asyncio.BaseEventLoop, filename: str, contents: str, perm='w'):
    def _write():
        with open(filename, perm) as fw:
            fw.write(contents)
    await loop.run_in_executor(None, _write())


class VolumeAgent(AbstractVolumeAgent):
    loop: asyncio.BaseEventLoop

    def __init__(self, mount_path, uid, gid, loop=None):
        self.registry = {}
        self.project_id_pool = []
        self.mount_path = mount_path
        self.uid = uid
        self.gid = gid

        self.loop = loop or asyncio.get_event_loop()

    async def init(self):
        if os.path.isfile('/etc/projid'):
            raw_projid = await read_file(self.loop, '/etc/projid')
            for line in raw_projid.splitlines():
                proj_name, proj_id = line.split(':')[:2]
                self.project_id_pool.append(int(proj_id))
        else:
            await run('touch /etc/projid')

        if not os.path.isfile('/etc/projects'):
            await run('touch /etc/projects')

    async def create(self, kernel_id: str, size: str) -> str:
        project_id = -1

        if kernel_id in self.registry.keys():
            return ''

        for i in range(len(self.project_id_pool) - 1):
            if self.project_id_pool[i] + 1 != self.project_id_pool[i + 1]:
                project_id = self.project_id_pool[i] + 1
                break
        if len(self.project_id_pool) == 0:
            project_id = 1
        if project_id == -1:
            project_id = self.project_id_pool[-1] + 1

        folder_path = (Path(self.mount_path) / kernel_id).resolve()

        os.mkdir(folder_path)
        os.chown(folder_path, self.uid, self.gid)

        await write_file(self.loop, '/etc/projects', f'{project_id}:{folder_path}', perm='a')
        await write_file(self.loop, '/etc/projid', f'{kernel_id}:{project_id}', perm='a')
        await run(f'xfs_quota -x -c "project -s {kernel_id}" {self.mount_path}')
        await run(f'xfs_quota -x -c "limit -p bhard={size} {kernel_id}" {self.mount_path}')
        self.registry[kernel_id] = project_id
        self.project_id_pool += [project_id]
        self.project_id_pool.sort()

        return folder_path.absolute().as_posix()

    async def remove(self, kernel_id: str):
        if kernel_id not in self.registry.keys():
            return

        await run(f'xfs_quota -x -c "limit -p bsoft=0 bhard=0 {kernel_id}" {self.mount_path}')

        raw_projects = await read_file(self.loop, '/etc/projects')
        raw_projid = await read_file(self.loop, '/etc/projid')
        new_projects = ''
        new_projid = ''
        for line in raw_projects.splitlines():
            if line.startswith(str(self.registry[kernel_id]) + ':'):
                continue
            new_projects += (line + '\n')
        for line in raw_projid.splitlines():
            if line.startswith(kernel_id + ':'):
                continue
            new_projid += (line + '\n')
        await write_file(self.loop, '/etc/projects', new_projects)
        await write_file(self.loop, '/etc/projid', new_projid)
        shutil.rmtree(Path(self.mount_path) / kernel_id)
        self.project_id_pool.remove(self.registry[kernel_id])
        del self.registry[kernel_id]

    async def get(self, kernel_id: str) -> str:
        return (Path(self.mount_path) / kernel_id).resolve().absolute().as_posix()
