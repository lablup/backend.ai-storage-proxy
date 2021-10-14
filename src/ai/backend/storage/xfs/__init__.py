import asyncio
import fcntl
import logging
import os
from pathlib import Path
import time
from typing import Dict, List
from uuid import UUID

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import BinarySize

from ..exception import ExecutionError, VFolderCreationError
from ..types import VFolderCreationOptions
from ..vfs import BaseVolume, run

log = BraceStyleAdapter(logging.getLogger(__name__))

LOCK_FILE = Path('/tmp/backendai-xfs-file-lock')
Path(LOCK_FILE).touch()


class FileLock:
    default_timeout: int = 3  # not allow infinite timeout for safety
    locked: bool = False

    def __init__(self, path: Path, *, mode: str = "rb", timeout: int = None):
        self._path = path
        self._mode = mode
        self._timeout = timeout if timeout is not None else self.default_timeout

    async def __aenter__(self):
        def _lock():
            start_time = time.perf_counter()
            self._fp = open(self._path, self._mode)
            while True:
                try:
                    fcntl.flock(self._fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.locked = True
                    log.debug("file lock acquired: {}", self._path)
                    return self._fp
                except BlockingIOError:
                    # Failed to get file lock. Waiting until timeout ...
                    if time.perf_counter() - start_time > self._timeout:
                        raise TimeoutError(f"failed to lock file: {self._path}")
                time.sleep(0.1)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _lock)

    async def __aexit__(self, *args):
        def _unlock():
            if self.locked:
                fcntl.flock(self._fp, fcntl.LOCK_UN)
                self.locked = False
                log.debug("file lock released: {}", self._path)
            self._fp.close()
            self.f_fp = None

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _unlock)


class Singleton(type):
    _instances: dict = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class XfsProjectRegistry(metaclass=Singleton):
    file_projects: Path = Path("/etc/projects")
    file_projid: Path = Path("/etc/projid")
    backend: BaseVolume
    name_id_map: Dict[UUID, int] = dict()
    project_id_pool: List[int] = list()

    async def init(self, backend: BaseVolume) -> None:
        self.backend = backend

    async def _read_project_info(self):
        # TODO: how to handle if /etc/proj* files are deleted by external reason?
        if self.file_projid.is_file():
            project_id_pool = []
            self.name_id_map = {}
            raw_projid = self.file_projid.read_text()
            for line in raw_projid.splitlines():
                proj_name, proj_id = line.split(":")[:2]
                project_id_pool.append(int(proj_id))
                self.name_id_map[UUID(proj_name)] = int(proj_id)
            self.project_id_pool = sorted(project_id_pool)
        else:
            await run(f"sudo touch {self.file_projid}")
        if not Path(self.file_projects).is_file():
            await run(f"sudo touch {self.file_projects}")

    async def add_project_entry(
        self,
        *,
        vfid: UUID,
        quota: int,
        project_id: int = None,
    ) -> None:
        vfpath = self.backend.mangle_vfpath(vfid)
        await self._read_project_info()
        if project_id is None:
            project_id = self.get_project_id()
        await run(f"sudo sh -c \"echo '{project_id}:{vfpath}' >> {self.file_projects}\"")
        await run(f"sudo sh -c \"echo '{str(vfid)}:{project_id}' >> {self.file_projid}\"")
        self.name_id_map[vfid] = project_id
        self.project_id_pool.append(project_id)
        self.project_id_pool.sort()

    async def remove_project_entry(self, vfid: UUID) -> None:
        await self._read_project_info()
        await run(f"sudo sed -i.bak '/{vfid.hex[4:]}/d' {self.file_projects}")
        await run(f"sudo sed -i.bak '/{vfid}/d' {self.file_projid}")
        try:
            self.project_id_pool.remove(self.name_id_map[vfid])
        except ValueError:
            pass
        self.name_id_map.pop(vfid, None)

    def get_project_id(self) -> int:
        """
        Get the next project_id, which is the smallest unused integer.
        """
        project_id = -1
        for i in range(len(self.project_id_pool) - 1):
            if self.project_id_pool[i] + 1 != self.project_id_pool[i + 1]:
                project_id = self.project_id_pool[i] + 1
                break
        if len(self.project_id_pool) == 0:
            project_id = 1
        if project_id == -1:
            project_id = self.project_id_pool[-1] + 1
        return project_id


class XfsVolume(BaseVolume):
    """
    XFS volume backend. XFS natively supports per-directory quota through
    the project qutoa. To enalbe project quota, the XFS volume should be
    mounted with `-o pquota` option.

    This backend requires `root` or no password `sudo` permission to run
    `xfs_quota` command and write to `/etc/projects` and `/etc/projid`.
    """
    registry: XfsProjectRegistry

    async def init(self, uid: int = None, gid: int = None) -> None:
        self.uid = uid if uid is not None else os.getuid()
        self.gid = gid if gid is not None else os.getgid()
        self.registry = XfsProjectRegistry()
        await self.registry.init(self)

    # ----- volume opeartions -----
    async def create_vfolder(
        self,
        vfid: UUID,
        options: VFolderCreationOptions = None,
    ) -> None:
        await super().create_vfolder(vfid, options)

        # NOTE: Do we need to register project ID for a directory without quota?
        if options is None or options.quota is None:  # max quota i.e. the whole fs size
            fs_usage = await self.get_fs_usage()
            quota = fs_usage.capacity_bytes
        else:
            quota = options.quota
        try:
            async with FileLock(LOCK_FILE):
                log.info("setting project quota (f:{}, q:{})", vfid, str(quota))
                await self.registry.add_project_entry(vfid=vfid, quota=quota)
                await self.set_quota(vfid, quota)
        except (asyncio.CancelledError, asyncio.TimeoutError) as e:
            log.exception("vfolder creation timeout", exc_info=e)
            await self.delete_vfolder(vfid)
            raise
        except Exception as e:
            log.exception("vfolder creation error", exc_info=e)
            await self.delete_vfolder(vfid)
            raise VFolderCreationError("problem in setting vfolder quota")

    async def delete_vfolder(self, vfid: UUID) -> None:
        async with FileLock(LOCK_FILE):
            if vfid in self.registry.name_id_map.keys():
                try:
                    log.info("removing project quota (f:{})", vfid)
                    await self.set_quota(vfid, BinarySize(0))
                except (asyncio.CancelledError, asyncio.TimeoutError) as e:
                    log.exception("vfolder deletion timeout", exc_info=e)
                    pass  # Pass to delete the physical directlry anyway.
                except Exception as e:
                    log.exception("vfolder deletion error", exc_info=e)
                    pass  # Pass to delete the physical directlry anyway.
                finally:
                    await self.registry.remove_project_entry(vfid)
            await super().delete_vfolder(vfid)

    async def get_quota(self, vfid: UUID) -> BinarySize:
        report = await run(
            f"sudo xfs_quota -x -c 'report -h' {self.mount_path}"
            f" | grep {str(vfid)[:-5]}"
        )
        if len(report.split()) != 6:
            raise ExecutionError("unexpected format for xfs_quota report")
        proj_name, _, _, quota, _, _ = report.split()
        if not str(vfid).startswith(proj_name):
            raise ExecutionError("vfid and project name does not match")
        return BinarySize.finite_from_str(quota)

    async def set_quota(self, vfid: UUID, size_bytes: BinarySize) -> None:
        if vfid not in self.registry.name_id_map.keys():
            await run(f"sudo xfs_quota -x -c 'project -s {vfid}' {self.mount_path}")
        await run(
            f"sudo xfs_quota -x -c "
            f"'limit -p bsoft={int(size_bytes)} bhard={int(size_bytes)} {vfid}' "
            f"{self.mount_path}"
        )
