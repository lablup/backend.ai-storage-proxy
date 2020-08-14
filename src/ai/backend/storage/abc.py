from abc import ABCMeta, abstractmethod
from pathlib import Path, PurePosixPath
from typing import (
    AsyncIterator,
    Sequence,
)
from uuid import UUID

from .types import (
    FSPerfMetric,
    FSUsage,
    VFolderUsage,
    DirEntry,
)


class AbstractVFolderHost(metaclass=ABCMeta):

    def __init__(self, mount_path: Path) -> None:
        self.mount_path = mount_path

    async def init(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    # ------ vfhost operations -------

    @abstractmethod
    async def create_vfolder(self, vfid: UUID) -> None:
        pass

    @abstractmethod
    async def delete_vfolder(self, vfid: UUID) -> None:
        pass

    @abstractmethod
    async def clone_vfolder(self, src_vfid: UUID, new_vfid: UUID) -> None:
        pass

    @abstractmethod
    async def put_metadata(self, vfid: UUID, payload: bytes) -> None:
        pass

    @abstractmethod
    async def get_metadata(self, vfid: UUID) -> bytes:
        pass

    @abstractmethod
    async def get_quota(self, vfid: UUID) -> int:
        pass

    @abstractmethod
    async def set_quota(self, vfid: UUID, size_bytes: int) -> None:
        pass

    @abstractmethod
    async def get_performance_metric(self) -> FSPerfMetric:
        pass

    @abstractmethod
    async def get_fs_usage(self) -> FSUsage:
        pass

    @abstractmethod
    async def get_usage(self, vfid: UUID, relpath: PurePosixPath = None) -> VFolderUsage:
        pass

    # ------ vfolder internal operations -------

    @abstractmethod
    def scandir(self, vfid: UUID, relpath: PurePosixPath) -> AsyncIterator[DirEntry]:
        pass

    @abstractmethod
    async def mkdir(self, vfid: UUID, relpath: PurePosixPath, *, parents: bool = False) -> None:
        pass

    @abstractmethod
    async def rmdir(self, vfid: UUID, relpath: PurePosixPath, *, recursive: bool = False) -> None:
        pass

    @abstractmethod
    async def move_file(self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath) -> None:
        pass

    @abstractmethod
    async def copy_file(self, vfid: UUID, src: PurePosixPath, dst: PurePosixPath) -> None:
        pass

    @abstractmethod
    async def add_file(self, vfid: UUID, relpath: PurePosixPath, payload: AsyncIterator[bytes]) -> None:
        pass

    @abstractmethod
    def read_file(
        self,
        vfid: UUID,
        relpath: PurePosixPath,
        *,
        chunk_size: int = 0,
    ) -> AsyncIterator[bytes]:
        pass

    @abstractmethod
    async def delete_files(self, vfid: UUID, relpaths: Sequence[PurePosixPath]) -> None:
        pass
