from abc import ABCMeta, abstractmethod
from pathlib import Path, PurePath, PurePosixPath
from typing import (
    Any,
    AsyncIterator,
    Final,
    FrozenSet,
    Mapping,
    Optional,
    Sequence,
)
from uuid import UUID

from .types import (
    FSPerfMetric,
    FSUsage,
    VFolderCreationOptions,
    VFolderUsage,
    DirEntry,
)
from ai.backend.common.types import BinarySize


# Available capabilities of a volume implementation
CAP_VFOLDER: Final = 'vfolder'
CAP_METRIC: Final = 'metric'
CAP_QUOTA: Final = 'quota'
CAP_FAST_SCAN: Final = 'fast-scan'


class AbstractVolume(metaclass=ABCMeta):

    def __init__(
        self,
        local_config: Mapping[str, Any],
        mount_path: Path,
        *,
        fsprefix: PurePath = None,
        options: Mapping[str, Any] = None,
    ) -> None:
        self.local_config = local_config
        self.mount_path = mount_path
        self.fsprefix = fsprefix or PurePath('.')
        self.config = options or {}

    async def init(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    def mangle_vfpath(self, vfid: UUID) -> Path:
        prefix1 = vfid.hex[0:2]
        prefix2 = vfid.hex[2:4]
        rest = vfid.hex[4:]
        return Path(self.mount_path, prefix1, prefix2, rest)

    def sanitize_vfpath(self, vfid: UUID, relpath: Optional[PurePosixPath]) -> Path:
        if relpath is None:
            relpath = PurePosixPath('.')
        vfpath = self.mangle_vfpath(vfid)
        target_path = (vfpath / relpath).resolve()
        try:
            target_path.relative_to(vfpath)
        except ValueError:
            raise PermissionError("cannot acess outside of the given vfolder")
        return target_path

    # ------ volume operations -------

    @abstractmethod
    async def get_capabilities(self) -> FrozenSet[str]:
        pass

    @abstractmethod
    async def create_vfolder(self, vfid: UUID, options: VFolderCreationOptions = None) -> None:
        pass

    @abstractmethod
    async def delete_vfolder(self, vfid: UUID) -> None:
        pass

    @abstractmethod
    async def clone_vfolder(self, src_vfid: UUID, target_path: str) -> None:
        pass

    @abstractmethod
    async def get_vfolder_mount(self, vfid: UUID) -> Path:
        pass

    @abstractmethod
    async def put_metadata(self, vfid: UUID, payload: bytes) -> None:
        pass

    @abstractmethod
    async def get_metadata(self, vfid: UUID) -> bytes:
        pass

    @abstractmethod
    async def get_quota(self, vfid: UUID) -> BinarySize:
        pass

    @abstractmethod
    async def set_quota(self, vfid: UUID, size_bytes: BinarySize) -> None:
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

    @abstractmethod
    async def get_used_bytes(self, vfid: UUID) -> BinarySize:
        pass

    # ------ vfolder operations -------

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
    async def prepare_upload(self, vfid: UUID) -> str:
        """
        Prepare an upload session by creating a dedicated temporary directory.
        Returns a unique session identifier.
        """
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
