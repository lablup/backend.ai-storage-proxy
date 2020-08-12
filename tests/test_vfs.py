import uuid

import pytest

from ai.backend.storage.vfs import BaseVFolderHost


@pytest.fixture
async def vfs(vfhost_local):
    vfs = BaseVFolderHost(vfhost_local)
    await vfs.init()
    try:
        yield vfs
    finally:
        await vfs.shutdown()


@pytest.mark.asyncio
async def test_vfs_vfolder_mgmt(vfs):
    vfid = uuid.uuid4()
    await vfs.create_vfolder(vfid)
    vfpath = vfs.mount_path / vfid.hex[0:2] / vfid.hex[2:4] / vfid.hex[4:]
    assert vfpath.is_dir()
    await vfs.delete_vfolder(vfid)
    assert not vfpath.exists()
