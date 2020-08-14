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


@pytest.fixture
async def empty_vfolder(vfs):
    vfid = uuid.uuid4()
    await vfs.create_vfolder(vfid)
    yield vfid
    await vfs.delete_vfolder(vfid)


@pytest.mark.asyncio
async def test_vfs_vfolder_mgmt(vfs):
    vfid = uuid.uuid4()
    await vfs.create_vfolder(vfid)
    vfpath = vfs.mount_path / vfid.hex[0:2] / vfid.hex[2:4] / vfid.hex[4:]
    assert vfpath.is_dir()
    await vfs.delete_vfolder(vfid)
    assert not vfpath.exists()
    assert not vfpath.parent.exists()
    assert not vfpath.parent.parent.exists()

    vfid1 = uuid.UUID(hex='82a6ba2b7b8e41deb5ee2c909ce34bcb')
    vfid2 = uuid.UUID(hex='82a6ba2b7b8e41deb5ee2c909ce34bcc')
    await vfs.create_vfolder(vfid1)
    await vfs.create_vfolder(vfid2)
    vfpath1 = vfs.mount_path / vfid1.hex[0:2] / vfid1.hex[2:4] / vfid1.hex[4:]
    vfpath2 = vfs.mount_path / vfid2.hex[0:2] / vfid2.hex[2:4] / vfid2.hex[4:]
    assert vfpath2.relative_to(vfpath1.parent).name == vfpath2.name
    assert vfpath1.is_dir()
    await vfs.delete_vfolder(vfid1)
    assert not vfpath1.exists()
    # if the prefix dirs are not empty, they shouldn't be deleted
    assert vfpath1.parent.exists()
    assert vfpath1.parent.parent.exists()
    await vfs.delete_vfolder(vfid2)
    assert not vfpath2.exists()
    # if the prefix dirs become empty, they should be deleted
    assert not vfpath2.parent.exists()
    assert not vfpath2.parent.parent.exists()


@pytest.mark.asyncio
async def test_vfs_get_usage(vfs, empty_vfolder):
    vfpath = vfs._mangle_vfpath(empty_vfolder)
    (vfpath / 'test.txt').write_bytes(b'12345')
    (vfpath / 'inner').mkdir()
    (vfpath / 'inner' / 'hello.txt').write_bytes(b'678')
    (vfpath / 'inner' / 'world.txt').write_bytes(b'901')
    usage = await vfs.get_usage(empty_vfolder)
    assert usage.file_count == 3
    assert usage.used_bytes == 11
