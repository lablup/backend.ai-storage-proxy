from pathlib import Path
import uuid
import os

import pytest

from ai.backend.storage.xfs import XfsVolume
from ai.backend.common.types import BinarySize


def read_etc_projid():
    with open('/etc/projid') as fp:
        content = fp.read()
    project_id_dict = {}
    for line in content.splitlines():
        proj_name, proj_id = line.split(':')[:2]
        project_id_dict[proj_name] = int(proj_id)
    return project_id_dict


def read_etc_projects():
    with open('/etc/projects') as fp:
        content = fp.read()
    vfpath_id_dict = {}
    for line in content.splitlines():
        proj_id, vfpath = line.split(':')[:2]
        vfpath_id_dict[int(proj_id)] = vfpath
    return vfpath_id_dict


@pytest.fixture
async def xfs():
    xfs = XfsVolume({}, Path('/vfroot/xfs'))
    await xfs.init(os.getuid(), os.getgid())
    try:
        yield xfs
    finally:
        await xfs.shutdown()


@pytest.fixture
async def empty_vfolder(xfs):
    vfid = uuid.uuid4()
    await xfs.create_vfolder(vfid, options={'quota': '10m'})
    yield vfid
    await xfs.delete_vfolder(vfid)


@pytest.mark.asyncio
async def test_xfs_single_vfolder_mgmt(xfs):
    vfid = uuid.uuid4()
    options = {'quota': '10m'}
    # vfolder create test
    await xfs.create_vfolder(vfid, options=options)
    vfpath = xfs.mount_path / vfid.hex[0:2] / vfid.hex[2:4] / vfid.hex[4:]
    project_id_dict = read_etc_projid()
    vfpath_id_dict = read_etc_projects()
    assert vfpath.is_dir()
    assert str(vfid) in project_id_dict
    vfid_project_id = project_id_dict[str(vfid)]
    # vfolder delete test
    assert vfpath_id_dict[project_id_dict[str(vfid)]] == str(vfpath)
    await xfs.delete_vfolder(vfid)
    assert not vfpath.exists()
    assert not vfpath.parent.exists()
    assert not vfpath.parent.parent.exists()
    project_id_dict = read_etc_projid()
    vfpath_id_dict = read_etc_projects()
    assert str(vfid) not in project_id_dict
    assert vfid_project_id not in vfpath_id_dict


@pytest.mark.asyncio
async def test_xfs_multiple_vfolder_mgmt(xfs):
    vfid1 = uuid.UUID(hex='82a6ba2b7b8e41deb5ee2c909ce34bcb')
    vfid2 = uuid.UUID(hex='82a6ba2b7b8e41deb5ee2c909ce34bcc')
    options = {'quota': '10m'}
    await xfs.create_vfolder(vfid1, options=options)
    await xfs.create_vfolder(vfid2, options=options)
    vfpath1 = xfs.mount_path / vfid1.hex[0:2] / vfid1.hex[2:4] / vfid1.hex[4:]
    vfpath2 = xfs.mount_path / vfid2.hex[0:2] / vfid2.hex[2:4] / vfid2.hex[4:]
    assert vfpath2.relative_to(vfpath1.parent).name == vfpath2.name
    assert vfpath1.is_dir()
    await xfs.delete_vfolder(vfid1)
    assert not vfpath1.exists()
    # if the prefix dirs are not empty, they shouldn't be deleted
    assert vfpath1.parent.exists()
    assert vfpath1.parent.parent.exists()
    await xfs.delete_vfolder(vfid2)
    # if the prefix dirs become empty, they should be deleted
    assert not vfpath2.parent.exists()
    assert not vfpath2.parent.parent.exists()


@pytest.mark.asyncio
async def test_xfs_quota(xfs):
    vfid = uuid.uuid4()
    options = {'quota': '10m'}
    await xfs.create_vfolder(vfid, options=options)
    vfpath = xfs.mount_path / vfid.hex[0:2] / vfid.hex[2:4] / vfid.hex[4:]
    assert vfpath.is_dir()
    assert await xfs.get_quota(vfid) == BinarySize.from_str('10m')
    await xfs.set_quota(vfid, BinarySize.from_str('1m'))
    assert await xfs.get_quota(vfid) == BinarySize.from_str('1m')
    await xfs.delete_vfolder(vfid)
    assert not vfpath.is_dir()


@pytest.mark.asyncio
async def test_xfs_get_usage(xfs, empty_vfolder):
    vfpath = xfs.mangle_vfpath(empty_vfolder)
    (vfpath / 'test.txt').write_bytes(b'12345')
    (vfpath / 'inner').mkdir()
    (vfpath / 'inner' / 'hello.txt').write_bytes(b'678')
    (vfpath / 'inner' / 'world.txt').write_bytes(b'901')
    usage = await xfs.get_usage(empty_vfolder)
    assert usage.file_count == 3
    assert usage.used_bytes == 11


@pytest.mark.asyncio
async def test_xfs_mkdir_rmdir(xfs, empty_vfolder):
    vfpath = xfs.mangle_vfpath(empty_vfolder)
    test_rel_path = 'test/abc'
    await xfs.mkdir(empty_vfolder, Path(test_rel_path), parents=True)
    assert Path(vfpath, test_rel_path).is_dir()
    await xfs.rmdir(empty_vfolder, Path(test_rel_path), recursive=True)
    assert not Path(vfpath, test_rel_path).is_dir()

