from pathlib import Path
import uuid
import os

import pytest

from ai.backend.storage.xfs import XfsVolume


@pytest.fixture
async def xfs():
    xfs = XfsVolume({}, Path('/vfroot/xfs'))
    await xfs.init(os.getuid(), os.getgid())
    try:
        yield xfs
    finally:
        await xfs.shutdown()


@pytest.mark.asyncio
async def test_xfs_vfolder_mgmt(xfs):
    vfid = uuid.uuid4()
    options = {'bsize': '10m'}
    await xfs.create_vfolder(vfid, options=options)
    vfpath = xfs.mount_path / vfid.hex[0:2] / vfid.hex[2:4] / vfid.hex[4:]
    assert vfpath.is_dir()
