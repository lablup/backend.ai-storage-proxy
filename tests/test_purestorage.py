import os
from pathlib import Path
import secrets
import shutil
import uuid

import pytest

from ai.backend.storage.purestorage import FlashBladeVFolderHost


@pytest.fixture
def fbroot():
    tmpdir_name = f"bai-storage-test-{secrets.token_urlsafe(12)}"
    tmpdir = Path(os.environ['BACKEND_STORAGE_TEST_FBMOUNT']) / tmpdir_name
    tmpdir.mkdir()
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)


@pytest.fixture
async def fbfs(fbroot):
    host = FlashBladeVFolderHost(fbroot)
    await host.init()
    try:
        yield host
    finally:
        await host.shutdown()


@pytest.fixture
async def empty_vfolder(fbfs):
    vfid = uuid.uuid4()
    await fbfs.create_vfolder(vfid)
    yield vfid
    await fbfs.delete_vfolder(vfid)


@pytest.mark.asyncio
async def test_fbfs_get_usage(fbfs, empty_vfolder):
    vfpath = fbfs._mangle_vfpath(empty_vfolder)
    (vfpath / 'test.txt').write_bytes(b'12345')
    (vfpath / 'inner').mkdir()
    (vfpath / 'inner' / 'hello.txt').write_bytes(b'678')
    (vfpath / 'inner' / 'world.txt').write_bytes(b'901')
    usage = await fbfs.get_usage(empty_vfolder)
    assert usage.file_count == 3  # including directory
    assert usage.used_bytes == 11
