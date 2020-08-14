from pathlib import Path
import tempfile

import pytest


@pytest.fixture
def vfroot():
    with tempfile.TemporaryDirectory(prefix="bai-storage-test-") as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def vfhost_local(vfroot):
    vfhost = vfroot / 'local'
    vfhost.mkdir(parents=True, exist_ok=True)
    yield vfhost
