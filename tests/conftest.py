from pathlib import Path
import tempfile

import pytest


@pytest.fixture
def vfroot():
    with tempfile.TemporaryDirectory(prefix="bai-storage-test-") as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def local_volume(vfroot):
    volume = vfroot / 'local'
    volume.mkdir(parents=True, exist_ok=True)
    yield volume
