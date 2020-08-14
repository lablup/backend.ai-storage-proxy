from datetime import datetime
from typing import (
    Union,
)


def fstime2datetime(t: Union[float, int]) -> datetime:
    return datetime.fromtimestamp(t)
