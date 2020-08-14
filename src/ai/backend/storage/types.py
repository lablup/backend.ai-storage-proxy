import attr


@attr.s(auto_attribs=True, slots=True)
class FSPerfMetric:
    iops_read: int
    iops_write: int
    iobytes_read: int
    iobytes_write: int


@attr.s(auto_attribs=True, slots=True)
class FSUsage:
    capacity_bytes: int
    used_bytes: int


@attr.s(auto_attribs=True, slots=True)
class VFolderUsage:
    file_count: int
    used_bytes: int
