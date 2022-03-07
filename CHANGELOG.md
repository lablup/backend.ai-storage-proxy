Changes
=======

<!--
    You should *NOT* be adding new change log entries to this file, this
    file is managed by towncrier. You *may* edit previous change logs to
    fix problems like typo corrections or such.

    To add a new change log entry, please refer
    https://pip.pypa.io/en/latest/development/contributing/#news-entries

    We named the news folder "changes".

    WARNING: Don't drop the last line!
-->

<!-- towncrier release notes start -->

## 21.09.3 (2022-01-26)

* Upgrade dependencies for Backend.AI common to 21.09.6


## 21.09.2 (2022-01-11)

### Features
* Enable basic vfolder operation such as caculating usage and cloning vfolder in storage-proxy using NetApp XCP. ([#34](https://github.com/lablup/backend.ai-storage-proxy/issues/34))

### Fixes
* Update aiotools version to 1.4 to work with the latest common pkg ([#36](https://github.com/lablup/backend.ai-storage-proxy/issues/36))

### Miscellaneous
* Workaround a mypy regression related with type inference on `os.scandir()` by pinning it to an older version (0.910) ([#37](https://github.com/lablup/backend.ai-storage-proxy/issues/37))


## 21.09.1 (2021-11-11)

* Upgrade dependencies for Backend.AI common 21.9.1 and aiohttp 3.8


## 21.09.0 (2021-11-08)

### Features
* Enable support of Storage Proxy for NetApp and NFS file system. ([#26](https://github.com/lablup/backend.ai-storage-proxy/issues/26))

### Fixes
* Introduced `FileLock` async context manager to avoid a race condition on accessing `/etc/proj*` files, which are needed to set per-directory quota in XFS backend, between multiple Storage-Proxy processes/threads. For easier management of project entries, `XfsProjectRegistry` is also introduced. ([#13](https://github.com/lablup/backend.ai-storage-proxy/issues/13))
* Remove the discouraged `loop` argument from the `AsyncFileWriter` constructor ([#23](https://github.com/lablup/backend.ai-storage-proxy/issues/23))
* Sliently skip when physical directory corresponding to a vfolder does not exist in deleting the vfolder. ([#29](https://github.com/lablup/backend.ai-storage-proxy/issues/29))
* Explicitly call `asyncio.get_child_watcher()` to initiallize subprocess child watchers used by several storage backends ([#32](https://github.com/lablup/backend.ai-storage-proxy/issues/32))
* Register every (XFS) virtual folder to XFS projects even if no quota is set, to easily fetch the inode count and used bytes those are provided by the filesystem. ([#33](https://github.com/lablup/backend.ai-storage-proxy/issues/33))

### Documentation Changes
* Add description for 3rd party storage, netapp in README ([#28](https://github.com/lablup/backend.ai-storage-proxy/issues/28))


21.03.0 (2021-03-29)
--------------------

This release has identical features and fixes in the v20.09 series but uses Python 3.9 as the running environment.
