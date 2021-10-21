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

.. towncrier release notes start

21.03.2 (2021-10-21)
--------------------

### Fixes
* Explicitly call `asyncio.get_child_watcher()` to initiallize subprocess child watchers used by several storage backends ([#32](https://github.com/lablup/backend.ai-storage-proxy/issues/32))
* Register every (XFS) virtual folder to XFS projects even if no quota is set, to easily fetch the inode count and used bytes those are provided by the filesystem. ([#33](https://github.com/lablup/backend.ai-storage-proxy/issues/33))


21.03.1 (2021-10-21)
--------------------

### Fixes
* Introduced `FileLock` async context manager to avoid a race condition on accessing `/etc/proj*` files, which are needed to set per-directory quota in XFS backend, between multiple Storage-Proxy processes/threads. For easier management of project entries, `XfsProjectRegistry` is also introduced. ([#13](https://github.com/lablup/backend.ai-storage-proxy/issues/13))
* Sliently skip when physical directory corresponding to a vfolder does not exist in deleting the vfolder. ([#29](https://github.com/lablup/backend.ai-storage-proxy/issues/29))


21.03.0 (2021-03-29)
--------------------

This release has identical features and fixes in the v20.09 series but uses Python 3.9 as the running environment.
