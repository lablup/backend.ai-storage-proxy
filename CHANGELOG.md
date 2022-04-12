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

## 22.03.0b1 (2022-04-12)

No significant changes.


## 22.03.0a1 (2022-03-14)

### Breaking Changes
* Now it requires Python 3.10 or higher to run. ([#42](https://github.com/lablup/backend.ai-storage-proxy/issues/42))

### Features
* Enable basic vfolder operation such as caculating usage and cloning vfolder in storage-proxy using NetApp XCP. ([#34](https://github.com/lablup/backend.ai-storage-proxy/issues/34))
* Add a new generic move-file manager-facing API ([#40](https://github.com/lablup/backend.ai-storage-proxy/issues/40))
* Add an explicit check for validity of subpaths to `get_vfolder_mount()` API for early failure when enqueueing sessions ([#41](https://github.com/lablup/backend.ai-storage-proxy/issues/41))

### Fixes
* Update aiotools version to 1.4 to work with the latest common pkg ([#36](https://github.com/lablup/backend.ai-storage-proxy/issues/36))
* Split vfolder creation from vfolder clone with `exist_ok=True` option to allow separate invocation of those two operations when cloning a vfolder to achieve better asynchrony in the manager side ([#38](https://github.com/lablup/backend.ai-storage-proxy/issues/38))

### Miscellaneous
* Update the `move_file` API to handle both move of files and folders and deprecate the `move_tree` API ([#31](https://github.com/lablup/backend.ai-storage-proxy/issues/31))
* Elaborate the sample config and README about preparation of `/vfroot` ([#35](https://github.com/lablup/backend.ai-storage-proxy/issues/35))
* Workaround a mypy regression related with type inference on `os.scandir()` by pinning it to an older version (0.910) ([#37](https://github.com/lablup/backend.ai-storage-proxy/issues/37))
* Upgrade aiotools to v1.5 series ([#39](https://github.com/lablup/backend.ai-storage-proxy/issues/39))


## Older changelogs

* [21.09](https://github.com/lablup/backend.ai-storage-proxy/blob/21.09/CHANGELOG.md)
