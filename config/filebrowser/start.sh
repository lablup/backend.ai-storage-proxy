#!/bin/sh
/bin/filebrowser config init -d /filebrowser_dir/filebrowser.db;

/bin/filebrowser users add admin admin;

/bin/filebrowser config import /filebrowser_dir/config.json -d /filebrowser_dir/filebrowser.db;

/bin/filebrowser -c /filebrowser_dir/settings.json -d /filebrowser_dir/filebrowser.db;

exit 0;