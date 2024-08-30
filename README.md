# MSGraphFS

This python package is a fsspec based filesystem-like interface to drive exposed
through the Microsoft graph API (OneDrive, Sharepoint, etc).

see:
https://learn.microsoft.com/en-us/graph/api/resources/onedrive?view=graph-rest-1.0

The supported applications are:

- SharePoint

## SharePoint

To use the SharePoint filesystem, you need to create a new instance of the
`msgraphfs.SharepointFS` class. You can also use the `shpt` protocol to lookup
the class using `fsspec.get_filesystem_class`.

```python
import msgraphfs

fs = msgraphfs.SharepointFS(
    client_id="YOUR_CLIENT_ID",
    drive_id="YOUR_DRIVE_ID",
    oauth2_client_params = {...})

fs.ls("/")

with fs.open("/path/to/file.txt") as f:
    print(f.read())
```

```python

import fsspec

fs = fsspec.get_filesystem_class("shpt")(
    client_id="YOUR_CLIENT
    drive_id="YOUR_DRIVE_ID",
    oauth2_client_params = {...})

fs.ls("/")

```

## Installation

```bash
pip install msgraphfs
```

## TODO

- [ ] Implement the OneDrive filesystem (Should be an extend of the base class
      `msgraphfs.AbstractMSGraphFS` implementing the `_path_to_url` method)
