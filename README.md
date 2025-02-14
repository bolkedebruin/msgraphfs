# MSGraphFS

This python package is a fsspec based filesystem-like interface to drive exposed
through the Microsoft graph API (OneDrive, Sharepoint, etc).

see:
https://learn.microsoft.com/en-us/graph/api/resources/onedrive?view=graph-rest-1.0

## Usage

To use the SharePoint filesystem, you need to create a new instance of the
`msgraphfs.MSGDriveFS` class. You can also use the `msgd` protocol to lookup the
class using `fsspec.get_filesystem_class`.

```python
import msgraphfs

fs = msgraphfs.MSGDriveFS(
    client_id="YOUR_CLIENT_ID",
    drive_id="YOUR_DRIVE_ID",
    oauth2_client_params = {...})

fs.ls("/")

with fs.open("/path/to/file.txt") as f:
    print(f.read())
```

```python

import fsspec

fs = fsspec.get_filesystem_class("msgd")(
    client_id="YOUR_CLIENT
    drive_id="YOUR_DRIVE_ID",
    oauth2_client_params = {...})

fs.ls("/")

```

### Specific functionalities

- `ls`, `info` : Both methods can take an `expand` additional argument. This
  argument is a string that will be passed as the `expand` query parameter to
  the microsoft graph API call used to get the file information. This can be
  used to get additional information about the file, such as the `thumbnails` or
  the `permissions` or ...

- `checkin`, `checkout` : These methods are used to checkin/checkout a file.
  They take the path of the file to checkin/checkout as argument. The `checking`
  method also take an additional `comment`.

- `get_versions` : This method returns the list of versions of a file. It takes
  the path of the file as argument.

- `preview` : This method returns a url to a preview of the file. It takes the
  path of the file as argument.

- `get_content` : This method returns the content of a file. It takes the path
  or the item_id of the file as argument. You can also pass the `format` argument
  to specify the expected format of the content. This is useful for example to
  convert a word document to a pdf.

In addition to the methods above, some methods can take an `item_id` additional
arguments. This argument is the id of the drive item provided by the Microsoft
Graph API. It can be used to avoid the need to make an additional API call to
get the item id or to store a reference to a drive item independently of the
path. (If the drive item is moved, the path will change but the item id will
not).

## Installation

```bash
pip install msgraphfs
```

### Get your drive id

To get the drive id of your drive, you can use the microsoft graph explorer:
https://developer.microsoft.com/en-us/graph/graph-explorer

The first step is to get the site id of your site. You can do this by making a
GET request to the following url:

```bash
https://graph.microsoft.com/v1.0/sites/{url}
```

where `{url}` is the url of your site without the protocol. For example, if your
site is `https://mycompany.sharepoint.com/sites/mysite`, you should use
`mycompany.sharepoint.com/sites/mysite` as the url.

In the response, you will find the `id` of the site. 


Now your can get the drive id of the drive you want to access. To do this, you
can make a GET request to the following url:

```bash
  https://graph.microsoft.com/v1.0/sites/{site_id}/drives/
```

where `{site_id}` is the id of the site you got in the previous step.
