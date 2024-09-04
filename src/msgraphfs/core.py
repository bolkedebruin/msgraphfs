import asyncio
import datetime
import mimetypes
import httpx
import logging
import weakref
from fsspec.asyn import (
    AsyncFileSystem,
    FSTimeoutError,
    sync,
    sync_wrapper,
    AbstractBufferedFile,
    AbstractAsyncStreamedFile,
)

from authlib.integrations.httpx_client import AsyncOAuth2Client


from httpx import HTTPStatusError, Response
from httpx._types import URLTypes

HTTPX_RETRYABLE_ERRORS = (
    asyncio.TimeoutError,
    httpx.NetworkError,
    httpx.ProxyError,
    httpx.TimeoutException,
)

HTTPX_RETRYABLE_HTTP_STATUS_CODES = (500, 502, 503, 504)


_logger = logging.getLogger(__name__)


def wrap_http_not_found_exceptions(func):
    """Wrap a function that calls an HTTP request to handle 404 errors."""

    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                path = e.request.url.path
                if "root:" in path:
                    path = path.split("root:")[-1]
                    path = path[:-1] if path[-1] == ":" else path
                raise FileNotFoundError(f"File not found: {path}") from e
            raise e

    return wrapper


@wrap_http_not_found_exceptions
async def _http_call_with_retry(func, *, args=(), kwargs=None, retries) -> Response:
    kwargs = kwargs or {}
    retries = 1
    for i in range(retries):
        try:
            response = await func(*args, **kwargs)
            response.raise_for_status()
            return response
        except HTTPX_RETRYABLE_ERRORS as e:
            if i == retries - 1:
                raise e
            _logger.debug("Retryable error: %s", e)
            await asyncio.sleep(min(1.7**i * 0.1, 15))
            continue
        except HTTPStatusError as e:
            if e.response.status_code in HTTPX_RETRYABLE_HTTP_STATUS_CODES:
                if i == retries - 1:
                    raise e
                _logger.debug("Retryable HTTP status code: %s", e.response.status_code)
                await asyncio.sleep(min(1.7**i * 0.1, 15))
                continue
            if e.response.status_code != 404:
                _logger.error(
                    "HTTP error %s: %s", e.response.status_code, e.response.content
                )
            raise e


class AbstractMSGraphFS(AsyncFileSystem):
    """A filesystem that represents microsoft files exposed through the microsoft graph
    API.

    parameters:
    oauth2_client_params (dict): Parameters for the OAuth2 client to use for
        authentication. see https://docs.authlib.org/en/latest/client/api.html#authlib.integrations.httpx_client.AsyncOAuth2Client
    """

    retries = 5
    blocksize = 10 * 1024 * 1024  # 10 MB

    def __init__(
        self,
        oauth2_client_params: dict,
        **kwargs,
    ):
        super_kwargs = kwargs.copy()
        super_kwargs.pop("use_listings_cache", None)
        super_kwargs.pop("listings_expiry_time", None)
        super_kwargs.pop("max_paths", None)
        # passed to fsspec superclass... we don't support directory caching
        super().__init__(**super_kwargs)

        self.client: AsyncOAuth2Client = AsyncOAuth2Client(
            **oauth2_client_params,
            follow_redirects=True,
        )
        if not self.asynchronous:
            weakref.finalize(self, self.close_http_session, self.client, self.loop)

    @staticmethod
    def close_http_session(
        client: AsyncOAuth2Client, loop: asyncio.AbstractEventLoop | None = None
    ):
        """Close the HTTP session."""
        if loop is not None and loop.is_running() and not loop.is_closed():
            try:
                loop = asyncio.get_event_loop()
                loop.create_task(client.aclose())
                return
            except RuntimeError:
                pass
            try:
                sync(loop, client.aclose, timeout=0.1)
                return
            except FSTimeoutError:
                pass

    def _path_to_url(self, path, item_id=None, action=None) -> str:
        """This method must be implemented by subclasses to convert a path to a valid
        URL to call the Microsoft Graph API for the given path according to the target
        service.

        (OneDrive, SharePoint, etc.)
        """
        raise NotImplementedError

    def _get_path(self, drive_item_info: dict) -> str:
        parent_path = drive_item_info["parentReference"].get("path")
        if not parent_path:
            return "/"
        # remove all the part before the "root:"
        parent_path = parent_path.split("root:")[1]
        if parent_path and not parent_path.startswith("/"):
            parent_path = "/" + parent_path
        return parent_path + "/" + drive_item_info["name"]

    def _drive_item_info_to_fsspec_info(self, drive_item_info: dict) -> dict:
        """Convert a drive item info to a fsspec info dictionary.

        see
        https://docs.microsoft.com/en-us/graph/api/resources/driveitem?view=graph-rest-1.0
        """
        _type = "other"
        if drive_item_info.get("folder"):
            _type = "directory"
        elif drive_item_info.get("file"):
            _type = "file"
        data = {
            "name": self._get_path(drive_item_info),
            "size": drive_item_info.get("size", 0),
            "type": _type,
            "sharepoint_info": drive_item_info,
            "time": datetime.datetime.fromisoformat(
                drive_item_info.get("createdDateTime", "1970-01-01T00:00:00Z")
            ),
            "mtime": datetime.datetime.fromisoformat(
                drive_item_info.get("lastModifiedDateTime", "1970-01-01T00:00:00Z")
            ),
        }
        if _type == "file":
            data["mimetype"] = drive_item_info.get("file", {}).get("mimeType", "")
        return data

    async def _get_item_id(self, path: str, throw_on_missing=False) -> str | None:
        """Get the item ID of a file or directory.

        Parameters:
        path (str): The path to the file or directory.

        Returns:
        str: The item ID of the file or directory if it exists, otherwise None.
        """
        url = self._path_to_url(path)
        try:
            response = await self._msgraph_get(url, params={"select": "id"})
            return response.json()["id"]
        except FileNotFoundError:
            if throw_on_missing:
                raise
            return None

    get_item_id = sync_wrapper(_get_item_id)

    async def _get_item_reference(self, path: str, item_id: str | None = None) -> dict:
        """Return a dictionary with information about the item reference of the given
        path.

        This method is useful when you need to get an itemReference to
        use as an argument in other methods. see
        https://docs.microsoft.com/en-us/graph/api/resources/itemreference?view=graph-rest-1.0
        """
        url = self._path_to_url(path, item_id=item_id)
        response = await self._msgraph_get(
            url,
            params={
                "select": "id,driveId,driveType,name,path,shareId,sharepointIds,siteId"
            },
        )
        return response.json()

    @staticmethod
    def _guess_type(path: str) -> str:
        return mimetypes.guess_type(path)[0] or "application/octet-stream"

    ################################################
    # Helper methods to call the Microsoft Graph API
    ################################################
    async def _call_msgraph(
        self, http_method: str, url: URLTypes, *args, **kwargs
    ) -> Response:
        """Call the Microsoft Graph API."""
        return await _http_call_with_retry(
            self.client.request,
            args=(http_method, url, *args),
            kwargs=kwargs,
            retries=self.retries,
        )

    call_msgraph = sync_wrapper(_call_msgraph)

    async def _msgraph_get(self, url: URLTypes, *args, **kwargs) -> Response:
        """Send a GET request to the Microsoft Graph API."""
        return await self._call_msgraph("GET", url, *args, **kwargs)

    msgraph_get = sync_wrapper(_msgraph_get)

    async def _msgraph_post(self, url: URLTypes, *args, **kwargs) -> Response:
        """Send a POST request to the Microsoft Graph API."""
        return await self._call_msgraph("POST", url, *args, **kwargs)

    msgraph_post = sync_wrapper(_msgraph_post)

    async def _msgraph_put(self, url: URLTypes, *args, **kwargs) -> Response:
        """Send a PUT request to the Microsoft Graph API."""
        return await self._call_msgraph("PUT", url, *args, **kwargs)

    msgraph_put = sync_wrapper(_msgraph_put)

    async def _msgraph_delete(self, url: URLTypes, *args, **kwargs) -> Response:
        """Send a DELETE request to the Microsoft Graph API."""
        return await self._call_msgraph("DELETE", url, *args, **kwargs)

    msgraph_delete = sync_wrapper(_msgraph_delete)

    async def _msg_graph_patch(self, url: URLTypes, *args, **kwargs) -> Response:
        """Send a PATCH request to the Microsoft Graph API."""
        return await self._call_msgraph("PATCH", url, *args, **kwargs)

    msgraph_patch = sync_wrapper(_msg_graph_patch)

    #############################################################
    # Implement required async methods for the fsspec interface
    #############################################################

    async def _exists(self, path: str, **kwargs) -> bool:
        return await self._get_item_id(path) is not None

    async def _info(self, path: str, item_id: str | None = None, **kwargs) -> dict:
        """Get information about a file or directory.

        Parameters
        ----------
        path : str
            Path to get information about
        item_id: str
            If given, the item_id will be used instead of the path to get
            information about the given path.
        """
        url = self._path_to_url(path, item_id=item_id)
        response = await self._msgraph_get(url)
        return self._drive_item_info_to_fsspec_info(response.json())

    async def _ls(
        self, path: str, detail: bool = True, item_id: str | None = None, **kwargs
    ) -> list[dict | str]:
        """List files in the given path.

        Parameters
        ----------
        path : str
            Path to list files in
        detail: bool
            if True, gives a list of dictionaries, where each is the same as
            the result of ``info(path)``. If False, gives a list of paths
            (str).
        item_id: str
            If given, the item_id will be used instead of the path to list
            the files in the given path.
        kwargs: may have additional backend-specific options, such as version
            information
        """
        url = self._path_to_url(path, item_id=item_id, action="children")
        params = None
        if not detail:
            params = {"select": "name,parentReference"}
        response = await self._msgraph_get(url, params=params)
        items = response.json().get("value", [])
        if not items:
            # maybe the path is a file
            try:
                item = await self._info(path)
                if item["type"] == "file":
                    items = [item["sharepoint_info"]]
            except FileNotFoundError:
                pass
        if detail:
            return [self._drive_item_info_to_fsspec_info(item) for item in items]
        else:
            return [self._get_path(item) for item in items]

    async def _cat_file(
        self,
        path: str,
        start: int = None,
        end: int = None,
        item_id: str | None = None,
        **kwargs,
    ):
        url = self._path_to_url(path, item_id=item_id, action="content")
        headers = kwargs.get("headers", {})
        if start is not None and end is not None:
            headers["Range"] = f"bytes={start}-{end - 1}"
        response = await self._msgraph_get(url, headers=headers)
        return response.content

    async def _pipe_file(self, path: str, value: bytes, **kwargs):
        with self.open(path, "wb") as f:
            await f.write(value)

    async def _get_file(self, rpath: str, lpath: str, **kwargs):
        headers = kwargs.get("headers", {})
        content = await self._cat_file(rpath, **kwargs, headers=headers)
        with open(lpath, "wb") as f:
            f.write(content)

    async def _put_file(self, lpath: str, rpath: str, **kwargs):
        with open(lpath, "rb") as f:
            data = f.read()
        await self._pipe_file(rpath, data, **kwargs)
        while rpath:
            self.invalidate_cache(rpath)
            rpath = self._parent(rpath)

    async def _rm_file(self, path: str, item_id: str | None = None, **kwargs):
        if not await self._isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        item_id = item_id or await self._get_item_id(path, throw_on_missing=True)
        url = self._path_to_url(path, item_id=item_id)
        await self._msgraph_delete(url)
        self.invalidate_cache(path)

    async def cp_file(self, path1: str, path2: str, **kwargs):
        source_item_id = await self._get_item_id(path1, throw_on_missing=True)
        url = self._path_to_url(path1, item_id=source_item_id, action="copy")
        path2 = self._strip_protocol(path2)
        parent_path, _file_name = path2.rsplit("/", 1)
        item_reference = await self._get_item_reference(parent_path)
        json = {
            "parentReference": item_reference,
            "name": _file_name,
        }
        await self._msgraph_post(url, json=json)

    async def _isfile(self, path: str) -> bool:
        url = self._path_to_url(path)
        response = await self._msgraph_get(url, params={"select": "file"})
        return response.json().get("file") is not None

    async def _isdir(self, path: str) -> bool:
        url = self._path_to_url(path)
        response = await self._msgraph_get(url, params={"select": "folder"})
        return response.json().get("folder") is not None

    async def _size(self, path: str) -> int:
        url = self._path_to_url(path)
        response = await self._msgraph_get(url, params={"select": "size"})
        return response.json().get("size", 0)

    async def _mkdir(self, path, create_parents=True, exist_ok=False, **kwargs) -> str:
        path = self._strip_protocol(path).rstrip("/")
        parent, child = path.rsplit("/", 1)
        parent_id = await self._get_item_id(parent)
        if not parent_id and not create_parents:
            raise FileNotFoundError(f"Parent directory does not exist: {parent}")
        if not parent_id:
            await self._mkdir(parent, create_parents=create_parents)
            parent_id = await self._get_item_id(parent)
        url = self._path_to_url(path, item_id=parent_id, action="children")
        response = await self._msgraph_post(
            url,
            json={
                "name": child,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "fail",
            },
        )
        return response.json()["id"]

    async def _makedirs(self, path: str, exist_ok: bool = False):
        try:
            await self._mkdir(path, create_parents=True)
        except HTTPStatusError as e:
            if e.response.status_code == 409:
                if not exist_ok:
                    raise FileExistsError(f"Directory already exists: {path}") from e
            else:
                raise e

    async def _rmdir(self, path: str):
        if not await self._isdir(path):
            raise FileNotFoundError(f"Directory not found: {path}")
        if not await self._ls(path):
            raise OSError(f"Directory not empty: {path}")
        item_id = await self._get_item_id(path, throw_on_missing=True)
        url = self._path_to_url(path, item_id=item_id)
        await self._msgraph_delete(url)
        self.invalidate_cache(path)

    rmdir = sync_wrapper(_rmdir)  # not into the list of async methods to auto wrap

    async def _rm(self, path, recursive=False, batch_size=None, **kwargs):
        if not recursive and await self._isdir(path) and await self._ls(path):
            raise OSError(f"Directory not empty: {path}")
        item_id = await self._get_item_id(path, throw_on_missing=True)
        url = self._path_to_url(path, item_id=item_id)
        await self._msgraph_delete(url)
        self.invalidate_cache(path)

    def _open(
        self,
        path,
        mode="rb",
        block_size="default",
        cache_type="readahead",
        autocommit=True,
        size=None,
        cache_options=None,
        **kwargs,
    ):
        """Open a file for reading or writing.

        Parameters
        ----------
        path: string
            Path of file
        mode: string
            One of 'r', 'w', 'a', 'rb', 'wb', or 'ab'. These have the same meaning
            as they do for the built-in `open` function.
        block_size: int
            Size of data-node blocks if reading
        fill_cache: bool
            If seeking to new a part of the file beyond the current buffer,
            with this True, the buffer will be filled between the sections to
            best support random access. When reading only a few specific chunks
            out of a file, performance may be better if False.
        cache_type: {"readahead", "none", "mmap", "bytes"}, default "readahead"
            Caching policy in read mode. See the definitions in ``core``.
        cache_options : dict
            Additional options passed to the constructor for the cache specified
            by `cache_type`.
        kwargs: dict-like
            Additional parameters used for s3 methods.  Typically used for
            ServerSideEncryption.
        """
        return SharepointBuffredFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
            **kwargs,
        )

    async def open_async(self, path, mode="rb", **kwargs):
        if "b" not in mode or kwargs.get("compression"):
            raise ValueError
        return SharepointStreamedFile(self, path, mode, **kwargs)

    async def _touch(self, path, truncate=True, item_id=None, **kwargs):
        # if the file exists, update the last modified date time
        # otherwise, create an empty file"""
        item_id = item_id or await self._get_item_id(path)
        if item_id and not truncate:
            if truncate:
                url = self._path_to_url(path, item_id=item_id, action="content")
                await self._msgraph_put(
                    url,
                    content=b"",
                    headers={"Content-Type": "application/octet-stream"},
                )
            else:
                url = self._path_to_url(path, item_id=item_id)
                await self._msgraph_patch(
                    url, json={"lastModifiedDateTime": datetime.now().isoformat()}
                )
        else:
            parent_path, file_name = path.rsplit("/", 1)
            parent_id = await self._get_item_id(parent_path, throw_on_missing=True)
            item_id = f"{parent_id}:/{file_name}:"
            url = self._path_to_url(path, item_id=item_id, action="content")
            headers = {"Content-Type": self._guess_type(path)}
            await self._msgraph_put(url, content=b"", headers=headers)
        self.invalidate_cache(path)

    touch = sync_wrapper(_touch)


class SharepointFS(AbstractMSGraphFS):
    """A filesystem that represents a SharePoint site dirve as a filesystem.

    parameters:
    site_id (str): The ID of the SharePoint site.
    drive_id (str): The ID of the SharePoint drive.
    oauth2_client_params (dict): Parameters for the OAuth2 client to use for
        authentication. see https://docs.authlib.org/en/latest/client/api.html#authlib.integrations.httpx_client.AsyncOAuth2Client
    """

    protocol = ["shpt"]

    def __init__(
        self,
        site_id: str,
        drive_id: str,
        oauth2_client_params: dict,
        **kwargs,
    ):
        super().__init__(oauth2_client_params=oauth2_client_params, **kwargs)
        self.site_id: str = site_id
        self.drive_id: str = drive_id
        self.drive_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}"

    def _path_to_url(self, path, item_id=None, action=None) -> str:
        action = action and f"/{action}" if action else ""
        path = self._strip_protocol(path).rstrip("/")
        if path:
            path = f":{path}:"
        if item_id:
            return f"{self.drive_url}/items/{item_id}{action}"

        return f"{self.drive_url}/root{path}{action}"


class AsyncStreamedFileMixin:
    """Mixin for streamed file-like objects using async iterators."""

    def _init__mixin(self, **kwargs):
        self.path = self.fs._strip_protocol(self.path)
        block_size = kwargs.get("block_size", "default")
        if block_size == "default":
            block_size = None
        self.blocksize = block_size if block_size is not None else self.fs.blocksize
        if "w" in self.mode or "b" in self.mode:
            # block_size must bet a multiple of 320 KiB
            if self.blocksize % (320 * 1024) != 0:
                raise ValueError("block_size must be a multiple of 320 KiB")
        self._append_mode = "a" in self.mode and self.item_id is not None
        self._reset_session_info()

    async def _create_upload_session(self) -> tuple[str, datetime.datetime]:
        """Create a new upload session for the file.

        Returns:
            tuple[str, datetime.datetime]: The URL of the upload session and the expiration date time.

        see https://learn.microsoft.com/en-us/graph/api/driveitem-createuploadsession?view=graph-rest-1.0
        """
        item_id = await self.item_id
        if not item_id:
            parent_path, file_name = self.path.rsplit("/", 1)
            parent_id = await self.fs._get_item_id(parent_path)
            item_id = f"{parent_id}:/{file_name}:"
        url = self.fs._path_to_url(
            self.path, item_id=item_id, action="createUploadSession"
        )
        response = await self.fs._msgraph_post(
            url,
            json={
                "@microsoft.graph.conflictBehavior": "replace",
                # We don't know the size of the file. Explicit commit is required.
                "deferCommit": True,
            },
        )
        json = response.json()
        expiration_dt = datetime.datetime.fromisoformat(json["expirationDateTime"])
        return json["uploadUrl"], expiration_dt

    @property
    def _is_upload_session_expired(self) -> bool:
        """Check if the current upload session is expired."""
        if not self._upload_expiration_dt:
            return True
        return datetime.datetime.now() > self._upload_expiration_dt

    def _reset_session_info(self):
        """Reset the upload session information."""
        self._upload_session_url = None
        self._upload_expiration_dt = None
        self._chunk_start_pos = 0
        self._remaining_bytes = None

    async def _upload_content_at_once(self, data):
        headers = self.kwargs.get("headers", {})
        if "content-type" not in headers:
            headers["content-type"] = self.fs._guess_type(self.path)
        item_id = await self.fs._get_item_id(self.path)
        if not item_id:
            parent_path, file_name = self.path.rsplit("/", 1)
            parent_id = await self.fs._get_item_id(parent_path, throw_on_missing=True)
            item_id = f"{parent_id}:/{file_name}:"
        url = self.fs._path_to_url(self.path, item_id=item_id, action="content")
        await self.fs._msgraph_put(url, content=data, headers=headers)
        self.fs.invalidate_cache(self.path)

    async def _abort_upload_session(self):
        """Abort the current upload session."""
        if self._upload_session_url and not self._is_upload_session_expired:
            await self.fs._msgraph_delete(self._upload_session_url)
        self._reset_session_info()

    async def _commit_upload_session(self):
        """Commit the current upload session."""
        if self._upload_session_url and self._is_upload_session_expired:
            raise RuntimeError("The upload session has expired.")
        if self._upload_session_url:
            await self.fs._msgraph_post(self._upload_session_url)
        self._reset_session_info()

    async def _commit(self):
        _logger.debug("Commit %s" % self)
        if self.tell() == 0:
            if self.buffer is not None:
                _logger.debug("Empty file committed %s" % self)
                await self._abort_upload_session()
                await self.fs._touch(self.path, **self.kwargs)
        elif not self._upload_session_url:
            if self.buffer is not None:
                _logger.debug("One-shot upload of %s" % self)
                self.buffer.seek(0)
                data = self.buffer.read()
                await self._upload_content_at_once(data)
            else:
                raise RuntimeError

        await self._commit_upload_session()
        # complex cache invalidation, since file's appearance can cause several
        # directories
        parts = self.path.split("/")
        path = parts[0]
        for p in parts[1:]:
            if path in self.fs.dircache and not [
                True for f in self.fs.dircache[path] if f["name"] == path + "/" + p
            ]:
                self.fs.invalidate_cache(path)
            path = path + "/" + p
        pass

    commit = sync_wrapper(_commit)

    async def _discard(self):
        await self._abort_upload_session()

    discard = sync_wrapper(_discard)

    ########################################################
    ## AbstractBufferedFile methods to implement or override
    ########################################################

    async def _upload_chunk(self, final=False):
        """Write one part of a multi-block file upload.

        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        if self.autocommit and final and self.tell() < self.blocksize:
            # only happens when closing small file, use on-shot PUT
            chunk_to_write = False
        else:
            self.buffer.seek(0)
            chunk_to_write = self.buffer.read(self.blocksize)
            if self._remaining_bytes:
                chunk_to_write = self._remaining_bytes
                self._remaining_bytes = None
                # complete the block
                chunk_to_write += self.buffer.read(self.blocksize - len(chunk_to_write))
        # we must write into chunk of the same block size. We therefore need to
        # buffer the remaining bytes if the buffer is not a multiple of the block size
        while chunk_to_write:
            chunk_size = len(chunk_to_write)
            if chunk_size < self.blocksize and not final:
                self._remaining_bytes = chunk_to_write
                break

            headers = {
                "Content-Length": str(chunk_size),
                "Content-Range": f"bytes {self._chunk_start_pos}-{self._chunk_start_pos + chunk_size - 1}/*",
            }
            response = await self.fs._msgraph_put(
                self._upload_session_url,
                content=chunk_to_write,
                headers=headers,
            )
            self._upload_expiration_dt = datetime.datetime.fromisoformat(
                response.json()["expirationDateTime"]
            )
            self._chunk_start_pos += chunk_size
            chunk_to_write = self.buffer.read(self.blocksize)

        if self.autocommit and final:
            await self._commit()
        return not final

    async def _initiate_upload(self):
        if self.autocommit and self.tell() < self.blocksize:
            # only happens when closing small file, use on-shot PUT
            return
        # If the file to be uploaded is larger than the block size, then we need to
        # create an upload session to upload the file in chunks.
        self._chunk_start_pos = 0
        self._upload_session_url, self._upload_expiration_dt = (
            await self._create_upload_session()
        )

    async def _fetch_range(self, start, end) -> bytes:
        """Get the specified set of bytes from remote."""
        item_id = await self.fs._get_item_id(self.path)
        return await self.fs._cat_file(self.path, start=start, end=end, item_id=item_id)

    @property
    def loop(self):
        return self.fs.loop


class SharepointBuffredFile(AsyncStreamedFileMixin, AbstractBufferedFile):
    """A file-like object representing a file in a SharePoint drive.

    Parameters
    ----------
    fs: SharepointFS
        The filesystem this file is part of.
    path: str
        The path to the file.
    mode: str
        The mode to open the file in.
        One of 'rb', 'wb', 'ab'. These have the same meaning
        as they do for the built-in `open` function.
    block_size: int
        Buffer size for reading or writing, 'default' for class default
    autocommit: bool
            Whether to write to final destination; may only impact what
            happens when file is being closed.
    cache_type: {"readahead", "none", "mmap", "bytes"}, default "readahead"
        Caching policy in read mode. See the definitions in ``core``.
    cache_options : dict
        Additional options passed to the constructor for the cache specified
        by `cache_type`.
    size: int
        If given and in read mode, suppressed having to look up the file size
    kwargs:
        Gets stored as self.kwargs
    """

    def __init__(
        self,
        fs: SharepointFS,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: dict | None = None,
        size: int | None = None,
        **kwargs,
    ):
        AbstractBufferedFile.__init__(
            self,
            fs,
            path,
            mode,
            block_size,
            autocommit,
            cache_type,
            cache_options,
            size,
            **kwargs,
        )
        kwargs_mixin = kwargs.copy()
        kwargs_mixin.update(
            {
                "fs": fs,
                "path": path,
                "mode": mode,
                "block_size": block_size,
                "autocommit": autocommit,
                "cache_type": cache_type,
                "cache_options": cache_options,
                "size": size,
            }
        )

        AsyncStreamedFileMixin._init__mixin(self, **kwargs_mixin)

    ########################################################
    ## AbstractBufferedFile methods to implement or override
    ########################################################
    _upload_chunk = sync_wrapper(AsyncStreamedFileMixin._upload_chunk)
    _initiate_upload = sync_wrapper(AsyncStreamedFileMixin._initiate_upload)
    _fetch_range = sync_wrapper(AsyncStreamedFileMixin._fetch_range)


class SharepointStreamedFile(AsyncStreamedFileMixin, AbstractAsyncStreamedFile):
    """A file-like object representing a file in a SharePoint drive.

    Parameters
    ----------
    fs: SharepointFS
        The filesystem this file is part of.
    path: str
        The path to the file.
    mode: str
        The mode to open the file in.
        One of 'rb', 'wb', 'ab'. These have the same meaning
        as they do for the built-in `open` function.
    block_size: int
        Buffer size for reading or writing, 'default' for class default
    autocommit: bool
            Whether to write to final destination; may only impact what
            happens when file is being closed.
    cache_type: {"readahead", "none", "mmap", "bytes"}, default "readahead"
        Caching policy in read mode. See the definitions in ``core``.
    cache_options : dict
        Additional options passed to the constructor for the cache specified
        by `cache_type`.
    size: int
        If given and in read mode, suppressed having to look up the file size
    kwargs:
        Gets stored as self.kwargs
    """

    def __init__(
        self,
        fs: SharepointFS,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: dict | None = None,
        size: int | None = None,
        **kwargs,
    ):
        AbstractAsyncStreamedFile.__init__(
            self,
            fs,
            path,
            mode,
            block_size,
            autocommit,
            cache_type,
            cache_options,
            size,
            **kwargs,
        )
        kwargs_mixin = kwargs.copy()
        kwargs_mixin.update(
            {
                "fs": fs,
                "path": path,
                "mode": mode,
                "block_size": block_size,
                "autocommit": autocommit,
                "cache_type": cache_type,
                "cache_options": cache_options,
                "size": size,
            }
        )

        AsyncStreamedFileMixin._init__mixin(self, **kwargs_mixin)
