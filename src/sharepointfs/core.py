import asyncio
import datetime
import mimetypes
import httpx
import logging
from fsspec.asyn import (
    AsyncFileSystem,
    FSTimeoutError,
    sync,
    sync_wrapper,
    AbstractBufferedFile,
    AbstractAsyncStreamedFile,
)

from authlib.integrations.httpx_client import AsyncOAuth2Client


from httpx import HTTPError, Response
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
        except HTTPError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(f"File not found: {e}") from e
            raise e

    return wrapper


@wrap_http_not_found_exceptions
async def _http_call_with_retry(func, *, args=(), kwargs=None, retries) -> Response:
    kwargs = kwargs or {}
    for i in range(retries):
        try:
            response = func(*args, **kwargs)
            response.raise_for_status()
            return response
        except HTTPX_RETRYABLE_ERRORS as e:
            if i == retries - 1:
                raise e
            _logger.debug("Retryable error: %s", e)
            await asyncio.sleep(min(1.7**i * 0.1, 15))
            continue
        except HTTPError as e:
            if e.response.status_code in HTTPX_RETRYABLE_HTTP_STATUS_CODES:
                if i == retries - 1:
                    raise e
                _logger.debug("Retryable HTTP status code: %s", e.response.status_code)
                await asyncio.sleep(min(1.7**i * 0.1, 15))
                continue
            raise e


class SharepointFS(AsyncFileSystem):
    """A filesystem that represents a SharePoint site dirve as a filesystem.

    parameters:
    site_id (str): The ID of the SharePoint site.
    drive_id (str): The ID of the SharePoint drive.
    oauth2_client_params (dict): Parameters for the OAuth2 client to use for
        authentication. see https://docs.authlib.org/en/latest/client/api.html#authlib.integrations.httpx_client.AsyncOAuth2Client
    """

    protocol = "sharepoint"
    retries = 5
    blocksize = 10 * 1024 * 1024  # 10 MB

    def __init__(
        self,
        site_id: str,
        drive_id: str,
        oauth2_client_params: dict,
        **kwargs,
    ):
        self.site_id: str = site_id
        self.drive_id: str = drive_id
        super_kwargs = {
            k: kwargs.pop(k)
            for k in ["use_listings_cache", "listings_expiry_time", "max_paths"]
            if k in kwargs
        }  # passed to fsspec superclass... we don't support directory caching
        super().__init__(**super_kwargs)

        self.client: AsyncOAuth2Client = AsyncOAuth2Client(
            **oauth2_client_params,
            follow_redirects=True,
        )
        self.drive_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
        if not self.asynchronous:
            #  TO BE FIXED
            # weakref.finalize(self, self.close_http_session, self.client, self.loop)
            pass

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

    def _get_path(self, shpt_info: dict) -> str:
        parent_path = shpt_info["parentReference"].get("path")
        if not parent_path:
            return "/"
        # remove all the part before the "root:"
        parent_path = parent_path.split("root:")[1]
        return parent_path + "/" + shpt_info["name"]

    def _path_to_url(self, path, item_id=None, action=None):
        action = action and f"/{action}" if action else ""
        path = self._strip_protocol(path).rstrip("/")
        if path:
            path = f":{path}:"
        if item_id:
            return f"{self.drive_url}/items/{item_id}{action}"

        return f"{self.drive_url}/root{path}{action}"

    def _shpt_info_to_fsspec_info(self, shpt_info: dict) -> dict:
        _type = "other"
        if shpt_info.get("folder"):
            _type = "directory"
        elif shpt_info.get("file"):
            _type = "file"
        data = {
            "name": self._get_path(shpt_info),
            "size": shpt_info.get("size", 0),
            "type": _type,
            "sharepoint_info": shpt_info,
        }
        if _type == "file":
            data["mimetype"] = shpt_info.get("file", {}).get("mimeType", "")
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
            response = await self._ms_graph_get(url, params={"select": "id"})
            return response.json()["id"]
        except FileNotFoundError:
            if throw_on_missing:
                raise
            return None

    get_item_id = sync_wrapper(_get_item_id)

    @staticmethod
    def _guess_type(path: str) -> str:
        return mimetypes.guess_type(path)[0] or "application/octet-stream"

    ################################################
    # Helper methods to call the Microsoft Graph API
    ################################################
    async def _call_ms_graph(
        self, http_method: str, url: URLTypes, *args, **kwargs
    ) -> Response:
        """Call the Microsoft Graph API."""
        return await _http_call_with_retry(
            self.client.request,
            args=(http_method, url, *args),
            kwargs=kwargs,
            retries=self.retries,
        )

    call_ms_graph = sync_wrapper(_call_ms_graph)

    async def _ms_graph_get(self, url: URLTypes, *args, **kwargs) -> Response:
        """Send a GET request to the Microsoft Graph API."""
        return await self._call_ms_graph("GET", url, *args, **kwargs)

    ms_graph_get = sync_wrapper(_ms_graph_get)

    async def _ms_graph_post(self, url: URLTypes, *args, **kwargs) -> Response:
        """Send a POST request to the Microsoft Graph API."""
        return await self._call_ms_graph("POST", url, *args, **kwargs)

    ms_graph_post = sync_wrapper(_ms_graph_post)

    async def _ms_graph_put(self, url: URLTypes, *args, **kwargs) -> Response:
        """Send a PUT request to the Microsoft Graph API."""
        return await self._call_ms_graph("PUT", url, *args, **kwargs)

    ms_graph_put = sync_wrapper(_ms_graph_put)

    async def _ms_graph_delete(self, url: URLTypes, *args, **kwargs) -> Response:
        """Send a DELETE request to the Microsoft Graph API."""
        return await self._call_ms_graph("DELETE", url, *args, **kwargs)

    ms_graph_delete = sync_wrapper(_ms_graph_delete)

    async def _msg_graph_patch(self, url: URLTypes, *args, **kwargs) -> Response:
        """Send a PATCH request to the Microsoft Graph API."""
        return await self._call_ms_graph("PATCH", url, *args, **kwargs)

    ms_graph_patch = sync_wrapper(_msg_graph_patch)

    #############################################################
    # Implement required async methods
    #############################################################

    async def _exists(self, path, **kwargs):
        return await self._get_item_id(path) is not None

    async def _info(self, path, item_id=None, **kwargs):
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
        response = await self._ms_graph_get(url)
        return self._shpt_info_to_fsspec_info(response.json())

    async def _ls(self, path, detail=True, item_id=None, **kwargs) -> list[dict | str]:
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
        response = await self._ms_graph_get(url)
        items = response.json().get("value", [])
        if detail:
            return [self._shpt_info_to_fsspec_info(item) for item in items]
        else:
            return [self._get_path(item) for item in items]

    async def _cat_file(self, path, start=None, end=None, item_id=None, **kwargs):
        url = self._path_to_url(path, item_id=item_id, action="content")
        headers = kwargs.get("headers", {})
        if start is not None and end is not None:
            headers["Range"] = f"bytes={start}-{end - 1}"
        response = await self._ms_graph_get(url, headers=headers)
        return response.content

    async def _pipe_file(self, path, value, **kwargs):
        with self.open(path, "wb") as f:
            await f.write(value)

    async def _get_file(self, rpath, lpath, **kwargs):
        headers = kwargs.get("headers", {})
        content = await self._cat_file(rpath, **kwargs, headers=headers)
        with open(lpath, "wb") as f:
            f.write(content)

    async def _put_file(self, lpath, rpath, **kwargs):
        with open(lpath, "rb") as f:
            data = f.read()
        await self._pipe_file(rpath, data, **kwargs)
        while rpath:
            self.invalidate_cache(rpath)
            rpath = self._parent(rpath)

    async def _rm_file(self, path, item_id=None, **kwargs):
        item_id = item_id or await self._get_item_id(path, throw_on_missing=True)
        url = self._path_to_url(path, item_id=item_id)
        await self._ms_graph_delete(url)
        self.invalidate_cache(path)

    async def cp_file(self, path1, path2, **kwargs):
        source_item_id = await self._get_item_id(path1, throw_on_missing=True)
        url = self._path_to_url(path1, item_id=source_item_id, action="copy")
        path2 = self._strip_protocol(path2)
        parent_path, _file_name = path2.rsplit("/", 1)
        parent_id = await self._get_item_id(parent_path)
        json = {
            "parentReference": {"id": parent_id, "driveId": self.drive_id},
            "name": _file_name,
        }
        await self._ms_graph_post(url, json=json)

    # "_expand_path", keep generic implementation
    # "_blob", keep generic implementation
    # "_isfile",
    # "_isdir",
    # "_walk",
    # "_find",
    # "_du",
    # "_size",
    # "_mkdir",
    # "_makedirs",

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
        if item_id:
            url = self._path_to_url(path, item_id=item_id)
            if truncate:
                await self._ms_graph_put(
                    url, data=b"", headers={"Content-Type": "application/octet-stream"}
                )
            else:
                await self._ms_graph_patch(
                    url, json={"lastModifiedDateTime": datetime.now().isoformat()}
                )
        else:
            parent_path, file_name = path.rsplit("/", 1)
            parent_id = await self._get_item_id(parent_path)
            item_id = f"{parent_id}:/{file_name}:"
            url = self._path_to_url(path, item_id=item_id)
            headers = {"Content-Type": self._guess_type(path)}
            await self._ms_graph_put(url, data=b"", headers=headers)
        self.invalidate_cache(path)

    touch = sync_wrapper(_touch)


class AsyncStreamedFileMixin:
    """Mixin for streamed file-like objects using async iterators."""

    def __init__mixin(self, **kwargs):
        self.path = self.fs._strip_protocol(self.path)
        block_size = kwargs.get("block_size", "default")
        if block_size == "default":
            block_size = None
        self.blocksize = block_size if block_size is not None else self.fs.blocksize
        if "w" in self.mode or "b" in self.mode:
            # block_size must bet a multiple of 320 KiB
            if block_size % (320 * 1024) != 0:
                raise ValueError("block_size must be a multiple of 320 KiB")
            raise NotImplementedError("Write mode is not supported.")
        self._append_mode = "a" in self.mode and self.item_id is not None
        self._reset_session_info()

    @property
    def _item_id(self) -> str | None:
        """Get the item ID of the file into Sharepoint.

        Returns:
            str: The item ID of the file if it exists, otherwise None.
        """
        if not hasattr(self, "_item_id"):
            self._item_id = self.fs.get_item_id(self.path)
        return self._item_id

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
        response = await self.fs._ms_graph_post(
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
        self.buffer = None

    async def _upload_content_at_once(self, data):
        headers = self.kwargs.get("headers", {})
        if "content-type" not in headers:
            headers["content-type"] = self.fs._guess_type(self.path)
        item_id = await self._get_item_id(self.path)
        if not item_id:
            parent_path, file_name = self.path.rsplit("/", 1)
            parent_id = await self._get_item_id(parent_path)
            item_id = f"{parent_id}:/{file_name}:"
        url = self._path_to_url(self.path, item_id=item_id, action="content")
        await self._ms_graph_put(url, data=data, headers=headers)
        self.fs.invalidate_cache(self.path)

    async def _abort_upload_session(self):
        """Abort the current upload session."""
        if self._upload_session_url and not self._is_upload_session_expired:
            await self.fs._ms_graph_delete(self._upload_session_url)
        self._reset_session_info()

    async def _commit_upload_session(self):
        """Commit the current upload session."""
        if self._upload_session_url and self._is_upload_session_expired:
            raise RuntimeError("The upload session has expired.")
        if self._upload_session_url:
            await self.fs._ms_graph_post(self._upload_session_url)
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
            response = await self.fs._ms_graph_put(
                self._upload_session_url,
                data=chunk_to_write,
                headers=headers,
            )
            self._upload_expiration_dt = datetime.datetime.fromisoformat(
                response.json()["expirationDateTime"]
            )
            self._chunk_start_pos += chunk_size
            chunk_to_write = self.buffer.read(self.blocksize)

        if self.autocommit and final:
            await self._commit()
            return True
        return not final

    async def _aync_initiate_upload(self):
        if self.autocommit and self.tell() < self.blocksize:
            # only happens when closing small file, use on-shot PUT
            return
        # If the file to be uploaded is larger than the block size, then we need to
        # create an upload session to upload the file in chunks.
        self._chunk_start_pos = 0
        self._upload_session_url, self._upload_expiration_dt = (
            await self._create_upload_session()
        )

    async def fetch_range(self, start, end) -> bytes:
        """Get the specified set of bytes from remote."""
        return await self.fs._cat_file(
            self.path, start=start, end=end, item_id=self._item_id
        )


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

        AsyncStreamedFileMixin.__init__mixin(self, **kwargs_mixin)

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

        AsyncStreamedFileMixin.__init__mixin(self, **kwargs_mixin)
