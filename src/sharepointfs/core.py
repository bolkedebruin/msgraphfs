import weakref
import asyncio
from fsspec.asyn import (
    AsyncFileSystem,
    FSTimeoutError,
    sync,
    AbstractAsyncStreamedFile,
)

from authlib.integrations.httpx_client import AsyncOAuth2Client


from httpx import HTTPError


def wrap_http_not_found_exceptions(func):
    """Wrap a function that calls an HTTP request to handle 404 errors."""

    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(f"File not found: {e}")
            raise e

    return wrapper


class SharepointFS(AsyncFileSystem):
    """A filesystem that represents a SharePoint site dirve as a filesystem.

    parameters:
    site_id (str): The ID of the SharePoint site.
    drive_id (str): The ID of the SharePoint drive.
    oauth2_client_params (dict): Parameters for the OAuth2 client to use for
        authentication. see https://docs.authlib.org/en/latest/client/api.html#authlib.integrations.httpx_client.AsyncOAuth2Client
    """

    protocol = "sharepoint"

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

    @wrap_http_not_found_exceptions
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
        path = self._strip_protocol(path).rstrip("/")
        if item_id:
            url = f"{self.drive_url}/items/{item_id}"
        elif path:
            url = f"{self.drive_url}/root:/{path}"
        else:
            url = self.drive_url + "/root"
        response = await self.client.get(url)
        response.raise_for_status()
        return self._shpt_info_to_fsspec_info(response.json())

    @wrap_http_not_found_exceptions
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
        path = self._strip_protocol(path).rstrip("/")
        if item_id:
            url = f"{self.drive_url}/items/{item_id}/children"
        elif path:
            url = f"{self.drive_url}/root:{path}:/children"
        else:
            url = self.drive_url + "/root/children"
        response = await self.client.get(url)
        response.raise_for_status()
        items = response.json().get("value", [])
        if detail:
            return [self._shpt_info_to_fsspec_info(item) for item in items]
        else:
            return [self._get_path(item) for item in items]

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        url = f"{self.drive_url}:{path}:/content"
        headers = {}
        if start is not None and end is not None:
            headers["Range"] = f"bytes={start}-{end - 1}"
        response = await self.client.get(url, headers=headers)
        response.raise_for_status()
        return response.content

    async def _pipe_file(self, path, value, **kwargs):
        url = f"{self.drive_url}:{path}:/content"
        response = await self.client.put(url, data=value)
        response.raise_for_status()

    async def _get_file(self, rpath, lpath, **kwargs):
        content = await self._cat_file(rpath, **kwargs)
        with open(lpath, "wb") as f:
            f.write(content)

    async def _put_file(self, lpath, rpath, **kwargs):
        with open(lpath, "rb") as f:
            data = f.read()
        await self._pipe_file(rpath, data, **kwargs)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        acl=False,
        version_id=None,
        fill_cache=None,
        cache_type=None,
        autocommit=True,
        size=None,
        requester_pays=None,
        cache_options=None,
        **kwargs,
    ):
        """Open a file for reading or writing

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
        version_id : str
            Explicit version of the object to open.  This requires that the s3
            filesystem is version aware and bucket versioning is enabled on the
            relevant bucket.
        cache_type : str
            See fsspec's documentation for available cache_type values. Set to "none"
            if no caching is desired. If None, defaults to ``self.default_cache_type``.
        kwargs: dict-like
            Additional parameters used for s3 methods.  Typically used for
            ServerSideEncryption.
        """
        if block_size is None:
            block_size = self.default_block_size
        if fill_cache is None:
            fill_cache = self.default_fill_cache
        if requester_pays is None:
            requester_pays = bool(self.req_kw)

        acl = (
            acl
            or self.s3_additional_kwargs.get("ACL", False)
            or self.s3_additional_kwargs.get("acl", False)
        )
        kw = self.s3_additional_kwargs.copy()
        kw.update(kwargs)
        if not self.version_aware and version_id:
            raise ValueError(
                "version_id cannot be specified if the filesystem "
                "is not version aware"
            )

        if cache_type is None:
            cache_type = self.default_cache_type
