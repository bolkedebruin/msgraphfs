import time
import uuid
import fsspec
from fsspec.implementations.dirfs import DirFileSystem
import pytest
import pytest_asyncio
import os
from msgraphfs import MSGDriveFS
from functools import partial
from contextlib import contextmanager, asynccontextmanager
from . import content


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--client-id", action="store", default=None, help="SharePoint client ID"
    )
    parser.addoption(
        "--client-secret", action="store", default=None, help="SharePoint client secret"
    )
    parser.addoption(
        "--site-name", action="store", default=None, help="SharePoint site name"
    )
    parser.addoption(
        "--drive-id", action="store", default=None, help="SharePoint drive ID"
    )
    parser.addoption(
        "--tenant-id", action="store", default=None, help="SharePoint drive ID"
    )


def _create_fs(request, fs_type, asynchronous=False) -> fsspec.AbstractFileSystem:
    if fs_type == "msgdrive":
        # Read configuration from command line arguments or environment variables
        # TODO
        client_id = request.config.getoption("--client-id") or os.getenv("CLIENT_ID")
        client_secret = request.config.getoption("--client-secret") or os.getenv(
            "CLIENT_SECRET"
        )
        site_name = request.config.getoption("--site-name") or os.getenv("SITE_NAME")
        drive_id = request.config.getoption("--drive-id") or os.getenv("DRIVE_ID")
        tenant_id = request.config.getoption("--tenant-id") or os.getenv("TENANT_ID")
        token = {
            "access_token": None,
            "refresh_token": None,
            "expires_at": 10,
        }

        # TO BE REMOVED
        try:
            from . import private

            tenant_id, site_name, drive_id, client_id, client_secret = (
                private.get_oauth2_client_params()
            )
        except ImportError:
            pass
        # for the token we read the content of the token.json file
        # if it exists
        try:
            import json

            with open("token.json") as f:
                token = json.load(f)
        except FileNotFoundError:
            pass

        # END TO BE REMOVED

        oauth2_client_params = {
            "scope": "offline_access openid Files.ReadWrite.All Sites.ReadWrite.All",
            "client_id": client_id,
            "client_secret": client_secret,
            "token": token,
            "token_endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            "timeout": 10.0,
        }

        def refresh_token_response(resp, initial_token):
            token = resp.json()
            token["expires_at"] = int(token["expires_in"]) + int(time.time())
            # only keep the access token, the refresh token and the expire at time
            token = {
                k: v
                for k, v in token.items()
                if k in ["access_token", "refresh_token", "expires_at"]
            }
            with open("token.json", "w") as f:
                json.dump(token, f, indent=4)
            initial_token.update(token)
            return resp

        # Initialize SharePointFS with the configuration
        kwargs = {
            "site_name": site_name,
            "drive_id": drive_id,
            "oauth2_client_params": oauth2_client_params,
            "asynchronous": asynchronous,
        }
        if asynchronous:
            kwargs["cache_type"] = "none"
        MSGDriveFS.clear_instance_cache()
        sp_fs = MSGDriveFS(**kwargs)

        sp_fs.client.register_compliance_hook(
            "refresh_token_response",
            partial(refresh_token_response, initial_token=token),
        )

        # Yield the initialized object
        return sp_fs


FS_TYPES = ["msgdrive"]


@pytest.fixture(scope="module", params=FS_TYPES)
def fs(request):
    # we use a fixture to be able to lanch the tests suite with different
    # filesystems supported by the microsoft graph api
    yield _create_fs(request, request.param, asynchronous=False)


@pytest.fixture(scope="module", params=FS_TYPES)
def afs(request):
    # we use a fixture to be able to lanch the tests suite with different
    # filesystems supported by the microsoft graph api
    yield _create_fs(request, request.param, asynchronous=True)


class MsGraphTempFS(DirFileSystem):
    def _relpath(self, path):
        path = super()._relpath(path)
        # we override the DirFileSystem method since all
        # paths returned are relative to the root. We want
        # to return path as if they are absolute
        if isinstance(path, str):
            if not path.startswith("/"):
                path = "/" + path
        return path

    def _join(self, path):
        # we override the DirFileSystem method since all the path are absolute
        if isinstance(path, str):
            if path.startswith("/"):
                path = path[1:]
        return super()._join(path)

    # add missing mapper to async methods
    async def _rmdir(self, path):
        return await self.fs._rmdir(self._join(path))

    async def _touch(self, path, **kwargs):
        return await self.fs._touch(self._join(path), **kwargs)

    async def _move(self, path1, path2, **kwargs):
        return await self.fs._mv(self._join(path1), self._join(path2), **kwargs)

    async def _open_async(self, path, mode, **kwargs):
        return await self.fs.open_async(self._join(path), mode, **kwargs)

    async def _modified(self, path):
        return await self.fs._modified(self._join(path))

    async def _created(self, path):
        return await self.fs._created(self._join(path))


@contextmanager
def _temp_dir(storagefs):
    # create a temporary directory
    temp_dir_name = f"/{str(uuid.uuid4())}"
    storagefs.mkdir(temp_dir_name)
    try:

        yield temp_dir_name
    finally:
        # cleanup
        storagefs.rm(temp_dir_name, recursive=True)


@asynccontextmanager
async def _a_temp_dir(storagefs):
    # create a temporary directory in async fs
    temp_dir_name = f"/{str(uuid.uuid4())}"
    await storagefs._mkdir(temp_dir_name)
    try:

        yield temp_dir_name
    finally:
        # cleanup
        await storagefs._rm(temp_dir_name, recursive=True)


@pytest.fixture(scope="module")
def sample_fs(fs):
    """A temporary filesystem with sample files and directories created from the content
    module.

    We use the fsspec dir filesystem to interact with the filesystem to
    test so we can use a temporary directory into the tested filesystem
    as root to avoid polluting the real filesystem and ensure isolation
    between tests.
    """
    with _temp_dir(fs) as temp_dir_name:
        sfs = MsGraphTempFS(path=temp_dir_name, fs=fs)
        for flist in [
            content.files,
            content.csv_files,
            content.text_files,
            content.glob_files,
        ]:
            for path, data in flist.items():
                root, _filename = os.path.split(path)
                if root:
                    sfs.makedirs(root, exist_ok=True)
                with sfs.open(path, "wb") as f:
                    f.write(data)
        sfs.makedirs("/emptydir")
        yield sfs


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def sample_afs(afs):
    """A temporary async filesystem with sample files and directories created from the
    content module.

    We use the fsspec dir filesystem to interact with the filesystem to
    test so we can use a temporary directory into the tested filesystem
    as root to avoid polluting the real filesystem and ensure isolation
    between tests.
    """
    async with _a_temp_dir(afs) as temp_dir_name:
        sfs = MsGraphTempFS(path=temp_dir_name, asynchronous=True, fs=afs)
        for flist in [
            content.files,
            content.csv_files,
            content.text_files,
            content.glob_files,
        ]:
            for path, data in flist.items():
                root, _filename = os.path.split(path)
                if root:
                    await sfs._makedirs(root, exist_ok=True)
                async with await sfs._open_async(path, "wb") as stream_file:
                    await stream_file.write(data)
        await sfs._makedirs("/emptydir")
        yield sfs


@pytest.fixture(scope="function")
def temp_fs(fs):
    """A temporary empty filesystem.

    We use the fsspec dir filesystem to interact with the filesystem to
    test so we can use a temporary directory into the tested filesystem
    as root to avoid polluting the real filesystem and ensure isolation
    between tests.
    """
    with _temp_dir(fs) as temp_dir_name:
        yield MsGraphTempFS(path=temp_dir_name, fs=fs)


@pytest_asyncio.fixture(scope="function", params=FS_TYPES, loop_scope="function")
async def function_afs(request):
    # we use a fixture to be able to lanch the tests suite with different
    # filesystems supported by the microsoft graph api
    yield _create_fs(request, request.param, asynchronous=True)


@pytest_asyncio.fixture(scope="function", loop_scope="function")
async def temp_afs(function_afs):
    """A temporary empty async filesystem.

    We use the fsspec dir filesystem to interact with the filesystem to
    test so we can use a temporary directory into the tested filesystem
    as root to avoid polluting the real filesystem and ensure isolation
    between tests.
    """
    afs = function_afs
    async with _a_temp_dir(afs) as temp_dir_name:
        yield MsGraphTempFS(path=temp_dir_name, asynchronous=True, fs=afs)


@pytest.fixture(scope="function")
def temp_nested_fs(fs):
    """A temporary empty filesystem with nested directories.

    We use the fsspec dir filesystem to interact with the filesystem to
    test so we can use a temporary directory into the tested filesystem
    as root to avoid polluting the real filesystem and ensure isolation
    between tests.
    """
    with _temp_dir(fs) as temp_dir_name:
        sfs = MsGraphTempFS(path=temp_dir_name, fs=fs)
        for path, data in content.text_files.items():
            root, _filename = os.path.split(path)
            if root:
                sfs.makedirs(root, exist_ok=True)
            with sfs.open(path, "wb") as f:
                f.write(data)
            sfs.touch("/emptyfile")
        yield sfs


@pytest_asyncio.fixture(scope="function", loop_scope="function")
async def temp_nested_afs(function_afs):
    """A temporary empty async filesystem with nested directories.

    We use the fsspec dir filesystem to interact with the filesystem to
    test so we can use a temporary directory into the tested filesystem
    as root to avoid polluting the real filesystem and ensure isolation
    between tests.
    """
    afs = function_afs
    # is created within an other loop scope
    async with _a_temp_dir(afs) as temp_dir_name:
        sfs = MsGraphTempFS(path=temp_dir_name, asynchronous=True, fs=afs)
        for path, data in content.text_files.items():
            root, _filename = os.path.split(path)
            if root:
                await sfs._makedirs(root, exist_ok=True)
            async with await sfs._open_async(path, "wb") as stream_file:
                await stream_file.write(data)
            await sfs.fs._touch(sfs._join("/emptyfile"))
        yield sfs
