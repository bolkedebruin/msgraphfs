import time
import uuid
import fsspec
from fsspec.implementations.dirfs import DirFileSystem
import pytest
import pytest_asyncio
import os
from msgraphfs import SharepointFS
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
        "--site-id", action="store", default=None, help="SharePoint site ID"
    )
    parser.addoption(
        "--drive-id", action="store", default=None, help="SharePoint drive ID"
    )
    parser.addoption(
        "--tenant-id", action="store", default=None, help="SharePoint drive ID"
    )


def _create_fs(request, fs_type, asynchronous=False) -> fsspec.AbstractFileSystem:
    if fs_type == "sharepoint":
        # Read configuration from command line arguments or environment variables
        # TODO
        client_id = request.config.getoption("--client-id") or os.getenv("CLIENT_ID")
        client_secret = request.config.getoption("--client-secret") or os.getenv(
            "CLIENT_SECRET"
        )
        site_id = request.config.getoption("--site-id") or os.getenv("SITE_ID")
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

            tenant_id, site_id, drive_id, client_id, client_secret = (
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
            print("refresh:", content)
            initial_token.update(token)
            return resp

        # Initialize SharePointFS with the configuration
        kwargs = {
            "site_id": site_id,
            "drive_id": drive_id,
            "oauth2_client_params": oauth2_client_params,
            "asynchronous": asynchronous,
        }
        if asynchronous:
            kwargs["cache_type"] = "none"
        sp_fs = SharepointFS(**kwargs)

        sp_fs.client.register_compliance_hook(
            "refresh_token_response",
            partial(refresh_token_response, initial_token=token),
        )

        # Yield the initialized object
        return sp_fs


@pytest.fixture(scope="session", params=["sharepoint"])
def fs(request):
    # we use a fixture to be able to lanch the tests suite with different
    # filesystems supported by the microsoft graph api
    yield _create_fs(request, request.param, asynchronous=False)


@pytest.fixture(scope="session", params=["sharepoint"])
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


@pytest_asyncio.fixture(scope="module")
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
                # TODO does not work with async open
                stream_file = await sfs.fs.open_async(sfs._join(path), "wb")
                try:
                    await stream_file.write(data)
                finally:
                    await stream_file.close()
        await sfs._makedirs("/emptydir")
        yield sfs


@pytest.fixture(scope="module")
def temp_fs(fs):
    """A temporary empty filesystem.

    We use the fsspec dir filesystem to interact with the filesystem to
    test so we can use a temporary directory into the tested filesystem
    as root to avoid polluting the real filesystem and ensure isolation
    between tests.
    """
    with _temp_dir(fs) as temp_dir_name:
        yield MsGraphTempFS("dir", path=temp_dir_name, fs=fs)


@pytest_asyncio.fixture(scope="module")
def temp_afs(afs):
    """A temporary empty async filesystem.

    We use the fsspec dir filesystem to interact with the filesystem to
    test so we can use a temporary directory into the tested filesystem
    as root to avoid polluting the real filesystem and ensure isolation
    between tests.
    """
    with _a_temp_dir(afs) as temp_dir_name:
        yield MsGraphTempFS("dir", path=temp_dir_name, fs=afs)
