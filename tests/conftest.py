import uuid
from fsspec.implementations.dirfs import DirFileSystem
import pytest
import os
from msgraphfs import SharepointFS
from contextlib import contextmanager
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


@pytest.fixture(scope="session", params=["sharepoint"])
def fs(request):
    if request.param == "sharepoint":
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
            "expires_in": 10,
        }

        # TO BE REMOVED
        try:
            from . import private

            tenant_id, site_id, drive_id, client_id, client_secret, token = (
                private.get_oauth2_client_params()
            )
        except ImportError:
            pass

        oauth2_client_params = {
            "scope": "offline_access openid Files.ReadWrite.All Sites.ReadWrite.All",
            "client_id": client_id,
            "client_secret": client_secret,
            "token": token,
            "token_endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        }

        def refresh_token_response(resp):
            content = resp.json()
            print("refresh:", content)
            token["access_token"] = content["access_token"]
            token["refresh_token"] = content["refresh_token"]
            token["expires_in"] = content["expires_in"]

        # Initialize SharePointFS with the configuration
        sp_fs = SharepointFS(
            site_id=site_id,
            drive_id=drive_id,
            oauth2_client_params=oauth2_client_params,
        )

        sp_fs.client.register_compliance_hook(
            "refresh_token_response", refresh_token_response
        )

        # Yield the initialized object
        yield sp_fs


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


@pytest.fixture
def temp_dir(fs):
    # create a temporary directory name
    temp_dir_name = str(uuid.uuid4())
    fs.mkdir(temp_dir_name)

    yield temp_dir_name

    # cleanup
    fs.rmdir(temp_dir_name)


@contextmanager
def _temp_dir(storagefs):
    # create a temporary directory name
    temp_dir_name = f"/{str(uuid.uuid4())}"
    storagefs.mkdir(temp_dir_name)
    try:

        yield temp_dir_name
    finally:
        # cleanup
        storagefs.rm(temp_dir_name, recursive=True)


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
