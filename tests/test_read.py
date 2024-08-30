import pytest
import datetime
from . import content


def test_ls(sample_fs):
    fs = sample_fs
    assert fs.ls("/", False) == [
        "/csv",
        "/emptydir",
        "/nested",
        "/test",
        "/file.dat",
        "/filexdat",
    ]
    assert fs.ls("/test", False) == ["/test/accounts.1.json", "/test/accounts.2.json"]
    assert fs.ls("/test/accounts.1.json", False) == ["/test/accounts.1.json"]
    assert fs.ls("/nested", False) == [
        "/nested/nested2",
        "/nested/file1",
        "/nested/file2",
    ]
    assert fs.ls("/nested/nested2", False) == [
        "/nested/nested2/file1",
        "/nested/nested2/file2",
    ]
    assert fs.ls("/file.dat", False) == ["/file.dat"]
    assert fs.ls("/emptydir", False) == []


def test_ls_detail(sample_fs):
    fs = sample_fs
    assert fs.ls("/", True)[0]["type"] == "directory"
    assert fs.ls("/test/accounts.1.json", True)[0]["type"] == "file"
    assert fs.ls("file.dat", True)[0]["type"] == "file"


@pytest.mark.parametrize(
    "path, expected_type, expected_size, expected_name, expected_mimetype",
    [
        ("/", "directory", 0, "/", None),
        ("/test", "directory", 0, "/test", None),
        (
            "/test/accounts.1.json",
            "file",
            len(content.files["test/accounts.1.json"]),
            "/test/accounts.1.json",
            "application/json",
        ),
    ],
)
def test_info(
    sample_fs, path, expected_type, expected_size, expected_name, expected_mimetype
):
    fs = sample_fs
    file_info = fs.info(path)
    assert file_info["type"] == expected_type
    if expected_type == "file":
        # size for directories is not computed synchronously
        assert file_info["size"] == expected_size
    assert (
        file_info["name"] == fs.path + expected_name
        if expected_name != "/"
        else fs.path
    )
    if expected_type == "file":
        assert file_info["mimetype"] == expected_mimetype

    # date are today
    assert (
        file_info["mtime"] is not None
        and file_info["mtime"].date() == datetime.datetime.now().date()
    )
    assert (
        file_info["time"] is not None
        and file_info["time"].date() == datetime.datetime.now().date()
    )
