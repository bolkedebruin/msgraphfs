import fsspec

from .core import (
    MSGDriveFS,
    MSGraphBufferedFile,
    MSGraphFileSystem,
    MSGraphStreamedFile,
    msgraph_filesystem_factory,
    parse_msgraph_url,
)

# Register MSGraphFileSystem for all supported protocols
# Use clobber=True to allow re-registration
fsspec.register_implementation("msgd", MSGraphFileSystem, clobber=True)
fsspec.register_implementation("sharepoint", MSGraphFileSystem, clobber=True)
fsspec.register_implementation("onedrive", MSGraphFileSystem, clobber=True)
