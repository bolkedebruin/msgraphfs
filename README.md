# MSGraphFS

A [fsspec](https://filesystem-spec.readthedocs.io/) based filesystem for Microsoft Graph API drives (SharePoint, OneDrive). Features lazy initialization, fork-safety for multi-process environments like Airflow, and comprehensive permission management.

📖 [Microsoft Graph OneDrive API Documentation](https://learn.microsoft.com/en-us/graph/api/resources/onedrive?view=graph-rest-1.0)

## 🚀 Quick Start

### Simple Usage

```python
import msgraphfs

# Easy setup - just provide your app credentials and site/drive names
fs = msgraphfs.MSGDriveFS(
    client_id="your-client-id",
    tenant_id="your-tenant-id",
    client_secret="your-client-secret",
    site_name="YourSiteName",        # SharePoint site name
    drive_name="Documents"           # Optional: defaults to site's default drive
)

# Start using it like any filesystem
files = fs.ls("/")
print(f"Found {len(files)} items")

# Read files
with fs.open("/path/to/file.txt") as f:
    content = f.read()

# Write files
with fs.open("/path/to/new_file.txt", "w") as f:
    f.write("Hello SharePoint!")
```

### Using fsspec Protocol

```python
import fsspec

fs = fsspec.filesystem("msgd",
    client_id="your-client-id",
    tenant_id="your-tenant-id",
    client_secret="your-client-secret",
    site_name="YourSiteName",
    drive_name="Documents"
)

fs.ls("/")
```

### Environment Variables

You can also use environment variables:

```bash
export MSGRAPHFS_CLIENT_ID="your-client-id"
export MSGRAPHFS_TENANT_ID="your-tenant-id"
export MSGRAPHFS_CLIENT_SECRET="your-client-secret"
```

```python
import msgraphfs

# Credentials loaded from environment
fs = msgraphfs.MSGDriveFS(
    site_name="YourSiteName",
    drive_name="Documents"
)
```

## ✨ Key Features

### 🔧 **Automatic Discovery**
- **No manual drive/site ID lookup required** - just provide site and drive names
- **Automatic OAuth2 token management** - handles client credentials flow
- **Fork-safe lazy initialization** - perfect for multi-process environments like Airflow

### 🔐 **Permission Management**
```python
# Get detailed permissions for any file/directory
permissions = fs.get_permissions("/sensitive-document.pdf")

print(f"Total permissions: {permissions['summary']['total_permissions']}")
print(f"Users with access: {permissions['summary']['user_count']}")

# Check specific users and roles
for user in permissions['users']:
    print(f"{user['display_name']}: {', '.join(user['roles'])}")
```

### 📁 **Enhanced File Operations**
- **Expand queries**: Get additional metadata with `expand="permissions"` or `expand="thumbnails"`
- **Version control**: `get_versions()`, `checkin()`, `checkout()`
- **File preview**: `preview()` for web preview URLs
- **Format conversion**: `get_content(format="pdf")` to convert documents

### 🚀 **Airflow Integration**
```python
from airflow.io.path import ObjectStoragePath, attach
import msgraphfs

# Safe to do at module level - lazy initialization prevents fork issues
attach(protocol="sharepoint", fs=msgraphfs.MSGDriveFS(
    site_name="YourSite",
    drive_name="Documents",
    # credentials from environment or parameters
))

@task
def process_files():
    # Works perfectly in Airflow tasks
    src_path = ObjectStoragePath("sharepoint://folder/file.docx")
    content = src_path.read_text()
    return content
```

## 📋 Advanced Usage

### Working with Item IDs
Many methods accept an optional `item_id` parameter for efficiency:

```python
# Get item ID for later use
item_id = fs.get_item_id("/important/document.pdf")

# Use item_id to avoid path lookups
info = fs.info("/any/path", item_id=item_id)
content = fs.get_content(item_id=item_id)
permissions = fs.get_permissions(item_id=item_id)
```

### Document Conversion
```python
# Convert Word document to PDF
pdf_content = fs.get_content("/document.docx", format="pdf")

# Get file preview URL
preview_url = fs.preview("/presentation.pptx")
```

### Version Control
```python
# Check out for editing
fs.checkout("/document.docx")

# Make changes...
with fs.open("/document.docx", "w") as f:
    f.write("Updated content")

# Check back in with comment
fs.checkin("/document.docx", "Updated quarterly numbers")

# View version history
versions = fs.get_versions("/document.docx")
for version in versions:
    print(f"Version {version['id']}: {version['lastModifiedDateTime']}")
```

## 🔧 Installation

```bash
uv add msgraphfs
```

Or with pip:
```bash
pip install msgraphfs
```

## ⚙️ Setup Requirements

### Azure App Registration

1. **Register an Azure AD application** at https://portal.azure.com
2. **Configure API permissions** (Application permissions for client credentials flow):
   - `Sites.Read.All` or `Sites.ReadWrite.All`
   - `Files.Read.All` or `Files.ReadWrite.All`
   - ⚠️ **Important**: Grant admin consent for your organization
3. **Create a client secret**
4. **Note down**:
   - Application (client) ID
   - Directory (tenant) ID
   - Client secret value

### OAuth2 Scopes

MSGraphFS uses **client credentials flow** with the **default scope** (`https://graph.microsoft.com/.default`). This automatically includes all the application permissions you've granted to your Azure app registration.

**You don't need to specify individual scopes** - the library handles this automatically! 🎯

### SharePoint Site Access

- Ensure your Azure app has access to the SharePoint site
- You only need the **site name** and **drive name** (e.g., "Documents")
- No manual ID lookups required! 🎉

### Legacy Usage (Advanced)

If you prefer to specify drive IDs directly:

```python
fs = msgraphfs.MSGDriveFS(
    client_id="your-client-id",
    tenant_id="your-tenant-id",
    client_secret="your-client-secret",
    drive_id="specific-drive-id"  # Skip auto-discovery
)
```

Find drive IDs using [Microsoft Graph Explorer](https://developer.microsoft.com/en-us/graph/graph-explorer):
- Sites: `GET /sites/{hostname}:/sites/{site-name}`
- Drives: `GET /sites/{site-id}/drives`

## 🛠️ Development

To develop this package, you can clone the repository and install the dependencies using uv:

```bash
git clone your-repo-url (a fork of https://github.com/acsone/msgraphfs)
cd msgraphfs
uv sync
```

This will install the package in editable mode with all dependencies, so you can make changes to the code and test them without having to reinstall the package every time.

To run the tests with the test dependencies:

```bash
uv run pytest
```

Or with pip (legacy):
```bash
pip install -e .[test]
pytest
```

Testing the package requires you to have access to a Microsoft Drive (OneDrive, Sharepoint, etc) and to have the `client_id`, `client_secret`, `tenant_id`, `dirve_id`, `site_name` and the user's
access token.

### How to get an access token required for testing

The first step is to get your user's access token.


### Prerequisites

- A registered Azure AD application with:
  - `client_id` and `client_secret`
  - Delegated permissions granted (e.g., `Files.ReadWrite.All`, `Sites.ReadWrite.All`)
  - A redirect URI configured (e.g., `http://localhost:5000/callback`)


#### 1. Build the OAuth2 authorization URL

Open the following URL in your browser (replace values as needed):

```bash
https://login.microsoftonline.com/<TENANT_ID>/oauth2/v2.0/authorize?
client_id=<CLIENT_ID>
&response_type=code
&redirect_uri=http://localhost:5000/callback
&response_mode=query
&scope=offline_access%20User.Read%20Files.ReadWrite.All%20Sites.ReadWrite.All
```

You will be asked to log in with your Microsoft account and to grant the requested permissions.

#### 2. Copy the Authorization Code

Once logged in, you'll be redirected to:

```bash
http://localhost:5000/callback?code=<AUTHORIZATION_CODE>
```

Copy the value of `code` from the URL.


### Launch the test suite

To run the test suite, you just need to run the pytest command in the root directory with the following arguments:

* --auth-code: The authorization code you got in the previous step. (It's only required if you launch the tests for the first time or if your refresh token is expired and you need to get a new access token)
* --client-id: The client id of your Azure AD application.
* --client-secret: The client secret of your Azure AD application.
* --tenant-id: The tenant id of your Azure AD application.
* --drive-id: The drive id of the drive you want to access.
* --site-name: The name of the site you want to access. (Only required for tests related to the access to the recycling bin)

```bash
pytest --auth-code <AUTH_CODE> \
       --client-id <CLIENT_ID> \
       --client-secret <CLIENT_SECRET> \
       --tenant-id <TENANT_ID> \
       --drive-id <DRIVE_ID> \
       --site-name <SITE_NAME> \
       tests
```

Alternatively, you can set the environment variables `MSGRAPHFS_AUTH_CODE`, `MSGRAPHFS_CLIENT_ID`, `MSGRAPHFS_CLIENT_SECRET`, `MSGRAPHFS_TENANT_ID`, `MSGRAPHFS_DRIVE_ID` and `MSGRAPHFS_SITE_NAME` to avoid passing the arguments to pytest.

When the auth-code is provided and we need to get the access token (IOW when it's the first time you run the tests or when your refresh token is expired), the package will automatically get the access token and store it
in a encrypted file into the keyring of your system. The call to the token endpoint requires a `redirect_uri` parameter. This one should match one of the redirect URIs you configured in your Azure AD application.
By default, it is set to `http://localhost:8069/microsoft_account/authentication`, but you can change it by setting the environment variable `MSGRAPHFS_AUTH_REDIRECT_URI` or by passing the `--auth-redirect-uri` argument to pytest.

### Pre-commit hooks

To ensure code quality, this package uses pre-commit hooks. You can install them by running:

```bash
pre-commit install
```
This will set up the pre-commit hooks to run automatically before each commit. You can also run them manually by executing:

```bash
pre-commit run --all-files
```
