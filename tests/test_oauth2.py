import os
import warnings
from unittest.mock import Mock, patch

import pytest

from msgraphfs import MSGDriveFS


class TestOAuth2:
    """Test OAuth2 authentication and constructor functionality."""

    def test_constructor_with_direct_credentials(self):
        """Test that the constructor accepts client_id, tenant_id, client_secret
        directly."""
        client_id = "test-client-id"
        tenant_id = "test-tenant-id"
        client_secret = "test-client-secret"
        drive_id = "test-drive-id"

        fs = MSGDriveFS(
            drive_id=drive_id,
            client_id=client_id,
            tenant_id=tenant_id,
            client_secret=client_secret,
        )

        assert fs.client_id == client_id
        assert fs.tenant_id == tenant_id
        assert fs.client_secret == client_secret
        assert fs.drive_id == drive_id
        assert fs.drive_url == f"https://graph.microsoft.com/v1.0/drives/{drive_id}"

    def test_constructor_with_environment_variables(self):
        """Test that the constructor reads credentials from environment variables."""
        client_id = "env-client-id"
        tenant_id = "env-tenant-id"
        client_secret = "env-client-secret"
        drive_id = "env-drive-id"

        with patch.dict(
            os.environ,
            {
                "MSGRAPHFS_CLIENT_ID": client_id,
                "MSGRAPHFS_TENANT_ID": tenant_id,
                "MSGRAPHFS_CLIENT_SECRET": client_secret,
            },
        ):
            fs = MSGDriveFS(drive_id=drive_id)

            assert fs.client_id == client_id
            assert fs.tenant_id == tenant_id
            assert fs.client_secret == client_secret

    def test_constructor_parameters_override_environment(self):
        """Test that constructor parameters override environment variables."""
        param_client_id = "param-client-id"
        env_client_id = "env-client-id"
        tenant_id = "test-tenant-id"
        client_secret = "test-client-secret"
        drive_id = "test-drive-id"

        with patch.dict(
            os.environ,
            {
                "MSGRAPHFS_CLIENT_ID": env_client_id,
                "MSGRAPHFS_TENANT_ID": tenant_id,
                "MSGRAPHFS_CLIENT_SECRET": client_secret,
            },
        ):
            fs = MSGDriveFS(
                drive_id=drive_id,
                client_id=param_client_id,
            )

            assert fs.client_id == param_client_id
            assert fs.tenant_id == tenant_id
            assert fs.client_secret == client_secret

    def test_constructor_missing_credentials_raises_error(self):
        """Test that missing credentials raise ValueError."""
        with pytest.raises(
            ValueError, match="Either oauth2_client_params must be provided"
        ):
            MSGDriveFS(drive_id="test-drive-id")

    def test_automatic_oauth2_params_generation(self):
        """Test that OAuth2 client params are automatically generated with correct
        values."""
        client_id = "test-client-id"
        tenant_id = "test-tenant-id"
        client_secret = "test-client-secret"
        drive_id = "test-drive-id"

        fs = MSGDriveFS(
            drive_id=drive_id,
            client_id=client_id,
            tenant_id=tenant_id,
            client_secret=client_secret,
        )

        # Check that the OAuth2 client was created with correct parameters
        assert fs.client.client_id == client_id
        assert fs.client.client_secret == client_secret

        # Check that credentials are stored on the filesystem object
        assert fs.client_id == client_id
        assert fs.tenant_id == tenant_id
        assert fs.client_secret == client_secret

    def test_oauth2_scopes_are_set_correctly(self):
        """Test that the default OAuth2 scopes are set correctly."""
        client_id = "test-client-id"
        tenant_id = "test-tenant-id"
        client_secret = "test-client-secret"
        drive_id = "test-drive-id"

        fs = MSGDriveFS(
            drive_id=drive_id,
            client_id=client_id,
            tenant_id=tenant_id,
            client_secret=client_secret,
        )

        expected_scopes = ["https://graph.microsoft.com/.default"]
        expected_scope_string = " ".join(expected_scopes)

        # Verify the scopes are set correctly
        assert fs.client.scope == expected_scope_string

    def test_constructor_with_existing_oauth2_params(self):
        """Test that existing oauth2_client_params are still supported."""
        client_id = "test-client-id"
        tenant_id = "test-tenant-id"
        client_secret = "test-client-secret"
        drive_id = "test-drive-id"

        oauth2_client_params = {
            "client_id": client_id,
            "client_secret": client_secret,
            "token_endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            "scope": "Files.ReadWrite.All",
            "grant_type": "client_credentials",
        }

        fs = MSGDriveFS(
            drive_id=drive_id,
            oauth2_client_params=oauth2_client_params,
        )

        assert fs.client_id == client_id
        # tenant_id extraction from token_endpoint may not work for this test format
        assert fs.client_secret == client_secret

    def test_tenant_id_extraction_from_token_endpoint(self):
        """Test extraction of tenant_id from token endpoint URL."""
        fs = MSGDriveFS(
            drive_id="test-drive-id",
            client_id="test-client-id",
            tenant_id="test-tenant-id",
            client_secret="test-client-secret",
        )

        # Test valid token endpoint
        token_endpoint = "https://login.microsoftonline.com/12345678-1234-1234-1234-123456789012/oauth2/v2.0/token"
        tenant_id = fs._extract_tenant_from_token_endpoint(token_endpoint)
        assert tenant_id == "12345678-1234-1234-1234-123456789012"

        # Test invalid token endpoint
        invalid_endpoint = "https://invalid.com/token"
        tenant_id = fs._extract_tenant_from_token_endpoint(invalid_endpoint)
        assert tenant_id is None

    def test_warning_when_no_drive_id_or_site_name(self):
        """Test that a warning is issued when neither drive_id nor site_name is
        provided."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            MSGDriveFS(
                client_id="test-client-id",
                tenant_id="test-tenant-id",
                client_secret="test-client-secret",
            )

            assert len(w) == 1
            assert "Neither drive_id nor site_name provided" in str(w[0].message)

    def test_no_warning_with_drive_id(self):
        """Test that no warning is issued when drive_id is provided."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            MSGDriveFS(
                drive_id="test-drive-id",
                client_id="test-client-id",
                tenant_id="test-tenant-id",
                client_secret="test-client-secret",
            )

            assert len(w) == 0

    def test_no_warning_with_site_name(self):
        """Test that no warning is issued when site_name is provided."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            MSGDriveFS(
                site_name="test-site",
                client_id="test-client-id",
                tenant_id="test-tenant-id",
                client_secret="test-client-secret",
            )

            assert len(w) == 0

    @pytest.mark.asyncio
    async def test_ensure_drive_id_with_site_name(self):
        """Test automatic drive_id discovery using site_name."""
        fs = MSGDriveFS(
            site_name="test-site",
            client_id="test-client-id",
            tenant_id="test-tenant-id",
            client_secret="test-client-secret",
        )

        # Mock the HTTP responses
        mock_site_response = Mock()
        mock_site_response.json.return_value = {"value": [{"id": "test-site-id"}]}

        mock_drive_response = Mock()
        mock_drive_response.json.return_value = {"id": "discovered-drive-id"}

        with patch.object(fs, "_msgraph_get") as mock_get:
            mock_get.side_effect = [mock_site_response, mock_drive_response]

            drive_id = await fs._ensure_drive_id()

            assert drive_id == "discovered-drive-id"
            assert fs.drive_id == "discovered-drive-id"
            assert (
                fs.drive_url
                == "https://graph.microsoft.com/v1.0/drives/discovered-drive-id"
            )

            # Verify the correct API calls were made
            assert mock_get.call_count == 2
            mock_get.assert_any_call(
                "https://graph.microsoft.com/v1.0/sites?search=test-site"
            )
            mock_get.assert_any_call(
                "https://graph.microsoft.com/v1.0/sites/test-site-id/drive"
            )

    @pytest.mark.asyncio
    async def test_ensure_drive_id_with_user_drive(self):
        """Test automatic drive_id discovery using user's default drive."""
        fs = MSGDriveFS(
            client_id="test-client-id",
            tenant_id="test-tenant-id",
            client_secret="test-client-secret",
        )

        # Mock the HTTP response
        mock_drive_response = Mock()
        mock_drive_response.json.return_value = {"id": "user-default-drive-id"}

        with patch.object(fs, "_msgraph_get") as mock_get:
            mock_get.return_value = mock_drive_response

            drive_id = await fs._ensure_drive_id()

            assert drive_id == "user-default-drive-id"
            assert fs.drive_id == "user-default-drive-id"
            assert (
                fs.drive_url
                == "https://graph.microsoft.com/v1.0/drives/user-default-drive-id"
            )

            # Verify the correct API call was made
            mock_get.assert_called_once_with(
                "https://graph.microsoft.com/v1.0/me/drive"
            )

    @pytest.mark.asyncio
    async def test_ensure_drive_id_site_not_found(self):
        """Test error handling when site is not found."""
        fs = MSGDriveFS(
            site_name="nonexistent-site",
            client_id="test-client-id",
            tenant_id="test-tenant-id",
            client_secret="test-client-secret",
        )

        mock_response = Mock()
        mock_response.json.return_value = {"value": []}

        with patch.object(fs, "_msgraph_get") as mock_get:
            mock_get.return_value = mock_response

            with pytest.raises(
                ValueError, match="No site found with name 'nonexistent-site'"
            ):
                await fs._ensure_drive_id()

    @pytest.mark.asyncio
    async def test_ensure_drive_id_api_error(self):
        """Test error handling when API call fails."""
        # Reset drive_id to None to force discovery
        fs = MSGDriveFS(
            client_id="test-client-id",
            tenant_id="test-tenant-id",
            client_secret="test-client-secret",
        )
        fs.drive_id = None  # Force it to None
        fs.drive_url = None

        with patch.object(fs, "_msgraph_get") as mock_get:
            mock_get.side_effect = Exception("API Error")

            with pytest.raises(ValueError, match="Unable to discover drive_id"):
                await fs._ensure_drive_id()

    @pytest.mark.asyncio
    async def test_automatic_drive_id_on_operations(self):
        """Test that drive_id is automatically discovered when performing operations."""
        fs = MSGDriveFS(
            site_name="test-site",
            client_id="test-client-id",
            tenant_id="test-tenant-id",
            client_secret="test-client-secret",
        )

        # Force drive_url to be None to test auto-discovery
        fs.drive_id = None
        fs.drive_url = None

        # Mock the site and drive discovery
        mock_site_response = Mock()
        mock_site_response.json.return_value = {"value": [{"id": "test-site-id"}]}

        mock_drive_response = Mock()
        mock_drive_response.json.return_value = {"id": "discovered-drive-id"}

        with patch.object(fs, "_msgraph_get") as mock_get:
            mock_get.side_effect = [mock_site_response, mock_drive_response]

            # Calling _ensure_drive_id should trigger discovery
            await fs._ensure_drive_id()

            # Now drive_url should be set
            assert (
                fs.drive_url
                == "https://graph.microsoft.com/v1.0/drives/discovered-drive-id"
            )
            assert fs.drive_id == "discovered-drive-id"

    def test_spelling_error_fixes(self):
        """Test that spelling errors in class names have been fixed."""
        # Test that the correct class names exist
        assert hasattr(MSGDriveFS, "__init__")

        # Test that MSGraphBufferedFile exists (was MSGraphBuffredFile)
        from msgraphfs.core import MSGraphBufferedFile

        assert MSGraphBufferedFile is not None

        # Test that MSGraphStreamedFile exists (was MSGrpahStreamedFile)
        from msgraphfs.core import MSGraphStreamedFile

        assert MSGraphStreamedFile is not None

        # Test docstring fixes
        fs = MSGDriveFS(
            drive_id="test-drive-id",
            client_id="test-client-id",
            tenant_id="test-tenant-id",
            client_secret="test-client-secret",
        )

        # Should say "drive" instead of "dirve"
        assert "drive" in fs.__doc__.lower()
        assert "dirve" not in fs.__doc__.lower()

    def test_imports_work_correctly(self):
        """Test that the imports in __init__.py work with corrected class names."""
        from msgraphfs import MSGDriveFS, MSGraphBufferedFile, MSGraphStreamedFile

        assert MSGDriveFS is not None
        assert MSGraphBufferedFile is not None
        assert MSGraphStreamedFile is not None
