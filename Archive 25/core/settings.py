from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import AliasChoices, Field
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = BASE_DIR / ".env"

class Settings(BaseSettings):
    APP_ENV: str = "local"

    # Database
    DATABASE_URL: Optional[str] = None
    NEON_DB_URL: str = "postgresql://neondb_owner:npg_EyGsgV7kAKC4@ep-dark-morning-aqz49q4z-pooler.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require"

    # Azure Storage Settings (replaces AWS S3)
    AZURE_STORAGE_ACCOUNT: Optional[str] = None
    AZURE_STORAGE_CONNECTION_STRING: Optional[str] = None
    AZURE_CONTAINER_NAME: Optional[str] = "datalake"        # main container (Raw/Bronze/Silver layers)
    ADLS_CONTAINER_NAME: Optional[str] = "landing"          # source landing container
    ADLS_ROOT_FOLDER: Optional[str] = None                   # virtual root prefix inside ADLS container
    AZURE_TENANT_ID: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("AZURE_TENANT_ID", "TENANT_ID"),
    )
    AZURE_CLIENT_ID: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("AZURE_CLIENT_ID", "CLIENT_ID"),
    )                   # Service Principal client id (optional)
    AZURE_CLIENT_SECRET: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("AZURE_CLIENT_SECRET", "CLIENT_SECRET"),
    )               # Service Principal secret (optional)
    AZURE_AUTHORITY: Optional[str] = None
    AZURE_REDIRECT_URI: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("AZURE_REDIRECT_URI", "REDIRECT_URI", "azure_redirect_uri"),
    )
    AZURE_SSO_SCOPES_AZURE: Optional[str] = "https://management.azure.com/user_impersonation"
    AZURE_SSO_SCOPES_FABRIC: Optional[str] = (
        "https://api.fabric.microsoft.com/Workspace.ReadWrite.All,"
        "https://api.fabric.microsoft.com/Item.ReadWrite.All,"
        "https://api.fabric.microsoft.com/Item.Execute.All,"
        "https://api.fabric.microsoft.com/DataPipeline.ReadWrite.All,"
        "https://api.fabric.microsoft.com/DataPipeline.Execute.All"
    )
    AZURE_ENABLE_LOCAL_SESSION_FALLBACK: bool = True
    FABRIC_ENABLE_LOCAL_SESSION_FALLBACK: bool = False
    AZURE_OPENAI_API_KEY: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("AZURE_OPENAI_API_KEY", "AZURE_OPENAI_KEY"),
    )

    # Email Settings (SMTP/Outlook)
    SMTP_SERVER: str = "smtp.office365.com"
    SMTP_PORT: int = 587
    SMTP_USERNAME: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    EMAIL_FROM: Optional[str] = None
    EMAIL_TO: Optional[str] = None

    # Allow reading from .env file
    model_config = SettingsConfigDict(env_file=str(ENV_FILE), extra="ignore")

settings = Settings()
