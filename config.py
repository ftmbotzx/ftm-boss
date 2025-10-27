"""
Configuration module for FTM Boss Assistant
Handles environment variables and application settings
"""

import os
from typing import Optional


class Config:
    """Configuration class for the bot"""

    def __init__(self):
        # Telegram Bot Configuration
        self.telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

        # Database Configuration
        self.database_url: str = os.getenv("DATABASE_URL", "")
        self.pg_host: str = os.getenv("PGHOST", "localhost")
        self.pg_port: str = os.getenv("PGPORT", "5432")
        self.pg_database: str = os.getenv("PGDATABASE", "ftm_boss_assistant")
        self.pg_user: str = os.getenv("PGUSER", "postgres")
        self.pg_password: str = os.getenv("PGPASSWORD", "")
        
        # MongoDB Configuration
        self.mongodb_uri: str = os.getenv("MONGODB_URI", "")

        # Google Translate API (optional, falls back to free googletrans)
        self.google_translate_api_key: Optional[str] = os.getenv("GOOGLE_TRANSLATE_API_KEY")

        # BKNMU Website Configuration
        self.bknmu_base_url: str = "https://www.bknmu.edu.in"
        self.bknmu_circulars_url: str = f"{self.bknmu_base_url}/NewsEventViewAll.aspx?ContentTypeId=7"

        # Fallback for older deployments
        if not hasattr(self, 'bknmu_circulars_url'):
            self.bknmu_circulars_url = f"{self.bknmu_base_url}/NewsEventViewAll.aspx?ContentTypeId=7"

        # Scraping Configuration
        self.user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        self.request_timeout: int = 30
        self.max_retries: int = 3

        # Date filtering configuration (format: YYYY-MM-DD)
        self.filter_from_date: str = os.getenv("FILTER_FROM_DATE", "2025-10-15")

        # Translation configuration
        self.enable_translation: bool = os.getenv("ENABLE_TRANSLATION", "true").lower() == "true"
        self.show_original_text: bool = os.getenv("SHOW_ORIGINAL_TEXT", "true").lower() == "true"

        # Validate required configuration
        self._validate_config()

    def _validate_config(self):
        """Validate required configuration parameters"""
        required_vars = {
            "TELEGRAM_BOT_TOKEN": self.telegram_bot_token,
            "TELEGRAM_CHAT_ID": self.telegram_chat_id,
        }

        missing_vars = [var for var, value in required_vars.items() if not value]

        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

        # Construct database URL if not provided
        if not self.database_url:
            if self.pg_password:
                self.database_url = f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_database}"
            else:
                self.database_url = f"postgresql://{self.pg_user}@{self.pg_host}:{self.pg_port}/{self.pg_database}"

    def get_database_config(self) -> dict:
        """Get database configuration as dictionary"""
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "database": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
        }
