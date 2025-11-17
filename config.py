"""Configuration module for Box Download Report Batch."""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load .env file
load_dotenv()


class Config:
    """Configuration class for Box Download Report Batch."""

    # Box API Configuration
    BOX_CONFIG_PATH: str = os.getenv("BOX_CONFIG_PATH", "")
    BOX_ROOT_FOLDER_ID: str = os.getenv("BOX_ROOT_FOLDER_ID", "")

    # Output Directories
    REPORT_OUTPUT_DIR: str = os.getenv("REPORT_OUTPUT_DIR", r"C:\box_reports")
    ACCESS_LOG_OUTPUT_DIR: str = os.getenv("ACCESS_LOG_OUTPUT_DIR", r"C:\box_reports")
    ANOMALY_OUTPUT_DIR: str = os.getenv("ANOMALY_OUTPUT_DIR", r"C:\box_reports")

    # SQLite Database
    DB_PATH: str = os.getenv("DB_PATH", r"C:\box_reports\box_audit.db")

    # Alert Settings
    ALERT_ENABLED: bool = os.getenv("ALERT_ENABLED", "True").lower() in ("true", "1", "yes")
    ALERT_USER_DOWNLOAD_COUNT_THRESHOLD: int = int(os.getenv("ALERT_USER_DOWNLOAD_COUNT_THRESHOLD", "200"))
    ALERT_USER_UNIQUE_FILES_THRESHOLD: int = int(os.getenv("ALERT_USER_UNIQUE_FILES_THRESHOLD", "100"))

    # Business Hours (JST)
    BUSINESS_HOURS_START: str = os.getenv("BUSINESS_HOURS_START", "08:00")
    BUSINESS_HOURS_END: str = os.getenv("BUSINESS_HOURS_END", "20:00")
    ALERT_OFFHOUR_DOWNLOAD_THRESHOLD: int = int(os.getenv("ALERT_OFFHOUR_DOWNLOAD_THRESHOLD", "50"))

    # Spike Detection
    ALERT_SPIKE_WINDOW_MINUTES: int = int(os.getenv("ALERT_SPIKE_WINDOW_MINUTES", "60"))
    ALERT_SPIKE_DOWNLOAD_THRESHOLD: int = int(os.getenv("ALERT_SPIKE_DOWNLOAD_THRESHOLD", "100"))

    # Email Configuration
    SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.example.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USE_TLS: bool = os.getenv("SMTP_USE_TLS", "True").lower() in ("true", "1", "yes")
    SMTP_USER: str = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    ALERT_MAIL_FROM: str = os.getenv("ALERT_MAIL_FROM", "alert@example.com")
    ALERT_MAIL_TO: str = os.getenv("ALERT_MAIL_TO", "security@example.com")
    ALERT_MAIL_SUBJECT_PREFIX: str = os.getenv("ALERT_MAIL_SUBJECT_PREFIX", "[BoxDL Alert]")

    # Attachment Settings
    ALERT_ATTACHMENT_MAX_ROWS: int = int(os.getenv("ALERT_ATTACHMENT_MAX_ROWS", "5000"))

    @classmethod
    def validate(cls) -> bool:
        """Validate required configuration."""
        required_fields = [
            ("BOX_CONFIG_PATH", cls.BOX_CONFIG_PATH),
            ("BOX_ROOT_FOLDER_ID", cls.BOX_ROOT_FOLDER_ID),
        ]

        missing = []
        for field_name, value in required_fields:
            if not value:
                missing.append(field_name)

        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")

        return True

    @classmethod
    def ensure_directories(cls) -> None:
        """Create output directories if they don't exist."""
        directories = [
            cls.REPORT_OUTPUT_DIR,
            cls.ACCESS_LOG_OUTPUT_DIR,
            cls.ANOMALY_OUTPUT_DIR,
        ]

        # Create DB directory
        db_dir = Path(cls.DB_PATH).parent
        directories.append(str(db_dir))

        for directory in set(directories):
            Path(directory).mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_mail_to_list(cls) -> list[str]:
        """Get email recipients as a list."""
        if not cls.ALERT_MAIL_TO:
            return []
        return [email.strip() for email in cls.ALERT_MAIL_TO.split(",")]

    @classmethod
    def get_business_hours_range(cls) -> tuple[int, int, int, int]:
        """
        Get business hours as tuple (start_hour, start_minute, end_hour, end_minute).

        Returns:
            tuple: (start_hour, start_minute, end_hour, end_minute)
        """
        start_parts = cls.BUSINESS_HOURS_START.split(":")
        end_parts = cls.BUSINESS_HOURS_END.split(":")

        return (
            int(start_parts[0]),
            int(start_parts[1]) if len(start_parts) > 1 else 0,
            int(end_parts[0]),
            int(end_parts[1]) if len(end_parts) > 1 else 0,
        )
