"""Database module for SQLite operations."""

import sqlite3
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class Database:
    """SQLite database handler for Box download audit."""

    def __init__(self, db_path: str):
        """
        Initialize database connection.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._ensure_db_directory()
        self.connection: Optional[sqlite3.Connection] = None

    def _ensure_db_directory(self) -> None:
        """Create database directory if it doesn't exist."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> None:
        """Establish database connection."""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        logger.info(f"Connected to database: {self.db_path}")

    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def initialize_tables(self) -> None:
        """Create all required tables if they don't exist."""
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()

        # Table: downloads (detailed event logs)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                stream_type TEXT,
                event_type TEXT,
                user_login TEXT,
                user_name TEXT,
                file_id TEXT,
                file_name TEXT,
                download_at_utc TEXT,
                download_at_jst TEXT,
                ip_address TEXT,
                client_type TEXT,
                user_agent TEXT,
                raw_json TEXT,
                inserted_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(event_id, download_at_utc)
            )
        """)

        # Create indexes for downloads
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_downloads_user_login
            ON downloads(user_login)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_downloads_file_id
            ON downloads(file_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_downloads_download_at_jst
            ON downloads(download_at_jst)
        """)

        # Table: anomalies
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_date TEXT NOT NULL,
                period_type TEXT NOT NULL,
                user_login TEXT,
                anomaly_type TEXT NOT NULL,
                value REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Table: monthly_user_summary
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monthly_user_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT NOT NULL,
                user_login TEXT NOT NULL,
                user_name TEXT,
                total_downloads INTEGER,
                active_days INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(month, user_login)
            )
        """)

        # Table: monthly_file_summary
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monthly_file_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT NOT NULL,
                file_id TEXT NOT NULL,
                file_name TEXT,
                total_downloads INTEGER,
                unique_users INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(month, file_id)
            )
        """)

        # Table: alert_history (アラート送信履歴 - 重複防止用)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_date TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                anomaly_count INTEGER,
                csv_path TEXT,
                box_file_id TEXT,
                email_sent INTEGER DEFAULT 0,
                box_uploaded INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(alert_date, alert_type)
            )
        """)

        self.connection.commit()
        logger.info("Database tables initialized successfully")

    def insert_download_event(self, event: Dict[str, Any]) -> bool:
        """
        Insert a download event into the downloads table.

        Args:
            event: Dictionary containing event data

        Returns:
            True if inserted, False if duplicate
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()

        try:
            cursor.execute("""
                INSERT INTO downloads (
                    event_id, stream_type, event_type, user_login, user_name,
                    file_id, file_name, download_at_utc, download_at_jst,
                    ip_address, client_type, user_agent, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event.get("event_id"),
                event.get("stream_type"),
                event.get("event_type"),
                event.get("user_login"),
                event.get("user_name"),
                event.get("file_id"),
                event.get("file_name"),
                event.get("download_at_utc"),
                event.get("download_at_jst"),
                event.get("ip_address"),
                event.get("client_type"),
                event.get("user_agent"),
                event.get("raw_json"),
            ))
            self.connection.commit()
            return True
        except sqlite3.IntegrityError:
            # Duplicate event
            return False

    def insert_anomaly(self, batch_date: str, period_type: str, user_login: str,
                      anomaly_type: str, value: float) -> None:
        """
        Insert an anomaly record.

        Args:
            batch_date: Date of the batch run (YYYY-MM-DD)
            period_type: 'confirmed' or 'tentative'
            user_login: User login ID
            anomaly_type: Type of anomaly
            value: Anomaly value
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO anomalies (
                batch_date, period_type, user_login, anomaly_type, value
            ) VALUES (?, ?, ?, ?, ?)
        """, (batch_date, period_type, user_login, anomaly_type, value))
        self.connection.commit()

    def get_downloads_by_period(self, start_time: str, end_time: str) -> List[Dict[str, Any]]:
        """
        Get download events for a specific time period.

        Args:
            start_time: Start time in ISO format (JST)
            end_time: End time in ISO format (JST)

        Returns:
            List of download events
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT * FROM downloads
            WHERE download_at_jst >= ? AND download_at_jst < ?
            ORDER BY download_at_jst
        """, (start_time, end_time))

        return [dict(row) for row in cursor.fetchall()]

    def get_downloads_by_user_and_period(self, user_login: str, start_time: str,
                                        end_time: str) -> List[Dict[str, Any]]:
        """
        Get download events for a specific user and time period.

        Args:
            user_login: User login ID
            start_time: Start time in ISO format (JST)
            end_time: End time in ISO format (JST)

        Returns:
            List of download events
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT * FROM downloads
            WHERE user_login = ?
              AND download_at_jst >= ?
              AND download_at_jst < ?
            ORDER BY download_at_jst
        """, (user_login, start_time, end_time))

        return [dict(row) for row in cursor.fetchall()]

    def upsert_monthly_user_summary(self, month: str, user_login: str, user_name: str,
                                   total_downloads: int, active_days: int) -> None:
        """
        Insert or update monthly user summary.

        Args:
            month: Month in YYYY-MM format
            user_login: User login ID
            user_name: User name
            total_downloads: Total download count
            active_days: Number of active days
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO monthly_user_summary (
                month, user_login, user_name, total_downloads, active_days
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(month, user_login) DO UPDATE SET
                user_name = excluded.user_name,
                total_downloads = excluded.total_downloads,
                active_days = excluded.active_days,
                created_at = CURRENT_TIMESTAMP
        """, (month, user_login, user_name, total_downloads, active_days))
        self.connection.commit()

    def upsert_monthly_file_summary(self, month: str, file_id: str, file_name: str,
                                   total_downloads: int, unique_users: int) -> None:
        """
        Insert or update monthly file summary.

        Args:
            month: Month in YYYY-MM format
            file_id: File ID
            file_name: File name
            total_downloads: Total download count
            unique_users: Number of unique users
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO monthly_file_summary (
                month, file_id, file_name, total_downloads, unique_users
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(month, file_id) DO UPDATE SET
                file_name = excluded.file_name,
                total_downloads = excluded.total_downloads,
                unique_users = excluded.unique_users,
                created_at = CURRENT_TIMESTAMP
        """, (month, file_id, file_name, total_downloads, unique_users))
        self.connection.commit()

    def get_monthly_user_summary(self, month: str) -> List[Dict[str, Any]]:
        """
        Get monthly user summary for a specific month.

        Args:
            month: Month in YYYY-MM format

        Returns:
            List of user summaries
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT month, user_login, user_name, total_downloads, active_days, created_at
            FROM monthly_user_summary
            WHERE month = ?
            ORDER BY total_downloads DESC
        """, (month,))

        return [dict(row) for row in cursor.fetchall()]

    def get_monthly_file_summary(self, month: str) -> List[Dict[str, Any]]:
        """
        Get monthly file summary for a specific month.

        Args:
            month: Month in YYYY-MM format

        Returns:
            List of file summaries
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT month, file_id, file_name, total_downloads, unique_users, created_at
            FROM monthly_file_summary
            WHERE month = ?
            ORDER BY total_downloads DESC
        """, (month,))

        return [dict(row) for row in cursor.fetchall()]

    def check_alert_sent(self, alert_date: str, alert_type: str = 'daily') -> bool:
        """
        Check if an alert has already been sent for the given date.

        Args:
            alert_date: Date in YYYY-MM-DD format
            alert_type: Type of alert ('daily', 'confirmed', 'tentative')

        Returns:
            True if alert was already sent, False otherwise
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT id FROM alert_history
            WHERE alert_date = ? AND alert_type = ? AND email_sent = 1
        """, (alert_date, alert_type))

        return cursor.fetchone() is not None

    def check_alert_uploaded(self, alert_date: str, alert_type: str = 'daily') -> bool:
        """
        Check if an alert CSV has already been uploaded to Box for the given date.

        Args:
            alert_date: Date in YYYY-MM-DD format
            alert_type: Type of alert ('daily', 'confirmed', 'tentative')

        Returns:
            True if already uploaded, False otherwise
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT id FROM alert_history
            WHERE alert_date = ? AND alert_type = ? AND box_uploaded = 1
        """, (alert_date, alert_type))

        return cursor.fetchone() is not None

    def record_alert_sent(self, alert_date: str, alert_type: str, anomaly_count: int,
                          csv_path: str = None) -> None:
        """
        Record that an alert email has been sent.

        Args:
            alert_date: Date in YYYY-MM-DD format
            alert_type: Type of alert ('daily', 'confirmed', 'tentative')
            anomaly_count: Number of anomalies detected
            csv_path: Path to the CSV file attached
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO alert_history (alert_date, alert_type, anomaly_count, csv_path, email_sent)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(alert_date, alert_type) DO UPDATE SET
                anomaly_count = excluded.anomaly_count,
                csv_path = excluded.csv_path,
                email_sent = 1,
                created_at = CURRENT_TIMESTAMP
        """, (alert_date, alert_type, anomaly_count, csv_path))
        self.connection.commit()
        logger.info(f"Alert recorded: {alert_date} ({alert_type}), {anomaly_count} anomalies")

    def record_alert_uploaded(self, alert_date: str, alert_type: str, box_file_id: str) -> None:
        """
        Record that an alert CSV has been uploaded to Box.

        Args:
            alert_date: Date in YYYY-MM-DD format
            alert_type: Type of alert ('daily', 'confirmed', 'tentative')
            box_file_id: Box file ID of the uploaded file
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO alert_history (alert_date, alert_type, box_file_id, box_uploaded)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(alert_date, alert_type) DO UPDATE SET
                box_file_id = excluded.box_file_id,
                box_uploaded = 1,
                created_at = CURRENT_TIMESTAMP
        """, (alert_date, alert_type, box_file_id))
        self.connection.commit()
        logger.info(f"Box upload recorded: {alert_date} ({alert_type}), file_id={box_file_id}")
