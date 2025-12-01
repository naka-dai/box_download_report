"""Email notification module with CSV attachments."""

import smtplib
import logging
import csv
import sys
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


def get_app_dir() -> Path:
    """Get application directory (EXE directory or script directory)."""
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent
    else:
        return Path(__file__).parent


def load_recipients_from_csv(csv_path: Optional[str] = None) -> List[str]:
    """
    Load email recipients from CSV file.

    CSV format:
        email,name,enabled
        user@example.com,User Name,1

    Args:
        csv_path: Path to CSV file. If None, uses mail_recipients.csv in app directory.

    Returns:
        List of enabled email addresses
    """
    if csv_path is None:
        csv_path = get_app_dir() / 'mail_recipients.csv'
    else:
        csv_path = Path(csv_path)

    if not csv_path.exists():
        logger.warning(f"Recipients CSV not found: {csv_path}")
        return []

    recipients = []
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                email = row.get('email', '').strip()
                enabled = row.get('enabled', '1').strip()

                # Skip disabled or empty entries
                if not email:
                    continue
                if enabled.lower() in ('0', 'false', 'no', ''):
                    logger.debug(f"Skipping disabled recipient: {email}")
                    continue

                recipients.append(email)
                logger.debug(f"Loaded recipient: {email}")

        logger.info(f"Loaded {len(recipients)} recipients from {csv_path}")
        return recipients

    except Exception as e:
        logger.error(f"Failed to load recipients CSV: {e}")
        return []


class Mailer:
    """Email sender for anomaly alerts."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        smtp_user: str,
        smtp_password: str,
        use_tls: bool = True
    ):
        """
        Initialize mailer.

        Args:
            smtp_host: SMTP server host
            smtp_port: SMTP server port
            smtp_user: SMTP username
            smtp_password: SMTP password
            use_tls: Whether to use TLS (default: True)
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.use_tls = use_tls

    def send_anomaly_alert(
        self,
        from_addr: str,
        to_addrs: List[str],
        subject_prefix: str,
        date_str: str,
        anomaly_summary: str,
        attachment_paths: List[str] = None,
        severity_info: Dict[str, Any] = None
    ) -> bool:
        """
        Send anomaly alert email with CSV attachments.

        Args:
            from_addr: Sender email address
            to_addrs: List of recipient email addresses
            subject_prefix: Subject line prefix
            date_str: Date string for subject
            anomaly_summary: Summary text for email body
            attachment_paths: List of file paths to attach
            severity_info: Severity information dict with 'level' and 'max_ratio'

        Returns:
            True if sent successfully, False otherwise
        """
        if not to_addrs:
            logger.warning("No recipients specified for anomaly alert")
            return False

        try:
            # Determine severity level
            severity = severity_info.get('level', 'normal') if severity_info else 'normal'
            max_ratio = severity_info.get('max_ratio', 1.0) if severity_info else 1.0

            # Create message
            msg = MIMEMultipart()
            msg['From'] = from_addr
            msg['To'] = ', '.join(to_addrs)

            # Create subject based on severity
            if severity == 'critical':
                msg['Subject'] = f"🚨🚨🚨 {subject_prefix} 【緊急】大量ダウンロード検知（閾値の{max_ratio:.0f}倍超過）- {date_str} 🚨🚨🚨"
            elif severity == 'high':
                msg['Subject'] = f"⚠️ {subject_prefix} 【警告】異常ダウンロード検知（閾値の{max_ratio:.0f}倍超過）- {date_str}"
            else:
                msg['Subject'] = f"{subject_prefix} Box Download Anomalies Detected - {date_str}"

            # Create email body
            body = self._create_email_body(date_str, anomaly_summary, severity=severity, max_ratio=max_ratio)
            msg.attach(MIMEText(body, 'plain', 'utf-8'))

            # Attach files
            if attachment_paths:
                for filepath in attachment_paths:
                    self._attach_file(msg, filepath)

            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()

                if self.smtp_user and self.smtp_password:
                    server.login(self.smtp_user, self.smtp_password)

                server.send_message(msg)

            logger.info(f"Anomaly alert email sent to {', '.join(to_addrs)}")
            return True

        except Exception as e:
            logger.error(f"Failed to send anomaly alert email: {e}")
            return False

    def _create_email_body(
        self,
        date_str: str,
        anomaly_summary: str,
        severity: str = 'normal',
        max_ratio: float = 1.0
    ) -> str:
        """
        Create email body text.

        Args:
            date_str: Date string
            anomaly_summary: Anomaly summary text
            severity: Severity level ('critical', 'high', 'normal')
            max_ratio: Maximum threshold ratio exceeded

        Returns:
            Email body text
        """
        if severity == 'critical':
            body = f"""🚨🚨🚨 緊急セキュリティアラート 🚨🚨🚨

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  大量ダウンロードを検知しました  ⚠️
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【検知日時】{date_str}
【深刻度】🔴 緊急（閾値の {max_ratio:.0f} 倍超過）

【検知内容】
{anomaly_summary}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔔 緊急対応が必要です 🔔
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【推奨アクション】
1. 該当ユーザーのアカウントを一時停止してください
2. ダウンロードされたファイルの内容を確認してください
3. 本人に連絡して意図を確認してください
4. 情報漏洩の可能性がある場合は上長に報告してください

添付のCSVファイルに詳細情報が記載されています。
直ちに確認をお願いします。

📊 ダッシュボードで詳細を確認:
https://box-dashboard-report.pages.dev/

---
⚡ Box Download Report 自動アラート
"""
        elif severity == 'high':
            body = f"""⚠️ セキュリティ警告 ⚠️

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
異常なダウンロード活動を検知しました
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【検知日時】{date_str}
【深刻度】🟠 警告（閾値の {max_ratio:.0f} 倍超過）

【検知内容】
{anomaly_summary}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【推奨アクション】
1. 該当ユーザーの活動内容を確認してください
2. 業務上必要なダウンロードかどうか確認してください
3. 不審な点があれば上長に報告してください

添付のCSVファイルで詳細をご確認ください。

📊 ダッシュボードで詳細を確認:
https://box-dashboard-report.pages.dev/

---
Box Download Report 自動アラート
"""
        else:
            body = f"""Box Download Anomaly Alert

Date: {date_str}

{anomaly_summary}

Please review the attached CSV files for detailed information about the anomalous download activities.

📊 View Dashboard:
https://box-dashboard-report.pages.dev/

---
This is an automated alert from Box Download Report Batch.
"""
        return body

    def _attach_file(self, msg: MIMEMultipart, filepath: str) -> None:
        """
        Attach a file to the email message.

        Args:
            msg: Email message object
            filepath: Path to file to attach
        """
        try:
            path = Path(filepath)
            if not path.exists():
                logger.warning(f"Attachment file not found: {filepath}")
                return

            # Open and read the file
            with open(filepath, 'rb') as f:
                attachment = MIMEBase('application', 'octet-stream')
                attachment.set_payload(f.read())

            # Encode the file in base64
            encoders.encode_base64(attachment)

            # Add header
            filename = path.name
            attachment.add_header(
                'Content-Disposition',
                f'attachment; filename={filename}'
            )

            msg.attach(attachment)
            logger.info(f"Attached file: {filename}")

        except Exception as e:
            logger.error(f"Failed to attach file {filepath}: {e}")

    def test_connection(self) -> bool:
        """
        Test SMTP connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()

                if self.smtp_user and self.smtp_password:
                    server.login(self.smtp_user, self.smtp_password)

            logger.info("SMTP connection test successful")
            return True

        except Exception as e:
            logger.error(f"SMTP connection test failed: {e}")
            return False

    def send_anomaly_alert_from_csv(
        self,
        from_addr: str,
        recipients_csv_path: Optional[str],
        subject_prefix: str,
        date_str: str,
        anomaly_summary: str,
        attachment_paths: List[str] = None
    ) -> bool:
        """
        Send anomaly alert email with recipients loaded from CSV.

        Args:
            from_addr: Sender email address
            recipients_csv_path: Path to recipients CSV file (None for default)
            subject_prefix: Subject line prefix
            date_str: Date string for subject
            anomaly_summary: Summary text for email body
            attachment_paths: List of file paths to attach

        Returns:
            True if sent successfully, False otherwise
        """
        # Load recipients from CSV
        to_addrs = load_recipients_from_csv(recipients_csv_path)

        if not to_addrs:
            logger.warning("No recipients loaded from CSV")
            return False

        return self.send_anomaly_alert(
            from_addr=from_addr,
            to_addrs=to_addrs,
            subject_prefix=subject_prefix,
            date_str=date_str,
            anomaly_summary=anomaly_summary,
            attachment_paths=attachment_paths
        )

    def send_test_email(
        self,
        from_addr: str,
        to_addrs: List[str] = None,
        recipients_csv_path: Optional[str] = None
    ) -> bool:
        """
        Send a test email to verify configuration.

        Args:
            from_addr: Sender email address
            to_addrs: List of recipient addresses (if None, loads from CSV)
            recipients_csv_path: Path to recipients CSV file

        Returns:
            True if sent successfully, False otherwise
        """
        # Load from CSV if no addresses provided
        if to_addrs is None:
            to_addrs = load_recipients_from_csv(recipients_csv_path)

        if not to_addrs:
            logger.warning("No recipients specified for test email")
            return False

        try:
            msg = MIMEMultipart()
            msg['From'] = from_addr
            msg['To'] = ', '.join(to_addrs)
            msg['Subject'] = '[Test] Box Download Report - Email Configuration Test'

            body = """Box Download Report - テストメール

このメールは、Box Download Report システムのメール通知機能のテストです。

このメールが届いていれば、メール通知設定は正常に動作しています。

---
Box Download Report Batch
"""
            msg.attach(MIMEText(body, 'plain', 'utf-8'))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()

                if self.smtp_user and self.smtp_password:
                    server.login(self.smtp_user, self.smtp_password)

                server.send_message(msg)

            logger.info(f"Test email sent to {', '.join(to_addrs)}")
            return True

        except Exception as e:
            logger.error(f"Failed to send test email: {e}")
            return False


def create_mailer_from_env() -> Optional['Mailer']:
    """
    Create Mailer instance from environment variables.

    Expected environment variables:
        SMTP_HOST: SMTP server host
        SMTP_PORT: SMTP server port (default: 587)
        SMTP_USER: SMTP username
        SMTP_PASSWORD: SMTP password
        SMTP_USE_TLS: Whether to use TLS (default: True)

    Returns:
        Mailer instance or None if required variables are missing
    """
    smtp_host = os.getenv('SMTP_HOST')
    if not smtp_host:
        logger.warning("SMTP_HOST not configured")
        return None

    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    smtp_user = os.getenv('SMTP_USER', '')
    smtp_password = os.getenv('SMTP_PASSWORD', '')
    use_tls = os.getenv('SMTP_USE_TLS', 'True').lower() in ('true', '1', 'yes')

    return Mailer(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        use_tls=use_tls
    )
