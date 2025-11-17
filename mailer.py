"""Email notification module with CSV attachments."""

import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


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
        attachment_paths: List[str] = None
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

        Returns:
            True if sent successfully, False otherwise
        """
        if not to_addrs:
            logger.warning("No recipients specified for anomaly alert")
            return False

        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = from_addr
            msg['To'] = ', '.join(to_addrs)
            msg['Subject'] = f"{subject_prefix} Box Download Anomalies Detected - {date_str}"

            # Create email body
            body = self._create_email_body(date_str, anomaly_summary)
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

    def _create_email_body(self, date_str: str, anomaly_summary: str) -> str:
        """
        Create email body text.

        Args:
            date_str: Date string
            anomaly_summary: Anomaly summary text

        Returns:
            Email body text
        """
        body = f"""Box Download Anomaly Alert

Date: {date_str}

{anomaly_summary}

Please review the attached CSV files for detailed information about the anomalous download activities.

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
