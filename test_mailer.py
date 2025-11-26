"""Test script for mailer module."""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load .env
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

from mailer import Mailer, load_recipients_from_csv, create_mailer_from_env


def test_load_recipients():
    """Test loading recipients from CSV."""
    print("\n" + "=" * 60)
    print("Test 1: Loading recipients from CSV")
    print("=" * 60)

    # Test with default path
    recipients = load_recipients_from_csv()
    print(f"Loaded recipients: {recipients}")
    print(f"Count: {len(recipients)}")

    return recipients


def test_smtp_connection():
    """Test SMTP connection."""
    print("\n" + "=" * 60)
    print("Test 2: SMTP Connection Test")
    print("=" * 60)

    mailer = create_mailer_from_env()
    if mailer is None:
        print("ERROR: Could not create mailer from environment")
        return False

    print(f"SMTP Host: {mailer.smtp_host}")
    print(f"SMTP Port: {mailer.smtp_port}")
    print(f"SMTP User: {mailer.smtp_user}")
    print(f"Use TLS: {mailer.use_tls}")

    result = mailer.test_connection()
    print(f"Connection test result: {'SUCCESS' if result else 'FAILED'}")

    return result


def test_send_email():
    """Test sending email."""
    print("\n" + "=" * 60)
    print("Test 3: Send Test Email")
    print("=" * 60)

    mailer = create_mailer_from_env()
    if mailer is None:
        print("ERROR: Could not create mailer from environment")
        return False

    from_addr = os.getenv('ALERT_MAIL_FROM', os.getenv('SMTP_USER'))
    print(f"From: {from_addr}")

    # Load recipients from CSV
    recipients = load_recipients_from_csv()
    print(f"To: {recipients}")

    if not recipients:
        print("ERROR: No recipients loaded")
        return False

    result = mailer.send_test_email(
        from_addr=from_addr,
        to_addrs=recipients
    )

    print(f"Send result: {'SUCCESS' if result else 'FAILED'}")
    return result


def main():
    """Run all tests."""
    print("=" * 60)
    print("Box Download Report - Mailer Test")
    print("=" * 60)

    # Test 1: Load recipients
    recipients = test_load_recipients()

    # Test 2: SMTP connection (only if password is configured)
    smtp_password = os.getenv('SMTP_PASSWORD', 'xxxxxx')
    if smtp_password == 'xxxxxx':
        print("\n" + "=" * 60)
        print("SMTP_PASSWORD not configured. Skipping connection and send tests.")
        print("Please configure SMTP_PASSWORD in .env file.")
        print("=" * 60)
        return

    # Test 2: SMTP connection
    connection_ok = test_smtp_connection()

    if connection_ok:
        # Test 3: Send email
        confirm = input("\nSend test email? (y/n): ").strip().lower()
        if confirm == 'y':
            test_send_email()
        else:
            print("Skipped sending test email.")


if __name__ == '__main__':
    main()
