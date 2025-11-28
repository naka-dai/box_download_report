"""Main entry point for Box Download Report Batch."""

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import Config
from db import Database
from box_client import BoxClient
from events import EventsFetcher
from aggregator import DataAggregator
from anomaly import AnomalyDetector
from reporter import CSVReporter
from mailer import Mailer
from monthly_summary import MonthlySummaryGenerator


# Setup logging
def setup_logging():
    """Setup logging configuration."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('box_download_batch.log', encoding='utf-8')
        ]
    )


logger = logging.getLogger(__name__)


class BoxDownloadBatch:
    """Main batch processing class."""

    def __init__(self):
        """Initialize batch processor."""
        self.config = Config
        self.db = None
        self.box_client = None

    def run(self):
        """Run the batch process."""
        try:
            logger.info("=" * 80)
            logger.info("Box Download Report Batch - Starting")
            logger.info("=" * 80)

            # Validate configuration
            self.config.validate()
            self.config.ensure_directories()

            # Initialize database
            logger.info("Initializing database...")
            self.db = Database(self.config.DB_PATH)
            with self.db:
                self.db.initialize_tables()

            # Initialize Box client
            logger.info("Initializing Box client...")
            self.box_client = BoxClient(self.config.BOX_CONFIG_PATH)

            # Get target folder ID
            # If BOX_ROOT_FOLDER_ID is Box Reports folder (248280918136),
            # automatically find the latest "User Activity run on ~" folder
            target_folder_id = self.config.BOX_ROOT_FOLDER_ID

            if target_folder_id == "248280918136":
                logger.info(f"Box Reports folder detected (ID: {target_folder_id})")
                logger.info("Searching for latest 'User Activity run on ~' folder...")
                latest_folder_id = self.box_client.get_latest_user_activity_folder(target_folder_id)

                if latest_folder_id:
                    logger.info(f"Using latest User Activity folder as target: {latest_folder_id}")
                    target_folder_id = latest_folder_id
                else:
                    logger.warning("No User Activity folder found, using Box Reports folder as fallback")

            # Get target file IDs from folder
            logger.info(f"Getting file list from folder: {target_folder_id}")
            target_file_ids = self.box_client.get_all_file_ids_in_folder(target_folder_id)
            logger.info(f"Found {len(target_file_ids)} files in target folder")

            # Calculate target dates (JST)
            jst = timezone(timedelta(hours=9))
            today = datetime.now(jst).date()
            yesterday = today - timedelta(days=1)

            # Process confirmed period (yesterday)
            logger.info("\n" + "=" * 80)
            logger.info("Processing CONFIRMED period (yesterday)")
            logger.info("=" * 80)
            self.process_period(
                target_date=datetime.combine(yesterday, datetime.min.time()).replace(tzinfo=jst),
                period_type='confirmed',
                target_file_ids=target_file_ids
            )

            # Process tentative period (today)
            logger.info("\n" + "=" * 80)
            logger.info("Processing TENTATIVE period (today)")
            logger.info("=" * 80)
            self.process_period(
                target_date=datetime.combine(today, datetime.min.time()).replace(tzinfo=jst),
                period_type='tentative',
                target_file_ids=target_file_ids
            )

            # Check if monthly summary should be generated
            logger.info("\n" + "=" * 80)
            logger.info("Checking for monthly summary generation")
            logger.info("=" * 80)
            self.check_and_generate_monthly_summary(datetime.now(jst))

            logger.info("\n" + "=" * 80)
            logger.info("Box Download Report Batch - Completed Successfully")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Batch process failed: {e}", exc_info=True)
            sys.exit(1)

    def process_period(self, target_date: datetime, period_type: str, target_file_ids: set):
        """
        Process a specific period (confirmed or tentative).

        Args:
            target_date: Target date (JST)
            period_type: 'confirmed' or 'tentative'
            target_file_ids: Set of target file IDs
        """
        date_str = target_date.strftime('%Y%m%d')
        logger.info(f"Processing {period_type} period for date: {date_str}")

        # Fetch events
        logger.info("Fetching download events from Box API...")
        events_fetcher = EventsFetcher(self.box_client.get_client())
        events = events_fetcher.get_events_for_period(
            target_date=target_date,
            period_type=period_type,
            target_file_ids=target_file_ids
        )
        logger.info(f"Fetched {len(events)} download events")

        # Store events in database
        logger.info("Storing events in database...")
        with Database(self.config.DB_PATH) as db:
            inserted_count = 0
            duplicate_count = 0

            for event in events:
                if db.insert_download_event(event):
                    inserted_count += 1
                else:
                    duplicate_count += 1

            logger.info(f"Inserted {inserted_count} new events, {duplicate_count} duplicates skipped")

        # If no events, skip further processing
        if not events:
            logger.info("No events to process, skipping aggregation and anomaly detection")
            return

        # Aggregate data
        logger.info("Aggregating data...")
        aggregator = DataAggregator()

        file_stats = aggregator.aggregate_by_file(events)
        user_file_stats = aggregator.aggregate_by_user_and_file(events)
        user_stats = aggregator.aggregate_by_user(events)

        # Generate CSV reports
        logger.info("Generating CSV reports...")
        reporter = CSVReporter(self.config.REPORT_OUTPUT_DIR)

        reporter.write_file_downloads_report(file_stats, date_str, period_type)
        reporter.write_user_file_downloads_report(user_file_stats, date_str, period_type)
        reporter.write_access_log(
            events,
            date_str,
            period_type,
            self.config.ACCESS_LOG_OUTPUT_DIR
        )

        # Anomaly detection
        if self.config.ALERT_ENABLED:
            logger.info("Running anomaly detection...")

            # Get excluded users (system/admin accounts excluded from dashboard)
            excluded_users = self.config.get_alert_exclude_users()

            # Initialize anomaly detector
            detector = AnomalyDetector(
                download_count_threshold=self.config.ALERT_USER_DOWNLOAD_COUNT_THRESHOLD,
                unique_files_threshold=self.config.ALERT_USER_UNIQUE_FILES_THRESHOLD,
                offhour_threshold=self.config.ALERT_OFFHOUR_DOWNLOAD_THRESHOLD,
                spike_window_minutes=self.config.ALERT_SPIKE_WINDOW_MINUTES,
                spike_threshold=self.config.ALERT_SPIKE_DOWNLOAD_THRESHOLD,
                excluded_users=excluded_users
            )

            # Get business hours
            bh_start_hour, bh_start_min, bh_end_hour, bh_end_min = \
                self.config.get_business_hours_range()

            # Count off-hour downloads
            offhour_counts = aggregator.count_offhour_downloads_by_user(
                events,
                bh_start_hour,
                bh_start_min,
                bh_end_hour,
                bh_end_min
            )

            # Detect all anomalies
            anomalous_users = detector.detect_all_anomalies(user_stats, offhour_counts)

            if anomalous_users:
                logger.warning(f"Detected {len(anomalous_users)} anomalous users")

                # Generate anomaly summary
                anomaly_summary = detector.get_anomaly_summary(anomalous_users)
                logger.info(f"\n{anomaly_summary}")

                # Save anomalies to database
                with Database(self.config.DB_PATH) as db:
                    for user_login, data in anomalous_users.items():
                        anomaly_types = data.get('anomaly_types', [])
                        for anomaly in anomaly_types:
                            db.insert_anomaly(
                                batch_date=date_str,
                                period_type=period_type,
                                user_login=user_login,
                                anomaly_type=anomaly['type'],
                                value=anomaly['value']
                            )

                # Write anomaly details CSV
                anomaly_csv_path = reporter.write_anomaly_details(
                    anomalous_users,
                    date_str,
                    period_type,
                    self.config.ANOMALY_OUTPUT_DIR,
                    max_rows=self.config.ALERT_ATTACHMENT_MAX_ROWS
                )

                # Send email alert
                self.send_anomaly_alert(
                    date_str=date_str,
                    period_type=period_type,
                    anomaly_summary=anomaly_summary,
                    attachment_path=anomaly_csv_path
                )
            else:
                logger.info("No anomalies detected")
        else:
            logger.info("Anomaly detection is disabled")

    def send_anomaly_alert(
        self,
        date_str: str,
        period_type: str,
        anomaly_summary: str,
        attachment_path: str
    ):
        """
        Send anomaly alert email.

        Args:
            date_str: Date string
            period_type: Period type
            anomaly_summary: Anomaly summary text
            attachment_path: Path to CSV attachment
        """
        try:
            logger.info("Sending anomaly alert email...")

            mailer = Mailer(
                smtp_host=self.config.SMTP_HOST,
                smtp_port=self.config.SMTP_PORT,
                smtp_user=self.config.SMTP_USER,
                smtp_password=self.config.SMTP_PASSWORD,
                use_tls=self.config.SMTP_USE_TLS
            )

            to_addrs = self.config.get_mail_to_list()

            success = mailer.send_anomaly_alert(
                from_addr=self.config.ALERT_MAIL_FROM,
                to_addrs=to_addrs,
                subject_prefix=self.config.ALERT_MAIL_SUBJECT_PREFIX,
                date_str=f"{date_str} ({period_type})",
                anomaly_summary=anomaly_summary,
                attachment_paths=[attachment_path]
            )

            if success:
                logger.info("Anomaly alert email sent successfully")
            else:
                logger.error("Failed to send anomaly alert email")

        except Exception as e:
            logger.error(f"Error sending anomaly alert email: {e}", exc_info=True)

    def check_and_generate_monthly_summary(self, current_date: datetime):
        """
        Check if monthly summary should be generated and generate if needed.

        Args:
            current_date: Current date (JST)
        """
        with Database(self.config.DB_PATH) as db:
            summary_generator = MonthlySummaryGenerator(db)

            month_to_generate = summary_generator.should_generate_monthly_summary(current_date)

            if month_to_generate:
                logger.info(f"Generating monthly summary for {month_to_generate}")
                summary_generator.generate_monthly_summaries(month_to_generate)

                # Export to CSV
                reporter = CSVReporter(self.config.REPORT_OUTPUT_DIR)

                user_summary = db.get_monthly_user_summary(month_to_generate)
                file_summary = db.get_monthly_file_summary(month_to_generate)

                month_str = month_to_generate.replace('-', '')
                reporter.write_monthly_user_summary(user_summary, month_str)
                reporter.write_monthly_file_summary(file_summary, month_str)

                logger.info("Monthly summary generation completed")
            else:
                logger.info("Monthly summary generation not needed (not first day of month)")


def main():
    """Main entry point."""
    setup_logging()

    try:
        batch = BoxDownloadBatch()
        batch.run()
    except KeyboardInterrupt:
        logger.info("\nBatch process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
