from json import dumps
from os.path import basename
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from backend.config import settings
from backend.logging_config.logger import logger
from boto3 import client
from botocore.exceptions import ClientError
from jinja2 import Environment, FileSystemLoader
from mypy_boto3_s3.client import S3Client


class BaseAWSOperation:
    """Base class for AWS operations."""

    def __init__(self, service_name: str) -> None:
        """Initialize AWS client with credentials.

        Args:
            service_name: Name of the AWS service to initialize
        """
        self.client: Any = client(
            service_name,
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_ACCESS_SECRET,
            region_name=settings.AWS_ACCESS_REGION,
        )


class S3Operation(BaseAWSOperation):
    """Handles all S3-related operations."""

    def __init__(self) -> None:
        """Initialize S3 client and set bucket name."""
        super().__init__("s3")
        self.client: S3Client = self.client  # Type cast for better type hints
        self.bucket_name: str = settings.AWS_BUCKET_NAME

    def upload_file(self, filepath: str, object_name: Optional[str] = None) -> bool:
        """Upload a file to S3 bucket.

        Args:
            filepath: Local path to the file
            object_name: S3 object name. If not specified, filename will be used

        Returns:
            bool: True if file was uploaded successfully, False otherwise
        """
        try:
            object_name = object_name or basename(filepath)
            logger.debug(f"Uploading file to S3; Filepath - {filepath} ObjectName - {object_name}")
            self.client.upload_file(filepath, self.bucket_name, object_name)
            return True

        except ClientError as err:
            logger.error(f"Failed to upload file {filepath}: {str(err)}")
            return False

    def check_object_exists(self, object_name: str) -> bool:
        """Check if an object exists in S3 bucket.

        Args:
            object_name: Name of the object to check

        Returns:
            bool: True if object exists, False otherwise
        """
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=object_name)
            return True

        except ClientError as err:
            if err.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Error checking object {object_name}: {str(err)}")
            return False

    def generate_presigned_url(self, object_name: str, expiration: int = 3600) -> str:
        """Generate a presigned URL for an S3 object.

        Args:
            object_name: Name of the object to generate URL for
            expiration: Time in seconds until URL expires (default: 1 hour)

        Returns:
            str: Presigned URL or error message
        """
        try:
            url = self.client.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": self.bucket_name, "Key": object_name},
                ExpiresIn=int(expiration),
            )
            # Validate URL
            urlparse(url)
            return url

        except (ClientError, ValueError) as err:
            logger.error(f"Failed to generate presigned URL for {object_name}: {str(err)}")
            return str(err)

    def delete_object(self, key: str) -> bool:
        """Delete an object from S3 bucket.

        Args:
            key: Key of the object to delete

        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            logger.debug(f"Deleting object from S3; Key - {key}")

            self.client.delete_object(Bucket=self.bucket_name, Key=key)
            return True

        except ClientError as err:
            logger.error(f"Failed to delete object {key}: {str(err)}")
            return False

    def get_client(self) -> S3Client:
        """Get the S3 client."""
        return self.client


class EmailOperation(BaseAWSOperation):
    def __init__(self) -> None:
        super().__init__("ses")
        self.env = Environment(loader=FileSystemLoader("templates"))

    def render_template(self, template_name, context):
        template = self.env.get_template(template_name)
        return template.render(context)

    def send_email(
        self,
        subject,
        body_html,
        recipient,
    ) -> Optional[bool]:
        try:
            response = self.client.send_email(
                Destination={
                    "ToAddresses": [recipient],
                },
                Message={
                    "Body": {
                        "Html": {
                            "Charset": "UTF-8",
                            "Data": body_html,
                        }
                    },
                    "Subject": {
                        "Charset": "UTF-8",
                        "Data": subject,
                    },
                },
                Source=settings.EMAIL_FROM,
            )

        except ClientError as e:
            logger.error(e.response["Error"]["Message"])
            return False
        else:
            logger.info(f"Email sent to {recipient}! Message Id: {response['MessageId']}")
            return True

    def send_bulk_email_with_ses_template(
        self,
        template_name: str,
        recipients_data: List[Dict[str, dict]],
        default_data: Dict[str, str] = None,
    ) -> Optional[bool]:
        """
        Sends bulk email using AWS SES template with per-recipient data.

        :param template_name: SES template name (must already exist)
        :param recipients_data: [
            {
                "email": "alice@example.com",
                "template_data": {
                    "candidate_name": "Alice",
                    "company_name": "Klizos",
                    "expiration_time": "24 hours",
                    "interview_link": "https://yourdomain.com/interview?token=abc123"
                }
            },
            ...
        ]
        :param default_data: Optional fallback values
        """
        try:
            destinations = [
                {
                    "Destination": {"ToAddresses": [recipient["email"]]},
                    "ReplacementTemplateData": dumps(recipient["template_data"]),
                }
                for recipient in recipients_data
            ]

            response = self.client.send_bulk_templated_email(
                Source=settings.EMAIL_FROM,
                Template=template_name,
                DefaultTemplateData=dumps(default_data or {}),
                Destinations=destinations,
            )

            logger.info(f"Bulk email send status: {response['Status']}")
            return True
        except ClientError as e:
            logger.error(f"Bulk send failed: {e.response['Error']['Message']}")
            return False


class SMSOperation(BaseAWSOperation):
    def __init__(self) -> None:
        super().__init__("sns")

    def send_sms(self, phone_no: str, msg: str) -> Optional[bool]:
        try:
            response = self.client.publish(PhoneNumber=phone_no, Message=msg)
        except ClientError as e:
            logger.error(e.response["Error"]["Message"])
            return False
        else:
            logger.info(f"SMS Sent to {phone_no}! Message Id: {response['MessageId']}")
            return True


# Singleton instance
s3_operation = S3Operation()
email_operation = EmailOperation()
sms_operation = SMSOperation()
