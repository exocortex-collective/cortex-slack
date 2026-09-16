"""Object storage for Slack file attachments.

A local client rather than an import from postmark: CLAUDE.md forbids services
cross-importing each other, and the shared home would be cortex-utils, which is
a separate repo and a separate PR. Thirty lines of boto3 is the cheaper
duplication until a third caller makes extraction worth it.
"""

from __future__ import annotations

import boto3
from botocore.exceptions import ClientError


class S3Client:
    def __init__(
        self,
        endpoint_url: str | None,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        bucket_name: str,
    ) -> None:
        # An empty endpoint means "real AWS"; passing "" makes boto raise
        # "Invalid endpoint", which reads as a credential problem rather than a
        # configuration one.
        self.s3 = boto3.resource(
            "s3",
            region_name="us-east-1",
            endpoint_url=endpoint_url or None,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self.bucket_name = bucket_name
        self._create_bucket_if_missing()

    def _create_bucket_if_missing(self) -> None:
        try:
            self.s3.create_bucket(Bucket=self.bucket_name)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "BucketAlreadyOwnedByYou":
                raise

    def put_bytes(self, key: str, data: bytes) -> None:
        self.s3.Object(self.bucket_name, key).put(Body=data)

    def get_bytes(self, key: str) -> bytes:
        data: bytes = self.s3.Object(self.bucket_name, key).get()["Body"].read()
        return data
