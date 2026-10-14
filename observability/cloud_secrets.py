"""
cloud_secrets.py
----------------
Load secret settings (SLACK_WEBHOOK_URL, SMTP_PASSWORD, DATABRICKS_TOKEN, ...)
from AWS Secrets Manager into the environment, for cloud runtimes where there
is no .env file. Locally PIPELINE_SECRET_ID is unset and this does nothing.

    PIPELINE_SECRET_ID   name/ARN of a JSON secret: {"SLACK_WEBHOOK_URL": "...", ...}
    AWS_REGION           region of the secret (the runtime usually sets it)

Values already in the environment win, so a job can still override one setting.
The secret's values never appear in job definitions, Terraform state or logs.
"""

from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)


def load_secret_env(secret_id: str | None = None, client=None) -> list:
    """Copy the secret's keys into os.environ (without overwriting); return the keys set."""

    secret_id = secret_id or os.environ.get("PIPELINE_SECRET_ID")
    if not secret_id:
        return []

    if client is None:
        import boto3  # only on cloud runtimes (EMR, MWAA, EC2 have it)

        client = boto3.client("secretsmanager", region_name=os.environ.get("AWS_REGION"))

    values = json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])
    loaded = []
    for key, value in values.items():
        if key not in os.environ and value not in (None, ""):
            os.environ[key] = str(value)
            loaded.append(key)

    logger.info("Loaded %s setting(s) from secret %s: %s", len(loaded), secret_id, sorted(loaded))
    return loaded
