"""Google Cloud Storage raw layer.

Object layout:

    gs://<bucket>/raw/source=<source_name>/batch=<batch_id>/data.parquet

Hive-style ``key=value`` path segments mean the same prefix can be read by a
BigQuery external table or a Dataflow job later without renaming anything, and
make it obvious from the path alone which run produced an object.

The google-cloud-storage client is imported lazily so the rest of the package
stays importable (and unit-testable) without the cloud dependency installed.
Authentication uses Application Default Credentials - this module never reads
a key file path from configuration and never logs credential material.
"""

from __future__ import annotations

from pathlib import Path

from .logging_setup import get_logger

logger = get_logger(__name__)


def raw_object_name(source_name: str, batch_id: str, filename: str = "data.parquet") -> str:
    return f"raw/source={source_name}/batch={batch_id}/{filename}"


def gcs_uri(bucket: str, object_name: str) -> str:
    return f"gs://{bucket}/{object_name}"


def upload_file(bucket_name: str, local_path: Path, object_name: str, *, project_id: str | None = None) -> str:
    """Upload one file to GCS and return its gs:// URI."""
    try:
        from google.api_core import exceptions as gcp_exceptions  # noqa: PLC0415
        from google.cloud import storage  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError("google-cloud-storage is not installed. Run: pip install -r requirements.txt") from exc

    local_path = Path(local_path)
    if not local_path.exists():
        raise FileNotFoundError(f"Nothing to upload: {local_path} does not exist. Run the ingest step first.")

    client = storage.Client(project=project_id) if project_id else storage.Client()
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(str(local_path))
    except gcp_exceptions.Forbidden as exc:
        raise RuntimeError(
            f"Permission denied writing to gs://{bucket_name}/{object_name}. "
            f"The active identity needs roles/storage.objectAdmin on the bucket "
            f"(Terraform grants this to the pipeline service account). Original error: {exc}"
        ) from exc
    except gcp_exceptions.NotFound as exc:
        raise RuntimeError(
            f"Bucket {bucket_name!r} not found. Apply the Terraform configuration first "
            f"(cd terraform && terraform apply). Original error: {exc}"
        ) from exc

    uri = gcs_uri(bucket_name, object_name)
    logger.info(
        "uploaded to gcs",
        extra={"uri": uri, "local_path": str(local_path), "bytes": local_path.stat().st_size},
    )
    return uri
