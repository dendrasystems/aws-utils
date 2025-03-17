import functools
import os
from typing import Generator, NamedTuple
import boto3
from boto3.s3 import transfer
from urllib.parse import urlsplit
from botocore.exceptions import ClientError


@functools.lru_cache
def _get_client():
    return boto3.client("s3")


class S3UrlParts(NamedTuple):
    bucket: str
    key: str


def parse_s3_url(url: str) -> S3UrlParts:
    """
    Parses an s3 URL with either an s3:// or https:// scheme, to extract the bucket and key.

    Returns:
        A tuple of bucket and key

    Raises:
      ValueError: If the url is not a valid S3 URL
    """
    parts = urlsplit(url)
    if parts.scheme not in ("https", "s3"):
        raise ValueError(f"{url} is not a valid S3 URL")

    if parts.scheme == "s3":
        return S3UrlParts(bucket=parts.netloc, key=parts.path.lstrip("/"))
    if parts.netloc.endswith(".s3.amazonaws.com"):
        return S3UrlParts(
            bucket=parts.netloc.rsplit(".s3.", 1)[0], key=parts.path.lstrip("/")
        )
    if parts.netloc.startswith("s3-") and parts.netloc.endswith(".amazonaws.com"):
        bucket, key = parts.path.split("/", 2)[1:]
        return S3UrlParts(bucket=bucket, key=key.lstrip("/"))
    if parts.netloc.endswith(".amazonaws.com"):
        return S3UrlParts(bucket=parts.netloc.split(".")[0], key=parts.path.lstrip("/"))

    raise ValueError(f"{url} is not a valid S3 URL")


def make_s3_url(bucket: str, key: str):
    """
    Converts a bucket and key into an s3:// URL.
    """
    return f"s3://{bucket}/{key}"


def make_https_url(bucket: str, key: str, aws_region: str):
    """
    Converts a bucket and key into an https:// URL.
    """
    return f"https://{bucket}.s3.{aws_region}.amazonaws.com/{key}"


def iter_keys(
    bucket: str, prefix: str, max_keys: int | None = None, client=None
) -> Generator[dict, None, None]:
    """Yields objects in an S3 bucket for a given prefix.

    Args:
        bucket:             S3 bucket name
        prefix:             Key prefix
        min_object_size:    Minimum object size (in bytes) to keep
        max_keys:           Limits results to this size
    """
    client = client or _get_client()
    key_count = 0
    kwargs = {"Bucket": bucket, "Prefix": prefix}

    while True:
        resp = client.list_objects_v2(**kwargs)
        if resp["KeyCount"] == 0:
            break

        for obj in resp["Contents"]:
            key_count += 1
            yield obj

            if max_keys and key_count >= max_keys:
                break

        if resp["IsTruncated"]:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        else:
            break


def sync_object(src: dict, dest: dict, client=None) -> None:
    """
    Copy an object from one S3 location to another but only if the
    source is newer than the destination.

    Args:
        src: the source object URL in {"Bucket": "", "Key": ""} format
        dest: the source object URL in {"Bucket": "", "Key": ""} format
    """
    client = client or _get_client()

    # Check if the destination exists
    try:
        resp = client.head_object(**dest)
        dest_last_modified = resp["LastModified"]
    except ClientError:
        # This assumes a 404 - could also be some other issue, in which case the copy will fail anyway
        pass
    else:
        # Check if the source is newer
        resp = client.head_object(**src)
        src_last_modified = resp["LastModified"]
        if src_last_modified <= dest_last_modified:
            return

    client.copy(CopySource=src, **dest)


def upload_file(
    source_path: str,
    bucket: str,
    key: str,
    content_type=None,
    content_disposition=None,
    client=None,
):
    """
    Uploads a file on disk to S3, setting ContentType and ContentDisposition attributes if supplied.
    """
    client = client or _get_client()

    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if content_disposition:
        extra_args["ContentDisposition"] = content_disposition

    client.upload_file(source_path, Bucket=bucket, Key=key, ExtraArgs=extra_args)


def upload_dir(source_path: str, bucket: str, prefix: str, client=None):
    """
    Uploads all files in a directory to the specified bucket and prefix.
    """
    client = client or _get_client()
    manager = transfer.create_transfer_manager(
        client, transfer.TransferConfig(use_threads=True, max_concurrency=20)
    )
    for root, _, files in os.walk(source_path):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, source_path)
            s3_path = os.path.join(prefix, relative_path)
            manager.upload(local_path, bucket, s3_path)
    # Wait for all uploads to complete
    manager.shutdown()
