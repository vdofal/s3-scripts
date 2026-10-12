"""Download an S3 prefix to a local directory and assert the download is complete.

Every object under the prefix is downloaded recursively, keeping its relative path.
Objects are split into byte-range chunks fetched in parallel; each chunk is retried
with exponential backoff, and finished chunks are recorded next to the file so a rerun
resumes where the previous run stopped. At the end, every S3 object must exist locally
with the same byte size, otherwise the script exits with a non-zero code.

Usage:
    python download.py s3://spot-client-data/raw-data/LN18201 D:/LN18201
    python download.py s3://spot-client-data/raw-data/LN18201 D:/LN18201 --profile playground
"""
import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import boto3
from botocore.exceptions import BotoCoreError, ClientError

S3_SCHEME = "s3://"
DEFAULT_PROFILE = "playground"
CHUNK_SIZE_BYTES = 512 * 1024 * 1024
STREAM_BUFFER_BYTES = 1024 * 1024
MAX_WORKERS = 8
MAX_CHUNK_ATTEMPTS = 5
RETRY_BASE_DELAY_SECONDS = 2
PART_SUFFIX = ".part"
PROGRESS_SUFFIX = ".part.json"
PRECONDITION_FAILED_CODE = "PreconditionFailed"
BYTES_PER_GB = 1024 ** 3
EXIT_COMPLETE = 0
EXIT_INCOMPLETE = 1


class ShortReadError(Exception):
    """Raised when a ranged GET returns a different byte count than requested."""


class ChunkDownloadError(Exception):
    """Raised when a chunk cannot be downloaded, after retries when retrying makes sense."""


class UnsafeKeyError(ValueError):
    """Raised when an S3 key would resolve to a path outside the target directory."""


RETRYABLE_ERRORS = (ClientError, BotoCoreError, OSError, ShortReadError)


@dataclass(frozen=True)
class DownloadSettings:
    """Tuning knobs for chunked downloads.

    Attributes:
        chunk_size_bytes: Size of each byte-range request.
        max_workers: Number of chunks downloaded in parallel for one file.
        max_attempts: Attempts per chunk before the file is reported as failed.
        retry_base_delay_seconds: Backoff base, delay is base * 2 ** attempt.
    """

    chunk_size_bytes: int = CHUNK_SIZE_BYTES
    max_workers: int = MAX_WORKERS
    max_attempts: int = MAX_CHUNK_ATTEMPTS
    retry_base_delay_seconds: float = RETRY_BASE_DELAY_SECONDS


def parse_s3_uri(uri):
    """Split an S3 URI into bucket and key prefix.

    Args:
        uri: URI like ``s3://bucket/some/prefix``.

    Returns:
        dict: ``{"bucket": str, "prefix": str}``; prefix is empty or ends with ``/``.

    Raises:
        ValueError: If the URI does not start with ``s3://`` or has no bucket.
    """
    if not uri.startswith(S3_SCHEME):
        raise ValueError(f"Not an S3 URI (expected {S3_SCHEME}bucket/prefix): {uri}")
    bucket, _, prefix = uri[len(S3_SCHEME):].partition("/")
    if bucket == "":
        raise ValueError(f"Missing bucket name in S3 URI: {uri}")
    prefix = prefix.strip("/")
    if prefix != "":
        prefix += "/"
    return {"bucket": bucket, "prefix": prefix}


def list_objects(s3_client, bucket, prefix):
    """List every object under a prefix, recursively.

    Args:
        s3_client: boto3 S3 client.
        bucket: Bucket name.
        prefix: Key prefix, empty or ending with ``/``.

    Returns:
        dict: Key relative to ``prefix`` mapped to ``{"size_bytes": int, "etag": str}``.
        Folder marker keys (ending with ``/``) are skipped.
    """
    objects = {}
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            if key.endswith("/"):
                continue
            objects[key[len(prefix):]] = {"size_bytes": entry["Size"], "etag": entry["ETag"]}
    return objects


def local_path_for(local_dir, relative_key):
    """Map a relative S3 key to its local path, refusing paths outside ``local_dir``.

    Args:
        local_dir: Target directory.
        relative_key: S3 key relative to the downloaded prefix.

    Returns:
        str: Absolute local path of the file.

    Raises:
        UnsafeKeyError: If the key escapes ``local_dir`` (for example with ``..``).
    """
    root = os.path.realpath(local_dir)
    target = os.path.realpath(os.path.join(root, *relative_key.split("/")))
    try:
        is_inside_root = target != root and os.path.commonpath([root, target]) == root
    except ValueError:
        is_inside_root = False
    if not is_inside_root:
        raise UnsafeKeyError(f"S3 key escapes target directory: {relative_key}")
    return target


def compute_chunks(size_bytes, chunk_size_bytes):
    """Split an object into inclusive byte ranges.

    Args:
        size_bytes: Object size.
        chunk_size_bytes: Maximum size of one range.

    Returns:
        list[dict]: ``{"index": int, "start": int, "end": int}`` per chunk, ``end`` inclusive.
    """
    return [
        {"index": index, "start": start, "end": min(start + chunk_size_bytes, size_bytes) - 1}
        for index, start in enumerate(range(0, size_bytes, chunk_size_bytes))
    ]


def load_finished_chunks(progress_path):
    """Read the set of finished chunk indexes, empty if no progress file exists."""
    if not os.path.exists(progress_path):
        return set()
    with open(progress_path, encoding="utf-8") as progress_file:
        return set(json.load(progress_file))


def save_finished_chunks(progress_path, finished_chunks):
    """Atomically write the set of finished chunk indexes."""
    tmp_path = progress_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as tmp_file:
        json.dump(sorted(finished_chunks), tmp_file)
    os.replace(tmp_path, progress_path)


def prepare_part_file(part_path, progress_path, size_bytes):
    """Reuse a matching partial download, or start a fresh preallocated one.

    Args:
        part_path: Partial file receiving the chunks.
        progress_path: JSON file listing finished chunk indexes.
        size_bytes: Expected final size.

    Returns:
        set: Chunk indexes already downloaded.
    """
    can_resume = (
        os.path.exists(part_path)
        and os.path.exists(progress_path)
        and os.path.getsize(part_path) == size_bytes
    )
    if can_resume:
        return load_finished_chunks(progress_path)
    with open(part_path, "wb") as part_file:
        part_file.truncate(size_bytes)
    save_finished_chunks(progress_path, set())
    return set()


def is_precondition_failed(error):
    """Tell whether an error means the S3 object changed since it was listed."""
    if not isinstance(error, ClientError):
        return False
    return error.response.get("Error", {}).get("Code") == PRECONDITION_FAILED_CODE


def download_chunk(s3_client, source, chunk, part_path, settings):
    """Download one byte range into the partial file, retrying with backoff.

    Args:
        s3_client: boto3 S3 client.
        source: ``{"bucket", "key", "etag", "size_bytes"}`` of the object.
        chunk: ``{"index", "start", "end"}`` byte range.
        part_path: Partial file, already preallocated to full size.
        settings: DownloadSettings.

    Raises:
        ChunkDownloadError: If the object changed in S3, or all attempts failed.
    """
    expected_bytes = chunk["end"] - chunk["start"] + 1
    last_error = None
    for attempt in range(settings.max_attempts):
        try:
            response = s3_client.get_object(
                Bucket=source["bucket"],
                Key=source["key"],
                IfMatch=source["etag"],
                Range=f"bytes={chunk['start']}-{chunk['end']}",
            )
            written_bytes = 0
            with open(part_path, "r+b") as part_file:
                part_file.seek(chunk["start"])
                for piece in response["Body"].iter_chunks(STREAM_BUFFER_BYTES):
                    part_file.write(piece)
                    written_bytes += len(piece)
            if written_bytes != expected_bytes:
                raise ShortReadError(f"got {written_bytes} of {expected_bytes} bytes")
            return
        except RETRYABLE_ERRORS as error:
            if is_precondition_failed(error):
                raise ChunkDownloadError(
                    f"{source['key']} changed in S3 since listing, rerun the script"
                ) from error
            last_error = error
            if attempt + 1 < settings.max_attempts:
                delay_seconds = settings.retry_base_delay_seconds * 2 ** attempt
                print(f"  retry chunk {chunk['index']} in {delay_seconds}s: {error}")
                time.sleep(delay_seconds)
    raise ChunkDownloadError(
        f"chunk {chunk['index']} of {source['key']} failed after "
        f"{settings.max_attempts} attempts: {last_error}"
    )


def download_object(s3_client, source, local_path, settings):
    """Download one object in parallel chunks, resuming a previous partial download.

    Args:
        s3_client: boto3 S3 client.
        source: ``{"bucket", "key", "etag", "size_bytes"}`` of the object.
        local_path: Final local path.
        settings: DownloadSettings.

    Raises:
        ChunkDownloadError: If any chunk fails. The partial file and progress file are
            kept so the next run resumes.
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    if source["size_bytes"] == 0:
        with open(local_path, "wb"):
            pass
        return
    part_path = local_path + PART_SUFFIX
    progress_path = local_path + PROGRESS_SUFFIX
    finished_chunks = prepare_part_file(part_path, progress_path, source["size_bytes"])
    pending_chunks = [
        chunk
        for chunk in compute_chunks(source["size_bytes"], settings.chunk_size_bytes)
        if chunk["index"] not in finished_chunks
    ]
    progress_lock = threading.Lock()

    def fetch(chunk):
        download_chunk(s3_client, source, chunk, part_path, settings)
        with progress_lock:
            finished_chunks.add(chunk["index"])
            save_finished_chunks(progress_path, finished_chunks)

    executor = ThreadPoolExecutor(max_workers=settings.max_workers)
    try:
        futures = [executor.submit(fetch, chunk) for chunk in pending_chunks]
        for future in futures:
            future.result()
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    os.replace(part_path, local_path)
    os.remove(progress_path)


def download_all(s3_client, location, objects, local_dir, settings=None):
    """Download every listed object, skipping files already complete locally.

    Args:
        s3_client: boto3 S3 client.
        location: ``{"bucket", "prefix"}`` from parse_s3_uri.
        objects: Output of list_objects.
        local_dir: Target directory.
        settings: DownloadSettings, defaults when None.

    Returns:
        dict: ``{"downloaded": int, "skipped": int, "failed": list[str]}``.
    """
    if settings is None:
        settings = DownloadSettings()
    report = {"downloaded": 0, "skipped": 0, "failed": []}
    total = len(objects)
    for position, (relative_key, info) in enumerate(sorted(objects.items()), start=1):
        try:
            local_path = local_path_for(local_dir, relative_key)
            if os.path.isfile(local_path) and os.path.getsize(local_path) == info["size_bytes"]:
                print(f"[{position}/{total}] skip, already complete: {relative_key}")
                report["skipped"] += 1
                continue
            print(f"[{position}/{total}] {relative_key} "
                  f"({info['size_bytes'] / BYTES_PER_GB:.2f} GB)")
            source = {
                "bucket": location["bucket"],
                "key": location["prefix"] + relative_key,
                "etag": info["etag"],
                "size_bytes": info["size_bytes"],
            }
            download_object(s3_client, source, local_path, settings)
            report["downloaded"] += 1
        except (ChunkDownloadError, UnsafeKeyError) as error:
            print(f"  FAILED: {error}")
            report["failed"].append(relative_key)
    return report


def check_completeness(objects, local_dir):
    """Compare listed S3 objects with local files by presence and byte size.

    Args:
        objects: Output of list_objects.
        local_dir: Target directory.

    Returns:
        dict: ``{"missing": list[str], "size_mismatch": list[dict]}``, each mismatch is
        ``{"key", "expected_bytes", "actual_bytes"}``. Both empty means complete.
    """
    missing = []
    size_mismatch = []
    for relative_key, info in sorted(objects.items()):
        try:
            local_path = local_path_for(local_dir, relative_key)
        except UnsafeKeyError:
            missing.append(relative_key)
            continue
        if not os.path.isfile(local_path):
            missing.append(relative_key)
            continue
        actual_bytes = os.path.getsize(local_path)
        if actual_bytes != info["size_bytes"]:
            size_mismatch.append({
                "key": relative_key,
                "expected_bytes": info["size_bytes"],
                "actual_bytes": actual_bytes,
            })
    return {"missing": missing, "size_mismatch": size_mismatch}


def print_summary(objects, report, completeness):
    """Print download counters and completeness verdict."""
    total_bytes = sum(info["size_bytes"] for info in objects.values())
    print("")
    print(f"Expected objects:   {len(objects)} ({total_bytes / BYTES_PER_GB:.2f} GB)")
    print(f"Downloaded:         {report['downloaded']}")
    print(f"Skipped (complete): {report['skipped']}")
    print(f"Failed:             {len(report['failed'])}")
    print(f"Missing locally:    {len(completeness['missing'])}")
    for relative_key in completeness["missing"]:
        print(f"  MISSING  {relative_key}")
    print(f"Size mismatch:      {len(completeness['size_mismatch'])}")
    for mismatch in completeness["size_mismatch"]:
        print(f"  SIZE     {mismatch['key']}: expected {mismatch['expected_bytes']}, "
              f"got {mismatch['actual_bytes']}")


def main(argv=None):
    """Parse arguments, download the prefix and verify completeness.

    Args:
        argv: Argument list, ``sys.argv[1:]`` when None.

    Returns:
        int: EXIT_COMPLETE if every object is present with the right size, else
        EXIT_INCOMPLETE.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("s3_uri", help="s3://bucket/prefix to download")
    parser.add_argument("local_dir", help="target directory")
    parser.add_argument(
        "--profile",
        default=os.environ.get("AWS_PROFILE", DEFAULT_PROFILE),
        help=f"AWS profile (default: $AWS_PROFILE or {DEFAULT_PROFILE})",
    )
    args = parser.parse_args(argv)
    try:
        location = parse_s3_uri(args.s3_uri)
    except ValueError as error:
        parser.error(str(error))

    s3_client = boto3.Session(profile_name=args.profile).client("s3")
    objects = list_objects(s3_client, location["bucket"], location["prefix"])
    if len(objects) == 0:
        print(f"ERROR: no objects found under {args.s3_uri}")
        return EXIT_INCOMPLETE

    report = download_all(s3_client, location, objects, args.local_dir)
    completeness = check_completeness(objects, args.local_dir)
    print_summary(objects, report, completeness)

    is_complete = len(completeness["missing"]) == 0 and len(completeness["size_mismatch"]) == 0
    print("RESULT: COMPLETE" if is_complete else "RESULT: INCOMPLETE")
    return EXIT_COMPLETE if is_complete else EXIT_INCOMPLETE


if __name__ == "__main__":
    sys.exit(main())
