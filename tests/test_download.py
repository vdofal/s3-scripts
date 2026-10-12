"""Tests for download.py, against a moto-simulated S3 bucket and a temporary directory."""
import io
import os
import glob
import shutil
import tempfile
import unittest
from unittest.mock import patch

import boto3
from botocore.exceptions import EndpointConnectionError
from moto import mock_aws
from parameterized import parameterized

import download
from download import DownloadSettings

BUCKET = "test-bucket"
PREFIX = "raw-data/LN18201/"
S3_URI = f"s3://{BUCKET}/{PREFIX}"
PROFILE = "test-profile"
SMALL_CHUNK_BYTES = 4
FAST_SETTINGS = DownloadSettings(
    chunk_size_bytes=SMALL_CHUNK_BYTES, max_workers=2, max_attempts=3, retry_base_delay_seconds=0
)
SINGLE_ATTEMPT_SETTINGS = DownloadSettings(
    chunk_size_bytes=SMALL_CHUNK_BYTES, max_workers=2, max_attempts=1, retry_base_delay_seconds=0
)
TEN_BYTES = b"0123456789"


def read_file(path):
    """Return the bytes of a local file."""
    with open(path, "rb") as file:
        return file.read()


def write_file(path, content):
    """Write bytes to a local file, creating parent directories."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as file:
        file.write(content)


def transient_failure(real_get_object, should_fail):
    """Build a get_object side effect raising a network error when should_fail(kwargs)."""
    def side_effect(**kwargs):
        if should_fail(kwargs):
            raise EndpointConnectionError(endpoint_url="https://s3.test")
        return real_get_object(**kwargs)
    return side_effect


class TestParseS3Uri(unittest.TestCase):
    """parse_s3_uri splits and normalizes S3 URIs."""

    @parameterized.expand([
        ("trailing_slash", "s3://bucket/raw/LN/", "raw/LN/"),
        ("no_trailing_slash", "s3://bucket/raw/LN", "raw/LN/"),
        ("bucket_only", "s3://bucket", ""),
    ])
    def test_valid_uri(self, _name, uri, expected_prefix):
        """Valid uri."""
        location = download.parse_s3_uri(uri)
        self.assertEqual({"bucket": "bucket", "prefix": expected_prefix}, location,
                         f"wrong split of {uri}")

    @parameterized.expand([
        ("local_path", "/d/LN18201"),
        ("missing_bucket", "s3:///raw/LN"),
    ])
    def test_invalid_uri_raises(self, _name, uri):
        """Invalid uri raises."""
        with self.assertRaises(ValueError, msg=f"{uri} should be rejected"):
            download.parse_s3_uri(uri)


class TestComputeChunks(unittest.TestCase):
    """compute_chunks produces inclusive, contiguous byte ranges."""

    def test_last_chunk_is_shorter(self):
        """Last chunk is shorter."""
        chunks = download.compute_chunks(10, 4)
        self.assertListEqual([
            {"index": 0, "start": 0, "end": 3},
            {"index": 1, "start": 4, "end": 7},
            {"index": 2, "start": 8, "end": 9},
        ], chunks, "ranges should cover all 10 bytes")

    def test_exact_multiple(self):
        """Exact multiple."""
        chunks = download.compute_chunks(8, 4)
        self.assertEqual(2, len(chunks), "8 bytes in 4-byte chunks is 2 chunks")


class TestLocalPathFor(unittest.TestCase):
    """local_path_for keeps files inside the target directory."""

    def test_nested_key(self):
        """Nested key."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = download.local_path_for(tmp_dir, "sub/file.sgy")
            self.assertEqual(os.path.join(os.path.realpath(tmp_dir), "sub", "file.sgy"), path,
                             "nested key should map to nested path")

    def test_key_escaping_target_raises(self):
        """Key escaping target raises."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            with self.assertRaises(download.UnsafeKeyError, msg="'..' must be refused"):
                download.local_path_for(tmp_dir, "../escape.txt")


class TestCheckCompleteness(unittest.TestCase):
    """check_completeness compares listing and local files without AWS."""

    def setUp(self):
        self.local_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.local_dir)

    def test_complete(self):
        """Complete."""
        write_file(os.path.join(self.local_dir, "a.bin"), TEN_BYTES)
        result = download.check_completeness({"a.bin": {"size_bytes": 10, "etag": "x"}},
                                             self.local_dir)
        self.assertEqual({"missing": [], "size_mismatch": []}, result, "should be complete")

    def test_missing_and_size_mismatch(self):
        """Missing and size mismatch."""
        write_file(os.path.join(self.local_dir, "short.bin"), b"123")
        objects = {
            "short.bin": {"size_bytes": 10, "etag": "x"},
            "sub/absent.bin": {"size_bytes": 5, "etag": "y"},
            "../unsafe.bin": {"size_bytes": 5, "etag": "z"},
        }
        result = download.check_completeness(objects, self.local_dir)
        self.assertListEqual(["../unsafe.bin", "sub/absent.bin"], result["missing"],
                             "absent and unsafe keys should be missing")
        self.assertListEqual(
            [{"key": "short.bin", "expected_bytes": 10, "actual_bytes": 3}],
            result["size_mismatch"], "short file should be a size mismatch")


class S3TestCase(unittest.TestCase):
    """Base: moto S3 bucket, AWS profile files, temp target dir, silenced stdout."""

    def setUp(self):
        tmp_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp_path)
        self.local_dir = os.path.join(tmp_path, "out")
        config_path = os.path.join(tmp_path, "aws_config")
        write_file(config_path, f"[profile {PROFILE}]\nregion = us-east-1\n".encode())
        credentials_path = os.path.join(tmp_path, "aws_credentials")
        write_file(
            credentials_path,
            f"[{PROFILE}]\naws_access_key_id = testing\naws_secret_access_key = testing\n".encode())
        env_patch = patch.dict(os.environ, {
            "AWS_CONFIG_FILE": str(config_path),
            "AWS_SHARED_CREDENTIALS_FILE": str(credentials_path),
            "AWS_PROFILE": PROFILE,
        })
        env_patch.start()
        self.addCleanup(env_patch.stop)
        aws_mock = mock_aws()
        aws_mock.start()
        self.addCleanup(aws_mock.stop)
        stdout_patch = patch("sys.stdout", new_callable=io.StringIO)
        self.stdout = stdout_patch.start()
        self.addCleanup(stdout_patch.stop)
        self.s3 = boto3.Session(profile_name=PROFILE).client("s3")
        self.s3.create_bucket(Bucket=BUCKET)
        self.location = {"bucket": BUCKET, "prefix": PREFIX}

    def put(self, relative_key, body):
        """Upload body under the test prefix."""
        self.s3.put_object(Bucket=BUCKET, Key=PREFIX + relative_key, Body=body)

    def read_local(self, relative_path):
        """Return the bytes of a downloaded file."""
        return read_file(os.path.join(self.local_dir, relative_path))

    def listing(self):
        """List the test prefix."""
        return download.list_objects(self.s3, BUCKET, PREFIX)


class TestListObjects(S3TestCase):
    """list_objects returns relative keys recursively."""

    def test_recursive_listing_skips_markers_and_other_prefixes(self):
        """Recursive listing skips markers and other prefixes."""
        self.put("a.sgy", TEN_BYTES)
        self.put("sub/b.sgy", b"12")
        self.put("sub/", b"")
        self.s3.put_object(Bucket=BUCKET, Key="raw-data/OTHER/c.sgy", Body=b"1")
        objects = self.listing()
        self.assertListEqual(["a.sgy", "sub/b.sgy"], sorted(objects),
                             "only real objects under prefix, recursive")
        self.assertEqual(10, objects["a.sgy"]["size_bytes"], "size should come from S3")


class TestDownloadAll(S3TestCase):
    """download_all downloads, splits, retries and resumes."""

    def test_nested_tree_downloaded_byte_identical(self):
        """Nested tree downloaded byte identical."""
        self.put("a.sgy", TEN_BYTES)
        self.put("sub/deep/b.sgy", b"abc")
        self.put("empty.txt", b"")
        objects = self.listing()
        report = download.download_all(self.s3, self.location, objects, self.local_dir,
                                       FAST_SETTINGS)
        self.assertEqual({"downloaded": 3, "skipped": 0, "failed": []}, report, "all downloaded")
        self.assertEqual(TEN_BYTES, self.read_local("a.sgy"), "chunked file intact")
        self.assertEqual(b"abc", self.read_local("sub/deep/b.sgy"), "nested file")
        self.assertEqual(b"", self.read_local("empty.txt"), "empty file created")
        self.assertListEqual([], glob.glob(os.path.join(self.local_dir, "**", "*.part*"),
                                           recursive=True),
                             "no partial or progress file left")

    def test_large_file_split_in_ranged_requests(self):
        """Large file split in ranged requests."""
        self.put("a.sgy", TEN_BYTES)
        with patch.object(self.s3, "get_object", wraps=self.s3.get_object) as get_object:
            download.download_all(self.s3, self.location, self.listing(), self.local_dir,
                                  FAST_SETTINGS)
        ranges = sorted(call.kwargs["Range"] for call in get_object.call_args_list)
        self.assertListEqual(["bytes=0-3", "bytes=4-7", "bytes=8-9"], ranges,
                             "10 bytes in 4-byte chunks is 3 ranged GETs")

    def test_complete_local_file_skipped(self):
        """Complete local file skipped."""
        self.put("a.sgy", TEN_BYTES)
        write_file(os.path.join(self.local_dir, "a.sgy"), TEN_BYTES)
        with patch.object(self.s3, "get_object", wraps=self.s3.get_object) as get_object:
            report = download.download_all(self.s3, self.location, self.listing(),
                                           self.local_dir, FAST_SETTINGS)
        self.assertEqual(1, report["skipped"], "same-size file should be skipped")
        get_object.assert_not_called()

    def test_wrong_size_local_file_redownloaded(self):
        """Wrong size local file redownloaded."""
        self.put("a.sgy", TEN_BYTES)
        write_file(os.path.join(self.local_dir, "a.sgy"), b"old")
        report = download.download_all(self.s3, self.location, self.listing(), self.local_dir,
                                       FAST_SETTINGS)
        self.assertEqual(1, report["downloaded"], "wrong-size file should be downloaded again")
        self.assertEqual(TEN_BYTES, self.read_local("a.sgy"), "file replaced")

    def test_transient_error_retried(self):
        """Transient error retried."""
        self.put("a.sgy", TEN_BYTES)
        attempts = {"bytes=4-7": 0}

        def fail_first_attempt(kwargs):
            if kwargs["Range"] != "bytes=4-7":
                return False
            attempts["bytes=4-7"] += 1
            return attempts["bytes=4-7"] == 1

        side_effect = transient_failure(self.s3.get_object, fail_first_attempt)
        with patch.object(self.s3, "get_object", side_effect=side_effect):
            report = download.download_all(self.s3, self.location, self.listing(),
                                           self.local_dir, FAST_SETTINGS)
        self.assertEqual([], report["failed"], "chunk should succeed on retry")
        self.assertEqual(2, attempts["bytes=4-7"], "failing chunk requested twice")
        self.assertEqual(TEN_BYTES, self.read_local("a.sgy"), "file intact")

    def test_exhausted_retries_reported_and_partial_kept(self):
        """Exhausted retries reported and partial kept."""
        self.put("a.sgy", TEN_BYTES)
        self.put("b.sgy", b"ok")
        always_fail_a = transient_failure(self.s3.get_object,
                                          lambda kwargs: kwargs["Key"].endswith("a.sgy"))
        with patch.object(self.s3, "get_object", side_effect=always_fail_a):
            report = download.download_all(self.s3, self.location, self.listing(),
                                           self.local_dir, FAST_SETTINGS)
        self.assertEqual({"downloaded": 1, "skipped": 0, "failed": ["a.sgy"]}, report,
                         "a.sgy failed, b.sgy still downloaded")
        final_path = os.path.join(self.local_dir, "a.sgy")
        self.assertFalse(os.path.exists(final_path), "no final file on failure")
        self.assertTrue(os.path.exists(final_path + ".part"), "partial kept for resume")
        self.assertTrue(os.path.exists(final_path + ".part.json"), "progress kept")

    def test_rerun_fetches_only_unfinished_chunks(self):
        """Rerun fetches only unfinished chunks."""
        self.put("a.sgy", TEN_BYTES)
        objects = self.listing()
        fail_middle = transient_failure(self.s3.get_object,
                                        lambda kwargs: kwargs["Range"] == "bytes=4-7")
        with patch.object(self.s3, "get_object", side_effect=fail_middle):
            first = download.download_all(self.s3, self.location, objects, self.local_dir,
                                          SINGLE_ATTEMPT_SETTINGS)
        self.assertEqual(["a.sgy"], first["failed"], "first run should fail on middle chunk")
        with patch.object(self.s3, "get_object", wraps=self.s3.get_object) as get_object:
            second = download.download_all(self.s3, self.location, objects, self.local_dir,
                                           FAST_SETTINGS)
        self.assertEqual(1, second["downloaded"], "second run should finish the file")
        self.assertListEqual(["bytes=4-7"],
                             [call.kwargs["Range"] for call in get_object.call_args_list],
                             "only the missing chunk is fetched again")
        self.assertEqual(TEN_BYTES, self.read_local("a.sgy"), "file intact")

    def test_object_changed_after_listing_fails_without_retry(self):
        """Object changed after listing fails without retry."""
        self.put("a.sgy", TEN_BYTES)
        objects = self.listing()
        self.put("a.sgy", b"9876543210")
        with patch.object(self.s3, "get_object", wraps=self.s3.get_object) as get_object:
            report = download.download_all(
                self.s3, self.location, objects, self.local_dir,
                DownloadSettings(chunk_size_bytes=16, max_attempts=3, retry_base_delay_seconds=0))
        self.assertEqual(["a.sgy"], report["failed"], "changed object should fail")
        self.assertEqual(1, get_object.call_count, "precondition failure is not retried")

    def test_unsafe_key_reported_as_failed(self):
        """Unsafe key reported as failed."""
        objects = {"../escape.sgy": {"size_bytes": 1, "etag": "x"}}
        report = download.download_all(self.s3, self.location, objects, self.local_dir,
                                       FAST_SETTINGS)
        self.assertEqual(["../escape.sgy"], report["failed"], "unsafe key must not be written")


class TestMain(S3TestCase):
    """main wires listing, download and verification into an exit code."""

    def test_complete_download_exit_code(self):
        """Complete download exit code."""
        self.put("a.sgy", TEN_BYTES)
        exit_code = download.main([S3_URI, str(self.local_dir)])
        self.assertEqual(download.EXIT_COMPLETE, exit_code, "complete download exits 0")
        self.assertIn("RESULT: COMPLETE", self.stdout.getvalue(), "verdict printed")

    def test_incomplete_download_exit_code(self):
        """Incomplete download exit code."""
        self.put("a.sgy", TEN_BYTES)
        always_fail = transient_failure(self.s3.get_object, lambda kwargs: True)
        fast_retry = DownloadSettings(max_attempts=1)
        with patch("download.DownloadSettings", return_value=fast_retry), \
                patch("boto3.session.Session.client", return_value=self.s3), \
                patch.object(self.s3, "get_object", side_effect=always_fail):
            exit_code = download.main([S3_URI, str(self.local_dir)])
        self.assertEqual(download.EXIT_INCOMPLETE, exit_code, "failed download exits 1")
        self.assertIn("MISSING  a.sgy", self.stdout.getvalue(), "missing file listed")

    def test_empty_prefix_exit_code(self):
        """Empty prefix exit code."""
        exit_code = download.main([S3_URI, str(self.local_dir)])
        self.assertEqual(download.EXIT_INCOMPLETE, exit_code, "empty listing exits 1")
        self.assertIn("no objects found", self.stdout.getvalue(), "error printed")

    def test_invalid_uri_exits_with_usage_error(self):
        """Invalid uri exits with usage error."""
        with patch("sys.stderr", new_callable=io.StringIO), \
                self.assertRaises(SystemExit, msg="argparse should reject bad URI"):
            download.main(["/d/LN18201", str(self.local_dir)])


if __name__ == "__main__":
    unittest.main()
