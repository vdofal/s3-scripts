# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Small toolbox for moving client raw data between Windows machines and
`s3://spot-client-data/raw-data/<project>`: two Git Bash scripts (upload, audit) and one
Python script (download + completeness check). `upload.log` and per-project folders at the
repo root are run artifacts, not code.

Everything here touches real client data. **Never run the scripts against S3 to test.**
Check bash with `bash -n`, test Python with the moto-based unit tests.

## Scripts

### upload.sh: sync local dirs/files to S3

```bash
./upload.sh /d/<client>/Raw_data/<project>          # one dir
./upload.sh /d/dir1 /d/dir2 10MB/s                   # multiple dirs, bandwidth cap
```

- Last arg matching `N[KMGT]B/s` is a bandwidth cap; everything else is a source.
- A **directory** syncs its top-level files only (`--exclude "*/*"`, no recursion) to
  `raw-data/<dirname>`. A **file** goes to `raw-data/<parent-dirname>/<filename>`.
  Keep this non-recursive unless explicitly asked.
- Uses `AWS_PROFILE=playground` and **mutates global AWS CLI config**
  (`aws configure set default.s3.max_concurrent_requests/multipart_chunksize/multipart_threshold/max_bandwidth`);
  these settings persist after the script exits.
- Appends to `upload.log`; aws output is filtered to upload/error lines and deduped.
- Paths must be Git Bash form (`/d/...`). A literal backslash in an arg means the user
  quoted *and* escaped spaces; the script hints at this on "source does not exist".

### list-upload-dates.sh: audit what's in S3 vs local

```bash
./list-upload-dates.sh /d/<project> s3://spot-client-data/raw-data/<project> [out.tsv]
```

Writes a TSV of `<upload date>\t<relative path>` sorted oldest-first, files missing from
S3 at the end as `MISSING-IN-S3`. Scans local files recursively (`find`). Uses
`AWS_PROFILE` if set, else `playground-key`. Fetches the S3 listing once rather than
one head-object per file.

### download.py: S3 prefix to local dir, then assert completeness

```bash
python download.py s3://spot-client-data/raw-data/<project> D:/<project> [--profile NAME]
```

- **Recursive**, keeps relative key paths. Profile: `--profile`, else `$AWS_PROFILE`,
  else `DEFAULT_PROFILE`.
- Every object is split into byte-range chunks (`CHUNK_SIZE_BYTES`, 512 MB) fetched by
  `MAX_WORKERS` threads into a preallocated `<file>.part`; finished chunk indexes are kept
  in `<file>.part.json`. A rerun resumes only missing chunks, and skips final files whose
  size already matches.
- Each chunk retried `MAX_CHUNK_ATTEMPTS` times with exponential backoff; a short read
  counts as a failure. Ranged GETs pin `IfMatch=<ETag>`, so an object changed in S3
  mid-download fails immediately (not retried) instead of mixing versions.
- A failing file is reported and the run continues. The final check compares every
  listed key to local presence + byte size (no checksums). Exit code 0 only if complete.
- Tuning lives in the `DownloadSettings` dataclass; tests pass tiny chunk sizes and zero
  backoff through it rather than patching constants.

## Python tests

Deps (`boto3`, `moto[s3]`, `parameterized`) in `requirements_tests.txt`. Run from repo root:

```bash
python -m unittest tests.test_download                                    # all
python -m unittest tests.test_download.TestDownloadAll.test_transient_error_retried  # one
pylint download.py tests/test_download.py && bandit -q download.py
```

- S3 is simulated with moto (`mock_aws`). `S3TestCase` writes a throwaway AWS
  config/credentials file and points `AWS_CONFIG_FILE` / `AWS_SHARED_CREDENTIALS_FILE` at
  it, because `boto3.Session(profile_name=...)` needs a real profile.
- Network failures are simulated with a `get_object` side effect that raises
  `EndpointConnectionError` and otherwise delegates to the real (moto) call.

## Conventions

- Use `os.path` and plain string paths in Python, not `pathlib`.
- Don't put real client or project names in docs, examples, or tests; use placeholders.
