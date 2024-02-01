from dotenv import load_dotenv
import s3fs

import argparse
import os
from pathlib import Path
import subprocess
from typing import List, Optional

load_dotenv()


def run_gdalbuildvrt(out: str, files: List[str]):
    cmd = [
        "gdalbuildvrt",
        out,
        *files,
    ]
    subprocess.run(cmd, env=os.environ)


def get_s3_bucket_from_url(s3url: str):
    return s3url.split("/")[2]


def glob_s3_files(s3url: str) -> List[str]:
    s3 = s3fs.S3FileSystem()
    files = s3.glob(s3url)
    return [f"/vsis3/{file}" for file in files]


def build_vrt(s3url: str, out: str, runs: Optional[int] = None):
    if runs:
        for i in range(1, runs + 1):
            s3url_run = s3url.format(run=i)
            files = glob_s3_files(s3url_run)
            out_path = Path(out).with_suffix(f".{i}.vrt")
            run_gdalbuildvrt(out_path, files)
    else:
        files = glob_s3_files(s3url)
        run_gdalbuildvrt(out, files)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build VRT from S3 data")
    parser.add_argument("s3url", help="S3 URL to the data, including wildcards")
    parser.add_argument("out", help="Output VRT file name")
    parser.add_argument("--runs", type=int,
                        help="Number of runs to process, if S3 URL includes '{run}'",
                        default=None, required=False)
    args = parser.parse_args()
    build_vrt(args.s3url, args.out, args.runs)
