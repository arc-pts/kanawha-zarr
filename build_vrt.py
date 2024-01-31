from dotenv import load_dotenv
import s3fs

import argparse
import os
import subprocess
from typing import List

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


def build_vrt(s3url: str, out: str):
    files = glob_s3_files(s3url)
    run_gdalbuildvrt(out, files)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build VRT from S3 data")
    parser.add_argument("s3url", help="S3 URL to the data, including wildcards")
    parser.add_argument("--out", help="Output VRT file name")
    args = parser.parse_args()
    build_vrt(args.s3url, args.out)
