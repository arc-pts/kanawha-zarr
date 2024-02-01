"""Build Zarr dataset"""
from build_vrt import build_vrt

from dotenv import load_dotenv
import xarray as xr

import argparse
from pathlib import Path
from typing import List, Optional

load_dotenv()


ZARR_CHUNKS = {
    "x": 512,
    "y": 512,
}

def build_temp_vrt(s3url: str, zarr_out: str, run: Optional[int] = None) -> Path:
    vrt_dir = Path("vrt")
    if vrt_dir.exists():
        vrt_dir.mkdir()
    zarr_name = Path(zarr_out).name
    vrt_basename = zarr_name if not run else f"{zarr_name}-{run:04d}"
    vrt = vrt_dir / f"{vrt_basename}.vrt"
    build_vrt(s3url, vrt)
    return vrt


def build_zarr_from_vrt(vrt: str, zarr_out: str):
    ds = xr.open_dataset(vrt).squeeze(drop=True)
    ds = ds.chunk(ZARR_CHUNKS)
    ds.to_zarr(zarr_out, mode="a")


def build_from_s3(s3url: str, zarr_out: str, runs: Optional[int] = None):
    if runs:
        for i in range(1, runs + 1):
            s3url = s3url.format(run=i)
            vrt = build_temp_vrt(s3url, zarr_out)
            build_zarr_from_vrt(vrt, zarr_out)
    else:
        vrt = build_temp_vrt(s3url, zarr_out)
        build_zarr_from_vrt(vrt, zarr_out)


def build_from_vrts(vrts: List[str], zarr_out: str):
    for vrt in vrts:
        build_zarr_from_vrt(vrt, zarr_out)


def build_zarr(vrts: Optional[List[str]], s3url: Optional[str],
               runs: Optional[int], zarr_out: str):
    if vrts:
        build_from_vrts(vrts, zarr_out)
    elif s3url:
        build_from_s3(s3url, zarr_out, runs=runs)
    else:
        raise ValueError("No input provided")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Zarr dataset from VRTs")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--vrts", nargs="+", help="VRT files")
    group.add_argument("--s3url", help="S3 URL")
    parser.add_argument("--runs", type=int,
                        help="Number of runs to process, if S3 URL includes '{run}'",
                        default=None, required=False)
    parser.add_argument("--zarr_out", help="Output Zarr dataset")
    args = parser.parse_args()
    build_zarr(args.vrts, args.s3url, args.zarr_out)