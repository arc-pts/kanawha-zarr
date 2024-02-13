"""Build Zarr dataset"""
from build_vrt import build_vrt
from cluster import create_cluster

import botocore
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from dask.distributed import Client
import rasterio
from rasterio.session import AWSSession
import s3fs
import xarray as xr

import argparse
from datetime import datetime
import os
from pathlib import Path
from typing import List, Optional

load_dotenv()

ZARR_CHUNKS = {
    "x": 2048,
    "y": 2048,
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


def build_zarr_from_vrt(vrt: str, zarr_out: str, aws_key: Optional[str] = None,
                        aws_secret: Optional[str] = None):
    # TODO: pull run number from VRT filename
    # TODO: skip a VRT if it already exists in the Zarr dataset
    # TODO: skip a VRT if it doesn't exist in S3
    # TODO: batch process a subset of multiple VRTs to minimize cluster idle time
    fs = s3fs.S3FileSystem(key=aws_key, secret=aws_secret)
    session = AWSSession(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    with rasterio.Env(session=session):
        ds = xr.open_dataset(vrt, engine="rasterio")
    ds = ds.rename({"band": "run", "band_data": "depth"})
    ds = ds.chunk(ZARR_CHUNKS)
    store = s3fs.S3Map(root=zarr_out, s3=fs)
    try:
        existing_ds = xr.open_zarr(store)
        if "run" in existing_ds.dims:
            mode = "a"
            append_dim = "run"
            run = existing_ds.run.max().item() + 1
            ds["run"] = [run]
        else:
            mode = "w"
            append_dim = None
    except Exception as e:  # barf... having trouble handling NoSuchKey error
        mode = "w"
        append_dim = None
    ds.to_zarr(store, mode=mode, write_empty_chunks=False, append_dim=append_dim)


def build_zarr_from_multiband_vrt(vrt: str, zarr_out: str, aws_key: Optional[str] = None,
                                  aws_secret: Optional[str] = None):
    fs = s3fs.S3FileSystem(key=aws_key, secret=aws_secret)
    session = AWSSession(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    with rasterio.Env(session=session):
        ds = xr.open_dataset(vrt, engine="rasterio")
    ds = ds.rename({"band": "run", "band_data": "depth"})
    # wide chunking!!! narrow chunking upfront is waaaay to slow
    ds = ds.chunk({
        "run": 1,
        "x": 4096,
        "y": 4096,
    })
    store = s3fs.S3Map(root=zarr_out, s3=fs)
    ds.to_zarr(store, mode="w", write_empty_chunks=False, consolidated=True)


def build_from_s3(s3url: str, zarr_out: str, runs: Optional[int] = None):
    if runs:
        for i in range(1, runs + 1):
            print(f"Run {i} of {runs}...")
            s3url = s3url.format(run=i)
            vrt = build_temp_vrt(s3url, zarr_out)
            build_zarr_from_vrt(vrt, zarr_out)
    else:
        vrt = build_temp_vrt(s3url, zarr_out)
        build_zarr_from_vrt(vrt, zarr_out)


def build_from_vrts(vrts: List[str], zarr_out: str, aws_key: Optional[str] = None,
                    aws_secret: Optional[str] = None):
    for i, vrt in enumerate(vrts):
        print(f"Processing {i + 1} of {len(vrts)} VRTs...")
        build_zarr_from_vrt(vrt, zarr_out, aws_key=aws_key, aws_secret=aws_secret)


def build_zarr(multiband_vrt: Optional[str], vrts: Optional[List[str]], s3url: Optional[str],
               runs: Optional[int], zarr_out: str, cluster: bool = False,
               cluster_workers: int = 2, 
               cluster_worker_vcpus: int = 4, cluster_worker_memory: int = 16,
               cluster_worker_vcpu_threads: int = 4,
               cluster_scheduler_timeout: int = 5,
               cluster_address: Optional[str] = None, dont_shutdown: bool = False,
               aws_key_name: Optional[str] = None, aws_secret_name: Optional[str] = None):
    print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
    aws_key = os.getenv(aws_key_name)
    aws_secret = os.getenv(aws_secret_name)
    if aws_key is None or aws_secret is None:
        raise ValueError(f"AWS credentials for S3 not found ('{aws_key_name}'/'{aws_secret_name})")
    else:
        print(f"Using AWS credentials '{aws_key_name}'/'{aws_secret_name}'")
        print(f"AWS key: {aws_key[0] + '*' * (len(aws_key) - 1)}")
        print(f"AWS secret: {aws_secret[0] + '*' * (len(aws_secret) - 1)}")
    if cluster and (vrts or s3url or multiband_vrt):
        if cluster_address:
            print(f"Connecting to existing cluster at {cluster_address}...")
            client = Client(cluster_address)
        else:
            print(f"Creating new cluster with {cluster_workers} workers, scheduler timeout {cluster_scheduler_timeout} mins...")
            environment = {
                # "GDAL_VRT_ENABLE_PYTHON": "YES",
                "AWS_ACCESS_KEY_ID": aws_key,
                "AWS_SECRET_ACCESS_KEY": aws_secret,
            }
            cluster = create_cluster(n_workers=cluster_workers, scheduler_timeout=cluster_scheduler_timeout,
                                     memory=cluster_worker_memory, vcpus=cluster_worker_vcpus, threads=cluster_worker_vcpu_threads,
                                     environment=environment)
            client = Client(cluster)
        print(client)
    if vrts:
        build_from_vrts(vrts, zarr_out, aws_key=aws_key, aws_secret=aws_secret)
    elif s3url:
        build_from_s3(s3url, zarr_out, runs=runs)
    elif multiband_vrt:
        build_zarr_from_multiband_vrt(multiband_vrt, zarr_out, aws_key=aws_key, aws_secret=aws_secret)
    else:
        raise ValueError("No input provided")
    if cluster and not dont_shutdown:
        print("Closing cluster...")
        cluster.close()
    print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Zarr dataset from VRTs")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--multiband-vrt", help="Single multiband VRT file")
    group.add_argument("--vrts", nargs="+", help="Mulitple single-band VRT files")
    group.add_argument("--s3url", help="S3 URL")
    parser.add_argument("--runs", type=int,
                        help="Number of runs to process, if S3 URL includes '{run}'",
                        default=None, required=False)
    parser.add_argument("--zarr-out", help="Output Zarr dataset")
    parser.add_argument("--cluster", action="store_true", help="Run on AWS Fargate cluster")
    parser.add_argument("--cluster-workers", type=int, help="Number of workers for the cluster",
                         default=2, required=False)
    parser.add_argument("--cluster-worker-vcpus", type=int, help="Number of vCPUs per worker for the cluster",
                        default=4, required=False)
    parser.add_argument("--cluster-worker-memory", type=int, help="Memory per worker for the cluster (GB)",
                        default=16, required=False)
    parser.add_argument("--cluster-worker-vcpu-threads", type=int, help="Threads per worker vCPU for the cluster",
                        default=4, required=False)
    parser.add_argument("--cluster-scheduler-timeout", type=int, help="Scheduler timeout for the cluster (mins)",
                         default=5, required=False)
    parser.add_argument("--cluster-address", help="Address of an existing cluster to use", required=False)
    parser.add_argument("--dont-shutdown", help="Don't shut down the cluster", action="store_true")
    parser.add_argument("--aws-key-name", help="Name of env var for AWS Access Key ID to read/write S3 data", required=False)
    parser.add_argument("--aws-secret-name", help="Name of env var for AWS Secret Access Key to read/write S3 data", required=False)
    args = parser.parse_args()
    build_zarr(args.multiband_vrt, args.vrts, args.s3url, args.runs, args.zarr_out, args.cluster, args.cluster_workers,
               args.cluster_worker_vcpus, args.cluster_worker_memory, args.cluster_worker_vcpu_threads,
               args.cluster_scheduler_timeout, args.cluster_address, args.dont_shutdown,
               args.aws_key_name, args.aws_secret_name)
