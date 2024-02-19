from cluster import create_cluster
from usgs_quad import UsgsQuad, find_intersecting_quads

from dask.distributed import Client
import rechunker
import s3fs
import xarray as xr

from argparse import ArgumentParser
from datetime import datetime
import os
from pathlib import Path
from typing import Optional

SOURCE_CHUNKS = {

}
TARGET_CHUNKS = {
    "x": 64,
    "y": 64,
}


def rechunk(ds: xr.Dataset, target_chunks: dict, max_mem: int, store_out: s3fs.S3Map, temp_store: Optional[s3fs.S3Map] = None):
    print("Creating plan...")
    plan = rechunker.rechunk(ds, target_chunks, max_mem, store_out, temp_store=temp_store)
    print(plan)
    print(f"Executing rechunking plan...")
    plan.execute()
    print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")


def main(zarr_in: str, zarr_out: str, zarr_temp: Optional[str] = None, rechunker_max_mem: int = 4,
         cluster_workers: int = 2, cluster_worker_vcpus: int = 4,
         cluster_worker_memory: int = 16, cluster_worker_vcpu_threads: int = 4,
         cluster_scheduler_timeout: int = 5,
         aws_key_name: Optional[str] = None, aws_secret_name: Optional[str] = None,
         usgs_quads: bool = False,
         quad_ids: Optional[str] = None):
    print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
    aws_key = os.getenv(aws_key_name)
    aws_secret = os.getenv(aws_secret_name)
    if aws_key is None or aws_secret is None:
        raise ValueError(f"AWS credentials for S3 not found ('{aws_key_name}'/'{aws_secret_name})")
    else:
        print(f"Using AWS credentials '{aws_key_name}'/'{aws_secret_name}'")
        print(f"AWS key: {aws_key[0] + '*' * (len(aws_key) - 1)}")
        print(f"AWS secret: {aws_secret[0] + '*' * (len(aws_secret) - 1)}")
    environment = {
        "AWS_ACCESS_KEY_ID": aws_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret,
    }
    print("Creating cluster...")
    cluster = create_cluster(n_workers=cluster_workers, scheduler_timeout=cluster_scheduler_timeout,
                             memory=cluster_worker_memory, vcpus=cluster_worker_vcpus,
                             threads=cluster_worker_vcpu_threads,
                             environment=environment)
    print(cluster)
    print(f"Cluster dashboard: {cluster.dashboard_link}")
    client = Client(cluster)
    print(client)
    print(f"Creating rechunking plan from {zarr_in} to {zarr_out}...")
    fs = s3fs.S3FileSystem(key=aws_key, secret=aws_secret)
    store_in = s3fs.S3Map(root=zarr_in, s3=fs)
    ds = xr.open_zarr(store_in)
    target_chunks = {
        **TARGET_CHUNKS,
        "run": ds.run.size,
    }
    max_mem = rechunker_max_mem * 1e9  # convert from GB to bytes
    if usgs_quads:
        if quad_ids:
            quads = [UsgsQuad(q) for q in quads.split(",")]
        else:
            lat_lower_left = ds.y.min().item()
            lon_lower_left = ds.x.min().item()
            lat_upper_right = ds.y.max().item()
            lon_upper_right = ds.x.max().item()
            quads = find_intersecting_quads(lat_lower_left, lon_lower_left, lat_upper_right, lon_upper_right)
        for quad in quads:
            # select data for the quad
            print(quad)
            ds_quad = ds.sel(x=slice(quad.min_lon, quad.max_lon), y=slice(quad.max_lat, quad.min_lat))
            ds_quad = ds_quad.chunk(ds.depth.encoding["preferred_chunks"])
            print(ds_quad)
            # zarr_in_quad = zarr_in.rstrip(".zarr") + f".{quad.quad_id}.zarr"
            # store_in_quad = s3fs.S3Map(root=zarr_in_quad, s3=fs)
            # # Split the input Zarr dataset by quad
            # print(f"Splitting out {quad}...")
            # ds_quad.to_zarr(store_in_quad, mode="w", consolidated=True)
            # ds_quad = xr.open_zarr(zarr_in_quad)
            print(f"Rechunking {quad}...")
            quad_zarr_out = zarr_out.rstrip(".zarr") + f".{quad.quad_id}.zarr"
            quad_zarr_temp = zarr_temp.rstrip(".zarr") + f".{quad.quad_id}.temp.zarr"
            store_out = s3fs.S3Map(root=quad_zarr_out, s3=fs)
            temp_store = s3fs.S3Map(root=quad_zarr_temp, s3=fs)
            rechunk(ds_quad, target_chunks, max_mem, store_out, temp_store=temp_store)
    else:
        store_out = s3fs.S3Map(root=zarr_out, s3=fs)
        temp_store = s3fs.S3Map(root=zarr_temp, s3=fs)
        rechunk(ds, target_chunks, max_mem, store_out, temp_store=temp_store)
    print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
    client.close()
    cluster.close()


if __name__ == "__main__":
    parser = ArgumentParser(description="Rechunk Zarr dataset using Dask on AWS Fargate")
    parser.add_argument("zarr_in", help="Zarr dataset")
    parser.add_argument("zarr_out", help="Output Zarr dataset")
    parser.add_argument("--usgs-quads", help="Separate output by USGS 7.5-minute quads", action="store_true")
    parser.add_argument("--quad-ids", type=str, help="Comma-separated list of USGS 7.5-minute quad ID")
    parser.add_argument("--zarr-temp", type=str, help="Temporary Zarr dataset for rechunking")
    parser.add_argument("--rechunker-max-mem", type=int, default=4, help="Maximum memory for rechunking in GB")
    parser.add_argument("--cluster-workers", type=int, default=2, help="Number of workers in the cluster")
    parser.add_argument("--cluster-worker-vcpus", type=int, default=4, help="Number of vCPUs per worker")
    parser.add_argument("--cluster-worker-memory", type=int, default=16, help="Memory per worker in GB")
    parser.add_argument("--cluster-worker-vcpu-threads", type=int, default=4, help="Number of threads per vCPU")
    parser.add_argument("--cluster-scheduler-timeout", type=int, default=5, help="Scheduler timeout in minutes")
    parser.add_argument("--aws-key-name", help="Name of env var for AWS Access Key ID to read/write S3 data", required=False)
    parser.add_argument("--aws-secret-name", help="Name of env var for AWS Secret Access Key to read/write S3 data", required=False)
    args = parser.parse_args()
    main(args.zarr_in, args.zarr_out,
         zarr_temp=args.zarr_temp,
         rechunker_max_mem=args.rechunker_max_mem,
         cluster_workers=args.cluster_workers,
         cluster_worker_vcpus=args.cluster_worker_vcpus,
         cluster_worker_memory=args.cluster_worker_memory,
         cluster_worker_vcpu_threads=args.cluster_worker_vcpu_threads,
         cluster_scheduler_timeout=args.cluster_scheduler_timeout,
         aws_key_name=args.aws_key_name,
         aws_secret_name=args.aws_secret_name,
         usgs_quads=args.usgs_quads,
         quad_ids=args.quad_ids)
