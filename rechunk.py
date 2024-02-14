from cluster import create_cluster

from dask.distributed import Client
import rechunker
import s3fs
import xarray as xr

from argparse import ArgumentParser
from datetime import datetime
import os
from typing import Optional


TARGET_CHUNKS = {
    "x": 64,
    "y": 64,
}


def rechunk(zarr_in: str, zarr_out: str, zarr_temp: Optional[str] = None, rechunker_max_mem: int = 4,
            cluster_workers: int = 2, cluster_worker_vcpus: int = 4,
            cluster_worker_memory: int = 16, cluster_worker_vcpu_threads: int = 4,
            cluster_scheduler_timeout: int = 5,
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
    environment = {
        "AWS_ACCESS_KEY_ID": aws_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret,
    }
    print(f"Creating rechunking plan from {zarr_in} to {zarr_out}...")
    fs = s3fs.S3FileSystem(key=aws_key, secret=aws_secret)
    store_in = s3fs.S3Map(root=zarr_in, s3=fs)
    ds = xr.open_zarr(store_in)
    target_chunks = {
        **TARGET_CHUNKS,
        "run": ds.run.size,
    }
    max_mem = rechunker_max_mem * 1e9  # convert from GB to bytes
    store_out = s3fs.S3Map(root=zarr_out, s3=fs)
    temp_store = s3fs.S3Map(root=zarr_temp, s3=fs)
    plan = rechunker.rechunk(ds, target_chunks, max_mem, store_out, temp_store=temp_store)
    print(plan)
    print("Creating cluster...")
    cluster = create_cluster(n_workers=cluster_workers, scheduler_timeout=cluster_scheduler_timeout,
                             memory=cluster_worker_memory, vcpus=cluster_worker_vcpus,
                             threads=cluster_worker_vcpu_threads,
                             environment=environment)
    print(cluster)
    print(f"Cluster dashboard: {cluster.dashboard_link}")
    client = Client(cluster)
    print(client)
    print(f"Executing rechunking plan...")
    plan.execute()
    print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
    client.close()
    cluster.close()


if __name__ == "__main__":
    parser = ArgumentParser(description="Rechunk Zarr dataset using Dask on AWS Fargate")
    parser.add_argument("zarr_in", help="Zarr dataset")
    parser.add_argument("zarr_out", help="Output Zarr dataset")
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
    rechunk(args.zarr_in, args.zarr_out, args.zarr_temp, args.rechunker_max_mem,
            args.cluster_workers, args.cluster_worker_vcpus, args.cluster_worker_memory,
            args.cluster_worker_vcpu_threads, args.cluster_scheduler_timeout,
            args.aws_key_name, args.aws_secret_name)
