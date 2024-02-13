from cluster import create_cluster

import s3fs
import rechunker
import xarray as xr

from argparse import ArgumentParser
from datetime import datetime


TARGET_CHUNKS = {
    "x": 64,
    "y": 64,
}


def rechunk(zarr_in: str, zarr_out: str, rechunker_max_mem: int,
            cluster_workers: int, cluster_worker_vcpus: int,
            cluster_worker_memory: int, cluster_worker_vcpu_threads: int,
            cluster_scheduler_timeout: int,
            aws_key_name: str, aws_secret_name: str):
    print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
    cluster = create_cluster(n_workers=cluster_workers, scheduler_timeout=cluster_scheduler_timeout,
                             memory=cluster_worker_memory, vcpus=cluster_worker_vcpus, threads=cluster_worker_vcpu_threads,
                             environment={"AWS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY"})
    print(cluster)
    print(f"Cluster dashboard: {cluster.dashboard_link}")
    print(f"Rechunking {zarr_in} to {zarr_out}...")
    fs = s3fs.S3FileSystem(key=aws_key_name, secret=aws_secret_name)
    store_in = s3fs.S3Map(root=zarr_in, s3=fs)
    ds = xr.open_zarr(store_in)
    target_chunks = {
        **TARGET_CHUNKS,
        "run": ds.run.size,
    }
    max_mem = rechunker_max_mem * 1e9  # convert from GB to bytes
    store_out = s3fs.S3Map(root=zarr_out, s3=fs)
    rechunker.rechunk(store_in, target_chunks, max_mem, store_out)
    print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")


if __name__ == "__main__":
    parser = ArgumentParser(description="Rechunk Zarr dataset using Dask on AWS Fargate")
    parser.add_argument("zarr-in", help="Zarr dataset")
    parser.add_argument("zarr-out", help="Output Zarr dataset")
    parser.add_argument("--rechunker-max-mem", type=int, default=4, help="Maximum memory for rechunking in GB")
    parser.add_argument("--cluster-workers", type=int, default=2, help="Number of workers in the cluster")
    parser.add_argument("--cluster-worker-vcpus", type=int, default=4, help="Number of vCPUs per worker")
    parser.add_argument("--cluster-worker-memory", type=int, default=16, help="Memory per worker in GB")
    parser.add_argument("--cluster-worker-vcpu-threads", type=int, default=4, help="Number of threads per vCPU")
    parser.add_argument("--cluster-scheduler-timeout", type=int, default=5, help="Scheduler timeout in minutes")
    parser.add_argument("--aws-key-name", help="Name of env var for AWS Access Key ID to read/write S3 data", required=False)
    parser.add_argument("--aws-secret-name", help="Name of env var for AWS Secret Access Key to read/write S3 data", required=False)
    args = parser.parse_args()
    rechunk(args.zarr_in, args.zarr_out, args.rechunker_max_mem,
            args.cluster_workers, args.cluster_worker_vcpus, args.cluster_worker_memory,
            args.cluster_worker_vcpu_threads, args.cluster_scheduler_timeout,
            args.aws_key_name, args.aws_secret_name)
