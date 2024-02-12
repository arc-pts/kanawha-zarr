from dask_cloudprovider.aws import FargateCluster

import configparser
import contextlib
import os
from typing import Optional


def get_aws_credentials() -> dict[str, str]:
    parser = configparser.ConfigParser()
    parser.read(os.path.expanduser("~/.aws/config"))
    config = parser.items("default")
    parser.read(os.path.expanduser("~/.aws/credentials"))
    credentials = parser.items("default")
    all_credentials = {key.upper(): value for key, value in [*config, *credentials]}
    with contextlib.suppress(KeyError):
        all_credentials["AWS_REGION"] = all_credentials.pop("REGION")
    return all_credentials


def create_cluster(n_workers: int = 2, scheduler_timeout: int = 5,
                   memory: int = 16, vcpus: int = 4, threads: int = 4,
                   environment: Optional[dict] = {}) -> FargateCluster:
    print("Creating Fargate cluster...")
    cluster = FargateCluster(n_workers=n_workers,
                             worker_cpu=vcpus * 1024,
                             worker_mem=memory * 1024,
                             worker_nthreads=vcpus * threads,
                             image="pangeo/pangeo-notebook:latest",
                             environment=environment,
                             scheduler_timeout=f"{scheduler_timeout} minutes",
                             scheduler_cpu=2 * 1024,
                             scheduler_mem=2 * 8 * 1024,)
    print(f"Cluster: {cluster}")
    print(f"Cluster dashboard: {cluster.dashboard_link}")
    return cluster
