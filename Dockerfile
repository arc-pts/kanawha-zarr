FROM pangeo/pangeo-notebook:latest

RUN /srv/conda/envs/notebook/bin/pip install python-dotenv dask-cloudprovider
