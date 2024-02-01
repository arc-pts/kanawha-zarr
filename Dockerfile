FROM mambaorg/micromamba

COPY environment.yml .
RUN micromamba install -n base -f environment.yml -y

COPY .env .
COPY build_vrt.py .
COPY build_zarr.py .
