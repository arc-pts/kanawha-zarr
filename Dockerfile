FROM mambaorg/micromamba

COPY .env .
COPY environment.yml .
COPY build_vrt.py .
COPY build_zarr.py .

RUN micromamba install -n base -f environment.yml -y
