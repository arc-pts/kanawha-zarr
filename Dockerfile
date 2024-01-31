FROM mambaorg/micromamba

COPY .env .
COPY environment.yml .
COPY build_vrt.py .

RUN micromamba install -n base -f environment.yml -y
