"""Build Zarr dataset from VRTs"""

from dotenv import load_dotenv
import xarray as xr

import argparse

load_dotenv()


ZARR_CHUNKS = {
    "x": 512,
    "y": 512,
    "band": 1001,
}


def build_zarr(vrts: List[str], zarr_out: str):
    for vrt in vrts:
        ds = xr.open_dataset(vrt).squeeze(drop=True)
        ds = ds.chunk(ZARR_CHUNKS)
        ds.to_zarr(zarr_out, mode="a")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Zarr dataset from VRTs")
    parser.add_argument("vrts", nargs="+", help="VRT files")
    parser.add_argument("--zarr_out", help="Output Zarr dataset")
    args = parser.parse_args()
    build_zarr(args.vrts, args.zarr_out)