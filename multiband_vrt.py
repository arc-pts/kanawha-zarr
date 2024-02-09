from dotenv import load_dotenv
import numpy as np
import s3fs

import argparse
from copy import deepcopy
from datetime import datetime
import inspect
import os
from pathlib import Path
import subprocess
from typing import List, Optional
import xml.etree.ElementTree as ET

from build_vrt import run_gdalbuildvrt

load_dotenv()


def extend_vrt_bands(vrt: Path, s3url: str, runs: int) -> str:
    with open(vrt, "r") as f:
        root = ET.fromstring(f.read())
    rootband = root.find("./VRTRasterBand")
    for i in range(2, runs + 1):
        if s3fs.S3FileSystem().exists(s3url.format(run=i)):
            band = deepcopy(rootband)
            source = band.find("./ComplexSource/SourceFilename")
            source.text = s3url.format(run=i).replace("s3://", "/vsis3/")
            band.set("band", str(i))
            root.append(band)
        else:
            print(f"Skipping run {i} because it doesn't exist in S3")
    return ET.tostring(root, encoding="unicode")


def main(s3url: str, out: str, runs: Optional[int] = None):
    """
    Create a multi-band VRT from an S3 URL pattern
    """
    tmp_out = Path(out).with_suffix(".tmp.vrt")
    run_gdalbuildvrt(tmp_out, [f"{s3url.format(run=1).replace('s3://', '/vsis3/')}"])
    extended_xml = extend_vrt_bands(tmp_out, s3url, runs)
    with open(out, "w") as f:
        f.write(extended_xml)
    os.remove(tmp_out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("s3url", help="S3 URL pattern")
    parser.add_argument("out", help="Output VRT file")
    parser.add_argument("--runs", type=int, help="Number of runs to process",
                        required=False, default=1)
    args = parser.parse_args()
    main(args.s3url, args.out, args.runs)