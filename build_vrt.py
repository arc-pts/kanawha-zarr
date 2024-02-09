from dotenv import load_dotenv
import numpy as np
import s3fs

import argparse
from datetime import datetime
import inspect
import os
from pathlib import Path
import subprocess
from typing import List, Optional
import xml.etree.ElementTree as ET

load_dotenv()


def get_s3_vrt_text(vrt: str, absolute_paths: bool = True) -> str:
    """
    Read VRT xml from S3, replacing relative paths with absolute paths
    """
    with s3fs.S3FileSystem().open(vrt, "r") as f:
        vrt_xml = f.read()
    root = ET.fromstring(vrt_xml)
    if absolute_paths:
        source_filenames = root.findall("./VRTRasterBand/ComplexSource/SourceFilename")
        for source_filename in source_filenames:
            source_filename.text = f"/vsis3/{vrt.lstrip('s3://')}"
    return ET.tostring(root, encoding="unicode")


def multiband_multivrt_xml(vrts: List[str]) -> str:
    # read the first VRT from s3 as xml
    vrt_xml = get_s3_vrt_text(vrts[0], absolute_paths=True)
    root = ET.fromstring(vrt_xml)
    for i, vrt in enumerate(vrts[1:], start=2):
        print(i)
        sub_vrt = get_s3_vrt_text(vrt, absolute_paths=True)
        sub_vrt_et = ET.fromstring(sub_vrt)
        band = sub_vrt_et.find("./VRTRasterBand")
        band.set("band", str(i))
        root.append(band)
    return ET.tostring(root, encoding="unicode")


def multiband_multivrt(out: str, vrts: List[str]):
    vrt_xml = multiband_multivrt_xml(vrts)
    with open(out, "w") as f:
        f.write(vrt_xml)


def run_gdalbuildvrt(out: str, files: List[str]):
    cmd = [
        "gdalbuildvrt",
        out,
        *files,
    ]
    subprocess.run(cmd, env=os.environ)


def get_s3_bucket_from_url(s3url: str):
    return s3url.split("/")[2]


def glob_s3_files(s3url: str) -> List[str]:
    s3 = s3fs.S3FileSystem()
    files = s3.glob(s3url)
    return [f"/vsis3/{file}" for file in files]


def max_pixel_value(in_ar: np.ndarray, out_ar: np.ndarray, xoff, yoff, xsize, ysize,
                    raster_xsize, raster_ysize, buf_radius, gt, **kwargs) -> int:
    np.amax(in_ar, axis=0, out=out_ar) 


def set_max_pixelfunc(vrt: Path):
    with open(vrt, "r") as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if "<VRTRasterBand" in line:
            lines[i] = f'  <VRTRasterBand dataType="Float32" band="1" subClass="VRTDerivedRasterBand">\n'
            lines.insert(i + 1, f'    <PixelFunctionType>max_pixel_value</PixelFunctionType>\n')
            lines.insert(i + 2, f'    <PixelFunctionLanguage>Python</PixelFunctionLanguage>\n')
            lines.insert(i + 3, f'    <PixelFunctionCode><![CDATA[\n')
            lines.insert(i + 4, "import numpy as np\n")
            lines.insert(i + 5, inspect.getsource(max_pixel_value))
            lines.insert(i + 6, f'    ]]></PixelFunctionCode>\n')
            break
    with open(vrt, "w") as f:
        f.writelines(lines)


def build_vrt(s3url: str, out: str, runs: Optional[int] = None, max_pixel_value: bool = False,
              multiband: bool = False):
    d1 = datetime.now()
    if runs:
        for i in range(1, runs + 1):
            s3url_run = s3url.format(run=i)
            files = glob_s3_files(s3url_run)
            out_path = Path(out).with_suffix(f".{i}.vrt")
            print(f"Run {i} of {runs}...")
            run_gdalbuildvrt(out_path, files)
            if max_pixel_value:
                set_max_pixelfunc(out_path)
    else:
        files = glob_s3_files(s3url)
        if multiband:
            multiband_multivrt(out, files)
        else:
            run_gdalbuildvrt(out, files)
        if max_pixel_value:
            set_max_pixelfunc(out)
    duration = datetime.now() - d1
    print(f"Done. Took: {duration.total_seconds() / 60:.2f} minutes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build VRT from S3 data")
    parser.add_argument("s3url", help="S3 URL to the data, including wildcards")
    parser.add_argument("out", help="Output VRT file name")
    parser.add_argument("--runs", type=int,
                        help="Number of runs to process, if S3 URL includes '{run}'",
                        default=None, required=False)
    parser.add_argument("--max_pixel_value", action="store_true",
                        help="Set max pixel value pixel function")
    parser.add_argument("--multiband", action="store_true",
                        help="Build multiband VRT")
    args = parser.parse_args()
    build_vrt(args.s3url, args.out, args.runs, args.max_pixel_value, args.multiband)
