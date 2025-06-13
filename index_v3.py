import os
import configparser
import rasterio
from shapely.geometry import box
from dask import delayed, compute
import dask.dataframe as dd
import pandas as pd
import re

# Load config
config = configparser.ConfigParser()
config.read("config.ini")

tile_dir = config["sentinel"]["sentinel_data_dir_v2"].rstrip("/")
output_filename = config["paths"]["output_csv_v3"]
export_format = config["metadata"].get("export_format", "csv").lower()

# Set output path: tile_dir/tile_index/output_filename
output_dir = os.path.join(tile_dir, "tile_index")
os.makedirs(output_dir, exist_ok=True)
output_file_path = os.path.join(output_dir, output_filename)


@delayed
def extract_tile_metadata(fname):
    if not fname.endswith(".tif"):
        return None

    path = os.path.join(tile_dir, fname)
    tile_id = os.path.splitext(fname)[0]

    # Regex match filename pattern
    match = re.match(
        r"(T[0-9]{2}[A-Z]{3})_(\d{8}T\d{6})_([A-Z0-9]+_\d+m)_R\d+m", tile_id)
    if not match:
        print(f"Skipping unrecognized filename format: {fname}")
        return None

    utm_tile, timestamp, band = match.groups()
    resolution = band.split("_")[-1]

    try:
        with rasterio.open(path) as src:
            bounds = src.bounds
            polygon_wkt = box(bounds.left, bounds.bottom,
                              bounds.right, bounds.top).wkt

        return {
            "tile_id": tile_id,
            "utm_tile": utm_tile,
            "timestamp": timestamp,
            "layer": band,
            "resolution": resolution,
            "bbox": polygon_wkt,
            "path": os.path.abspath(path),
        }

    except Exception as e:
        print(f"Failed to read {fname}: {e}")
        return None


def main():
    print(f"Indexing raster tiles from: {tile_dir}")

    tasks = [extract_tile_metadata(f) for f in os.listdir(
        tile_dir) if f.endswith(".tif")]
    results = compute(*tasks)
    records = [r for r in results if r is not None]

    if not records:
        print("No valid tiles found.")
        return

    df = dd.from_pandas(pd.DataFrame(records), npartitions=1)

    if export_format == "csv":
        df.to_csv(output_file_path, index=False, single_file=True)
        print(f"Tile index written to: {output_file_path}")
    elif export_format == "parquet":
        df.to_parquet(output_file_path, index=False)
        print(f"Tile index written to: {output_file_path} (Parquet)")
    else:
        print(f"Unsupported export_format: {export_format}")


if __name__ == "__main__":
    main()
