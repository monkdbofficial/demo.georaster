import os
import configparser
import rasterio
from shapely.geometry import box
from dask import delayed, compute
import dask.dataframe as dd
import pandas as pd

# Load config
config = configparser.ConfigParser()
config.read("config.ini")

sentinel_data_dir = config["sentinel"]["sentinel_data_dir"].rstrip("/")
output_filename = config["paths"]["output_csv_v2"]
export_format = config["metadata"].get("export_format", "csv").lower()

# Set output path
output_dir = os.path.join(sentinel_data_dir, "tile_index")
os.makedirs(output_dir, exist_ok=True)
output_file_path = os.path.join(output_dir, output_filename)


@delayed
def extract_tile_metadata(root, fname):
    if not fname.endswith(".tif"):
        return None

    full_path = os.path.join(root, fname)
    try:
        with rasterio.open(full_path) as src:
            bounds = src.bounds
            tile_id = os.path.splitext(fname)[0]
            polygon_wkt = box(bounds.left, bounds.bottom,
                              bounds.right, bounds.top).wkt
            layer_name = os.path.basename(root)  # NDVI / RGB / SAR
            return {
                "tile_id": tile_id,
                "bbox": polygon_wkt,
                "path": os.path.abspath(full_path),
                "layer": layer_name
            }
    except Exception as e:
        print(f"Error reading {full_path}: {e}")
        return None


def main():
    print(f"Indexing raster tiles from: {sentinel_data_dir}")
    tasks = []

    for root, dirs, files in os.walk(sentinel_data_dir):
        for fname in files:
            tasks.append(extract_tile_metadata(root, fname))

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
