#!/bin/bash

INPUT_DIR="$1"        # Full path to IMG_DATA (e.g., .../GRANULE/.../IMG_DATA)
OUTPUT_DIR="$2"       # Output directory for .tif files

if [[ -z "$INPUT_DIR" || -z "$OUTPUT_DIR" ]]; then
    echo "Usage: $0 <IMG_DATA_path> <output_dir>"
    exit 1
fi

if ! command -v gdal_translate &> /dev/null; then
    echo "gdal_translate not found. Please install GDAL first."
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

# Process each resolution folder
for RES in R10m R20m R60m; do
    RES_DIR="$INPUT_DIR/$RES"
    if [[ -d "$RES_DIR" ]]; then
        echo "Processing $RES_DIR"
        find "$RES_DIR" -type f -name "*.jp2" | while read -r JP2_FILE; do
            FILENAME=$(basename "$JP2_FILE" .jp2)
            OUT_FILE="$OUTPUT_DIR/${FILENAME}_${RES}.tif"
            echo "Converting: $JP2_FILE → $OUT_FILE"
            gdal_translate -of GTiff "$JP2_FILE" "$OUT_FILE"
        done
    else
        echo "Folder not found: $RES_DIR (skipping)"
    fi
done

echo "All conversions complete. TIFFs saved in: $OUTPUT_DIR"