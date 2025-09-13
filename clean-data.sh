#!/bin/bash

SOURCE_DIR="./data/balance-sync-logs"
DEST_DIR="./data-cleaned"

mkdir -p "$DEST_DIR"

find "$SOURCE_DIR" -type f -name "*.gz" | while read -r file; do
    parent_dir=$(dirname "$file")
    last_folder=$(basename "$parent_dir")
    cleaned_folder=$(echo "$last_folder" | sed 's/\[-\$LATEST\]//')
    if [[ "$cleaned_folder" == "$last_folder" ]]; then
        cleaned_folder=$(echo "$last_folder" | sed 's/\[\$LATEST\]//')
    fi
    dest_path="$DEST_DIR/$cleaned_folder"
    mkdir -p "$dest_path"
    cp "$file" "$dest_path/"
    echo "Copied: $file → $dest_path/$(basename "$file")"
done