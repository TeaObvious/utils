#!/usr/bin/env python3
"""Remove Adobe-specific EXIF/XMP/Photoshop data from images."""

import argparse
from pathlib import Path

from exifclient import ExifClient


def iter_images(root: Path):
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg"}:
            yield path


def main():
    parser = argparse.ArgumentParser(description="Clean Adobe EXIF data using exiftool")
    parser.add_argument("-i", "--input", action="append", required=True,
                        help="Input folder (can be given multiple times)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--dry-run", action="store_true", help="Only simulate")
    args = parser.parse_args()

    exif = ExifClient(verbose=args.verbose, parallel=False)

    for input_dir in args.input:
        root = Path(input_dir)
        for img in iter_images(root):
            if args.verbose or args.dry_run:
                print(f"[CLEAN] {img}")
            exif.clean_adobe_metadata(img, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
