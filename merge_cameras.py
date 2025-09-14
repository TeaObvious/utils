#!/usr/bin/env python3
import argparse
from pathlib import Path
import shutil

from exifclient import ExifClient, sanitize_filename, extract_datetime


def main():
    parser = argparse.ArgumentParser(description="Merge photos from two cameras based on timestamps")
    parser.add_argument("-i", "--input", action="append", required=True, help="Input folder (can be given multiple times)")
    parser.add_argument("-o", "--output", required=True, help="Output folder")
    parser.add_argument("-c", "--copy", action="store_true", help="Copy instead of move")
    parser.add_argument("--no-parallel", action="store_true", help="Disable parallel exiftool calls")
    parser.add_argument("--workers", type=int, default=16, help="Number of parallel workers")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--dry-run", action="store_true", help="Only simulate")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # collect all files
    all_files = []
    for input_dir in args.input:
        p = Path(input_dir)
        all_files.extend(list(p.rglob("*.JPG")))
        all_files.extend(list(p.rglob("*.jpg")))
        all_files.extend(list(p.rglob("*.NEF")))
        all_files.extend(list(p.rglob("*.nef")))

    if args.verbose:
        print(f"[INFO] Found {len(all_files)} files in input dirs")

    # exif client
    exif = ExifClient(parallel=not args.no_parallel, workers=args.workers, verbose=args.verbose)
    metadata_list = exif.read(all_files)

    # group by datetime key
    grouped = {}
    skipped = 0
    for meta in metadata_list:
        dt_key = extract_datetime(meta)
        if not dt_key:
            skipped += 1
            if args.verbose:
                print(f"[WARN] Skipping {meta.get('SourceFile')} (no CreateDate)")
            continue
        grouped.setdefault(dt_key, []).append(meta)

    # process groups
    collisions = 0
    counter = {}
    for dt_key, items in sorted(grouped.items()):
        base_name = dt_key
        if base_name in counter:
            counter[base_name] += 1
            base_name = f"{base_name}-{counter[base_name]}"
            collisions += 1
        else:
            counter[base_name] = 0

        for meta in items:
            src = Path(meta["SourceFile"])
            ext = src.suffix.lower()
            new_name = sanitize_filename(base_name) + ext.lower()
            dst = output_dir / new_name

            if args.dry_run or args.verbose:
                action = "COPY" if args.copy else "MOVE"
                print(f"[{action}] {src} -> {dst}")
            if not args.dry_run:
                if args.copy:
                    shutil.copy2(src, dst)
                else:
                    shutil.move(src, dst)

    # summary
    print("===== Summary =====")
    print(f"Files processed:     {len(all_files)}")
    print(f"Without CreateDate:  {skipped}")
    print(f"Collisions resolved: {collisions}")


if __name__ == "__main__":
    main()
