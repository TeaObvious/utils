#!/usr/bin/env python3
import argparse
import subprocess
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import shutil
import re


class ExifClient:
    """Wrapper around exiftool (sync or parallel)."""

    def __init__(self, parallel: bool = True, workers: int = 16, verbose: bool = False):
        self.parallel = parallel
        self.workers = workers
        self.verbose = verbose

    def _run_exiftool_json(self, path: Path) -> dict:
        cmd = ["exiftool", "-json", "-api", "RequestAll=3", str(path)]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            return {"SourceFile": str(path), "error": proc.stderr.strip()}
        try:
            data = json.loads(proc.stdout)
            return data[0] if data else {"SourceFile": str(path), "error": "No data"}
        except json.JSONDecodeError:
            return {"SourceFile": str(path), "error": "Invalid JSON"}

    def _read_parallel(self, files: list[Path]) -> list[dict]:
        results = []
        with ProcessPoolExecutor(max_workers=self.workers) as pool:
            future_to_file = {pool.submit(self._run_exiftool_json, f): f for f in files}
            for future in as_completed(future_to_file):
                try:
                    results.append(future.result())
                except Exception as e:
                    results.append({"SourceFile": str(future_to_file[future]), "error": str(e)})
        return results

    def _read_sync(self, files: list[Path]) -> list[dict]:
        return [self._run_exiftool_json(f) for f in files]

    def read(self, files: list[Path]) -> list[dict]:
        """Main entry: sync or parallel"""
        if self.verbose:
            mode = "parallel" if self.parallel else "sync"
            print(f"[INFO] Reading {len(files)} files with exiftool ({mode}, workers={self.workers}) …")
        if self.parallel:
            return self._read_parallel(files)
        return self._read_sync(files)


def sanitize_filename(name: str) -> str:
    """Replace unsafe characters with underscore."""
    return re.sub(r'[^A-Za-z0-9._-]', '_', name)


def extract_datetime(meta: dict) -> str | None:
    """Extract datetime string in YYYYMMDD_HHMMSS_mmmm format (4-digit millis)."""
    dt = (
        meta.get("CreateDate")
        or meta.get("DateTimeOriginal")
        or meta.get("DateTimeCreated")
    )
    subsec = (
        meta.get("SubSecTimeOriginal")
        or meta.get("SubSecTime")
        or meta.get("SubSecDateTimeOriginal")
    )

    if not dt:
        return None

    # Normalize: 2025:08:28 16:08:34 → 20250828_160834
    dt = dt.strip().replace(":", "").replace(" ", "_")

    if subsec:
        subsec = str(subsec).zfill(4)[:4]  # always 4 digits
        return f"{dt}_{subsec}"
    return dt


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
