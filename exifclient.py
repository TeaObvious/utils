#!/usr/bin/env python3
"""Wrapper library around exiftool with helpers for EXIF processing."""

from __future__ import annotations

import json
import re
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from shutil import which
from typing import Any, Optional


class ExifClient:
    """Thin wrapper around exiftool.

    It provides convenience helpers used by various scripts in this
    repository.  Calls to exiftool can optionally be executed in parallel
    when reading metadata for many files.
    """

    def __init__(self, parallel: bool = True, workers: int = 16, verbose: bool = False):
        if which("exiftool") is None:
            raise RuntimeError("'exiftool' not found. Please install it before using this tool.")
        self.parallel = parallel
        self.workers = workers
        self.verbose = verbose

    # ------------------------------------------------------------------
    # low level runners
    def run_json(self, args: list[str]) -> list[dict[str, Any]]:
        """Execute ``exiftool`` with the given arguments and return JSON data."""
        cmd = ["exiftool", "-json", "-n", "-api", "RequestAll=3"] + args
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd, proc.stdout, proc.stderr)
        return json.loads(proc.stdout) if proc.stdout.strip() else []

    def _read_single_file(self, path: Path) -> dict[str, Any]:
        try:
            data = self.run_json([str(path)])
            return data[0] if data else {"SourceFile": str(path), "error": "No data"}
        except subprocess.CalledProcessError as e:
            return {"SourceFile": str(path), "error": e.stderr.strip()}
        except json.JSONDecodeError:
            return {"SourceFile": str(path), "error": "Invalid JSON"}

    def read(self, files: list[Path]) -> list[dict[str, Any]]:
        """Read metadata for multiple files (optionally in parallel)."""
        if self.verbose:
            mode = "parallel" if self.parallel else "sync"
            print(f"[INFO] Reading {len(files)} files with exiftool ({mode}, workers={self.workers}) …")

        if self.parallel:
            results: list[dict[str, Any]] = []
            with ProcessPoolExecutor(max_workers=self.workers) as pool:
                future_to_file = {pool.submit(self._read_single_file, f): f for f in files}
                for future in as_completed(future_to_file):
                    results.append(future.result())
            return results

        return [self._read_single_file(f) for f in files]

    # ------------------------------------------------------------------
    # higher level helpers used by scripts
    def read_single(self, path: Path, tags: list[str]) -> Optional[dict[str, Any]]:
        try:
            data = self.run_json(tags + [str(path)])
            return data[0] if data else None
        except subprocess.CalledProcessError:
            return None

    def scan_tree(self, root: Path, wanted_exts: set[str], tags: list[str]) -> list[dict[str, Any]]:
        args = ["-r"]
        for ext in sorted(wanted_exts):
            args += ["-ext", ext]
        args += tags + [str(root)]
        return self.run_json(args)

    def copy_gps(self, raw_path: Path, jpeg_path: Path, dry_run: bool = False) -> bool:
        cmd = [
            "exiftool", "-overwrite_original_in_place", "-m",
            "-TagsFromFile", str(raw_path),
            "-GPS:all",
            str(jpeg_path),
        ]
        if dry_run:
            print(f"[DRY-RUN] {' '.join(cmd)}")
            return True
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, check=True)
            if self.verbose:
                print(r.stdout, end="")
                print(r.stderr, end="")
            return True
        except subprocess.CalledProcessError as e:
            if self.verbose:
                print(e.stdout, end="")
                print(e.stderr, end="")
            return False

    def clean_adobe_metadata(self, path: Path, dry_run: bool = False) -> bool:
        """Remove Adobe/XMP/Photoshop metadata from the given file."""
        cmd = [
            "exiftool",
            "-adobe:all=",
            "-xmp:all=",
            "-photoshop:all=",
            "-tagsfromfile",
            "@",
            "-iptc:all",
            "-overwrite_original_in_place",
            str(path),
        ]
        if dry_run:
            print(f"[DRY-RUN] {' '.join(cmd)}")
            return True
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return True
        except subprocess.CalledProcessError:
            return False

    # ----------------------------- helpers -----------------------------
    @staticmethod
    def sanitize_filename(name: str) -> str:
        """Replace unsafe characters with underscore."""
        return re.sub(r"[^A-Za-z0-9._-]", "_", name)

    @staticmethod
    def extract_datetime(meta: dict[str, Any]) -> Optional[str]:
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

        dt = dt.strip().replace(":", "").replace(" ", "_")
        if subsec:
            subsec = str(subsec).zfill(4)[:4]
            return f"{dt}_{subsec}"
        return dt

    @staticmethod
    def compose_creation(metadata: dict[str, Any]) -> Optional[str]:
        create_date = metadata.get("CreateDate")
        subsec_orig = metadata.get("SubSecTimeOriginal")
        subsec_digi = metadata.get("SubSecTimeDigitized")
        subsec_generic = metadata.get("SubSecTime")
        offset_any = metadata.get("OffsetTimeDigitized") or metadata.get("OffsetTimeOriginal") or metadata.get("OffsetTime")

        def _has_offset(s: str) -> bool:
            return (len(s) >= 6) and (s[-3] == ":") and (s[-6] in ["+", "-"])

        def _has_subsec(s: str) -> bool:
            return "." in s

        if create_date:
            s = create_date
            if not _has_subsec(s):
                sub = subsec_digi or subsec_orig or subsec_generic
                if sub:
                    s = f"{s}.{sub}"
            if not _has_offset(s) and offset_any:
                s = f"{s}{offset_any}"
            return s

        date_created = metadata.get("DateCreated")
        time_created = metadata.get("TimeCreated")
        if date_created and time_created:
            s = f"{date_created} {time_created}"
            sub = subsec_generic or subsec_digi or subsec_orig
            if sub and not _has_subsec(s):
                s = f"{s}.{sub}"
            if not _has_offset(s) and offset_any:
                s = f"{s}{offset_any}"
            return s
        return None

    @staticmethod
    def to_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(str(value).strip())
        except Exception:
            return None

    @staticmethod
    def extract_shuttercount(meta: dict[str, Any]) -> Optional[int]:
        for key in ["ShutterCount", "MechanicalShutterCount", "ImageNumber", "FileNumber"]:
            val = meta.get(key)
            try:
                if val is not None:
                    return int(str(val).strip())
            except Exception:
                continue
        return None

    @staticmethod
    def extract_lens(meta: dict[str, Any]) -> Optional[str]:
        return meta.get("LensID") or meta.get("LensModel")

    @staticmethod
    def extract_camera(meta: dict[str, Any]) -> Optional[str]:
        return meta.get("SerialNumber") or meta.get("CameraSerialNumber") or meta.get("BodySerialNumber")


# The module is intentionally importable without side effects.
# Expose common helpers at module level for convenience
sanitize_filename = ExifClient.sanitize_filename
extract_datetime = ExifClient.extract_datetime
