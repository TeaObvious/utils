#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import subprocess
import sys
from pathlib import Path
from shutil import which
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple

RAW_EXTS = {"nef"}
JPG_EXTS = {"jpg", "jpeg"}

# --------------------- tag sets ---------------------

RAW_TAGS = [
    "-CreateDate",
    "-SubSecTime", "-SubSecTimeOriginal", "-SubSecTimeDigitized",
    "-OffsetTime", "-OffsetTimeOriginal", "-OffsetTimeDigitized",
    "-DateCreated", "-TimeCreated",
    # für Referenz-/Sicherheits-Filter & Debug:
    "-ShutterCount", "-MechanicalShutterCount", "-ImageNumber", "-FileNumber",
    "-LensID", "-LensModel", "-SerialNumber",
]

JPG_TAGS = [
    "-CreateDate",
    "-SubSecTime", "-SubSecTimeOriginal", "-SubSecTimeDigitized",
    "-OffsetTime", "-OffsetTimeOriginal", "-OffsetTimeDigitized",
    "-DateCreated", "-TimeCreated",
    "-ImageNumber", "-PreservedFileName",
    "-LensID", "-LensModel", "-SerialNumber",
]

# --------------------- exif helper class ---------------------

class ExifClient:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        if which("exiftool") is None:
            print("Fehler: 'exiftool' wurde nicht gefunden. Bitte installieren.", file=sys.stderr)
            sys.exit(2)

    def run_json(self, args: List[str]) -> List[Dict[str, Any]]:
        cmd = ["exiftool", "-j", "-n", "-api", "RequestAll=3"] + args
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        out = proc.stdout.decode("utf-8", errors="replace")
        return json.loads(out) if out.strip() else []

    def read_single(self, path: Path, tags: List[str]) -> Optional[Dict[str, Any]]:
        try:
            data = self.run_json(tags + [str(path)])
            return data[0] if data else None
        except subprocess.CalledProcessError:
            return None

    def scan_tree(self, root: Path, wanted_exts: set, tags: List[str]) -> List[Dict[str, Any]]:
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
            r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            if self.verbose:
                sys.stdout.write(r.stdout.decode("utf-8", errors="replace"))
                sys.stdout.write(r.stderr.decode("utf-8", errors="replace"))
            return True
        except subprocess.CalledProcessError as e:
            sys.stderr.write(e.stdout.decode("utf-8", errors="replace"))
            sys.stderr.write(e.stderr.decode("utf-8", errors="replace"))
            return False

    @staticmethod
    def _has_offset(s: str) -> bool:
        return (len(s) >= 6) and (s[-3] == ":") and (s[-6] in ["+", "-"])

    @staticmethod
    def _has_subsec(s: str) -> bool:
        return "." in s

    def compose_creation(self, metadata: Dict[str, Any]) -> Optional[str]:
        create_date = metadata.get("CreateDate")
        subsec_orig = metadata.get("SubSecTimeOriginal")
        subsec_digi = metadata.get("SubSecTimeDigitized")
        subsec_generic = metadata.get("SubSecTime")
        offset_any = metadata.get("OffsetTimeDigitized") or metadata.get("OffsetTimeOriginal") or metadata.get("OffsetTime")

        if create_date:
            s = create_date
            if not self._has_subsec(s):
                sub = subsec_digi or subsec_orig or subsec_generic
                if sub is not None and sub != "":
                    s = f"{s}.{sub}"
            if not self._has_offset(s) and offset_any:
                s = f"{s}{offset_any}"
            return s

        date_created = metadata.get("DateCreated")
        time_created = metadata.get("TimeCreated")
        if date_created and time_created:
            s = f"{date_created} {time_created}"
            sub = subsec_generic or subsec_digi or subsec_orig
            if sub is not None and sub != "" and not self._has_subsec(s):
                s = f"{s}.{sub}"
            if not self._has_offset(s) and offset_any:
                s = f"{s}{offset_any}"
            return s

        return None

    @staticmethod
    def to_int(value) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(str(value).strip())
        except Exception:
            return None

# --------------------- on-demand Referenz-Lookup ---------------------

class RefLookup:
    def __init__(self, exif: ExifClient, ref_root: Optional[Path], verbose: bool = False):
        self.exif = exif
        self.ref_root = ref_root
        self.verbose = verbose
        self.cache: Dict[str, Optional[Dict[str, Any]]] = {}

    def find(self, filename: str) -> Optional[Dict[str, Any]]:
        if not self.ref_root:
            return None

        key = filename.lower()
        if key in self.cache:
            return self.cache[key]

        candidate_paths: List[Path] = []
        direct = self.ref_root / filename
        if direct.exists():
            candidate_paths = [direct]
        else:
            candidate_paths = list(self.ref_root.rglob(filename))
            if not candidate_paths:
                for p in self.ref_root.rglob("*"):
                    if p.is_file() and p.suffix.lower() in (".jpg", ".jpeg") and p.name.lower() == key:
                        candidate_paths = [p]
                        break

        ref_metadata = None
        for path in candidate_paths:
            ref_metadata = self.exif.read_single(path, [
                "-CreateDate",
                "-SubSecTime", "-SubSecTimeOriginal", "-SubSecTimeDigitized",
                "-OffsetTime", "-OffsetTimeOriginal", "-OffsetTimeDigitized",
                "-ImageNumber", "-PreservedFileName",
                "-LensID", "-LensModel", "-SerialNumber",
            ])
            if ref_metadata:
                break

        if self.verbose:
            if ref_metadata:
                print(f"[REF] {filename} -> CreateDate={ref_metadata.get('CreateDate')} "
                      f"ImageNumber={ref_metadata.get('ImageNumber')} "
                      f"PreservedFileName={ref_metadata.get('PreservedFileName')} "
                      f"Serial={ref_metadata.get('SerialNumber')} "
                      f"Lens={ref_metadata.get('LensID') or ref_metadata.get('LensModel')}")
            else:
                print(f"[REF] {filename}: keine Referenz gefunden")

        self.cache[key] = ref_metadata
        return ref_metadata

# --------------------- main worker ---------------------

class GPSCopier:
    def __init__(self, args: argparse.Namespace):
        self.jpeg_root = Path(args.jpeg_root)
        self.raw_root = Path(args.raw_root)
        self.ref_root = Path(args.ref_jpeg_root) if args.ref_jpeg_root else None
        self.raw_cache_path = Path(args.raw_cache)
        self.reindex = bool(args.reindex)
        self.dry_run = bool(args.dry_run)
        self.verbose = bool(args.verbose)
        self.require_serial = bool(args.require_serial)
        self.require_lens = bool(args.require_lens_id)

        if not self.jpeg_root.is_dir() or not self.raw_root.is_dir():
            print("Fehler: Ordner nicht gefunden.", file=sys.stderr)
            sys.exit(2)
        if self.ref_root and not self.ref_root.is_dir():
            print(f"Fehler: Referenz-Ordner nicht gefunden: {self.ref_root}", file=sys.stderr)
            sys.exit(2)

        self.exif = ExifClient(verbose=self.verbose)
        self.ref_lookup = RefLookup(self.exif, self.ref_root, verbose=self.verbose)

        self.by_creation: Dict[str, List[Dict[str, Any]]] = {}
        self.by_shutter: Dict[int, List[Dict[str, Any]]] = {}
        self.by_image_number: Dict[int, List[Dict[str, Any]]] = {}
        self.by_name: Dict[str, List[Dict[str, Any]]] = {}
        self.by_basename: Dict[str, List[Dict[str, Any]]] = {}

    # --------------- scanning & indexing ---------------

    def load_raw_metadata(self) -> List[Dict[str, Any]]:
        if self.raw_cache_path.exists() and not self.reindex:
            if self.verbose:
                print(f"[CACHE] Lade RAW-Metadaten aus {self.raw_cache_path}")
            try:
                with open(self.raw_cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                if self.verbose:
                    print("[CACHE] Konnte Cache nicht lesen, scanne neu …")

        print("Scanne RAWs …")
        raw_metadata_list = self.exif.scan_tree(self.raw_root, RAW_EXTS, RAW_TAGS)
        try:
            if self.verbose:
                print(f"[CACHE] Speichere RAW-Metadaten nach {self.raw_cache_path}")
            self.raw_cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.raw_cache_path, "w", encoding="utf-8") as f:
                json.dump(raw_metadata_list, f, ensure_ascii=False)
        except Exception as e:
            print(f"[WARN] RAW-Cache konnte nicht geschrieben werden: {self.raw_cache_path} ({e})")
        return raw_metadata_list

    def build_raw_indices(self, raw_metadata_list: List[Dict[str, Any]]):
        by_creation: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        by_shutter: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        by_image_number: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        by_name: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        by_basename: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for metadata in raw_metadata_list:
            shutter_count = self.exif.to_int(metadata.get("ShutterCount"))
            if shutter_count is None:
                shutter_count = self.exif.to_int(metadata.get("MechanicalShutterCount"))
            image_number = self.exif.to_int(metadata.get("ImageNumber"))
            if shutter_count is None and image_number is not None:
                shutter_count = image_number

            entry = {
                "path": Path(metadata["SourceFile"]),
                "CreateKey": self.exif.compose_creation(metadata),
                "ShutterCount": shutter_count,
                "MechanicalShutterCount": self.exif.to_int(metadata.get("MechanicalShutterCount")),
                "ImageNumber": image_number,
                "FileNumber": self.exif.to_int(metadata.get("FileNumber")),
                "SerialNumber": metadata.get("SerialNumber"),
                "LensID": metadata.get("LensID") or metadata.get("LensModel"),
            }

            if entry["CreateKey"]:
                by_creation[entry["CreateKey"]].append(entry)
            if entry["ShutterCount"] is not None:
                by_shutter[entry["ShutterCount"]].append(entry)
            if entry["ImageNumber"] is not None:
                by_image_number[entry["ImageNumber"]].append(entry)

            name_lower = entry["path"].name.lower()
            base_lower = entry["path"].stem.lower()
            by_name[name_lower].append(entry)
            by_basename[base_lower].append(entry)

        self.by_creation = by_creation
        self.by_shutter = by_shutter
        self.by_image_number = by_image_number
        self.by_name = by_name
        self.by_basename = by_basename

        print(f"RAW-Dateien indexiert: {sum(len(v) for v in by_creation.values())} | Keys={len(by_creation)}")

    # --------------- helpers ---------------

    @staticmethod
    def _norm(s: Optional[str]) -> Optional[str]:
        return s.lower().strip() if isinstance(s, str) else None

    def _apply_lens_serial_requirements(
        self,
        candidates: List[Dict[str, Any]],
        jpeg_meta: Dict[str, Any],
        ref_meta: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        if not candidates:
            return candidates

        # Quelle für Vergleichswerte: JPEG selbst → Referenz (falls vorhanden)
        jpeg_serial = jpeg_meta.get("SerialNumber")
        jpeg_lens = jpeg_meta.get("LensID") or jpeg_meta.get("LensModel")

        ref_serial = ref_meta.get("SerialNumber") if ref_meta else None
        ref_lens = (ref_meta.get("LensID") or ref_meta.get("LensModel")) if ref_meta else None

        want_serial = jpeg_serial or ref_serial
        want_lens = jpeg_lens or ref_lens

        if self.require_serial:
            if want_serial:
                candidates = [c for c in candidates if self._norm(c.get("SerialNumber")) == self._norm(want_serial)]
            else:
                if self.verbose:
                    print("[INFO] --require-serial gesetzt, aber keine SerialNumber in JPEG/Referenz gefunden – ignoriere für dieses Bild.")

        if self.require_lens:
            if want_lens:
                candidates = [c for c in candidates if self._norm(c.get("LensID")) == self._norm(want_lens)]
            else:
                if self.verbose:
                    print("[INFO] --require-lens-id gesetzt, aber keine LensID/LensModel in JPEG/Referenz gefunden – ignoriere für dieses Bild.")

        return candidates

    def _apply_imagenumber_fallback(self, image_number: Optional[int], creation_key: Optional[str]) -> List[Dict[str, Any]]:
        if image_number is None:
            return []
        candidates = list({e["path"]: e for e in self.by_shutter.get(image_number, [])}.values())
        if len(candidates) > 1 and creation_key:
            narrowed = [e for e in candidates if e.get("CreateKey") == creation_key]
            return narrowed or candidates
        return candidates

    def _apply_preserved_name_fallback(self, preserved_name: Optional[str], creation_key: Optional[str]) -> List[Dict[str, Any]]:
        if not preserved_name:
            return []
        exact_key = preserved_name.lower()
        base_key = Path(preserved_name).stem.lower()
        by_name = self.by_name.get(exact_key, [])
        by_base = self.by_basename.get(base_key, [])
        candidates = list({e["path"]: e for e in (by_name + by_base)}.values())
        if len(candidates) > 1 and creation_key:
            narrowed = [e for e in candidates if e.get("CreateKey") == creation_key]
            return narrowed or candidates
        return candidates

    # --------------- matching ---------------

    def match_jpeg_to_raw(self, jpeg_metadata: Dict[str, Any], use_reference: bool = True
                          ) -> Tuple[Optional[Dict[str, Any]], bool, bool]:
        """
        Liefert (best_raw, used_reference_fallback, was_ambiguous)
        """
        jpeg_path = Path(jpeg_metadata["SourceFile"])
        used_ref = False

        creation_key = self.exif.compose_creation(jpeg_metadata)
        candidate_list: List[Dict[str, Any]] = []
        if creation_key:
            candidate_list = list({e["path"]: e for e in self.by_creation.get(creation_key, [])}.values())

        # Erst: Fallbacks aus DIESEM JPEG
        if not candidate_list or len(candidate_list) > 1:
            image_number_self = self.exif.to_int(jpeg_metadata.get("ImageNumber"))
            preserved_self = jpeg_metadata.get("PreservedFileName")
            c_sc = self._apply_imagenumber_fallback(image_number_self, creation_key)
            if c_sc:
                candidate_list = c_sc
            if (not candidate_list or len(candidate_list) > 1) and preserved_self:
                c_name = self._apply_preserved_name_fallback(preserved_self, creation_key)
                if c_name:
                    candidate_list = c_name

        # Optional: Referenz-JPEG (wenn vorhanden)
        ref_metadata = None
        if (not candidate_list or len(candidate_list) > 1) and use_reference and self.ref_root:
            ref_metadata = self.ref_lookup.find(jpeg_path.name)
            if ref_metadata:
                if not creation_key:
                    ref_key = self.exif.compose_creation(ref_metadata)
                    if ref_key:
                        creation_key = ref_key
                image_number_ref = self.exif.to_int(ref_metadata.get("ImageNumber"))
                c_sc = self._apply_imagenumber_fallback(image_number_ref, creation_key)
                if len(c_sc) == 1:
                    candidate_list = c_sc
                    used_ref = True
                elif len(c_sc) > 1:
                    candidate_list = c_sc
                if not candidate_list or len(candidate_list) > 1:
                    preserved_ref = ref_metadata.get("PreservedFileName")
                    c_name = self._apply_preserved_name_fallback(preserved_ref, creation_key)
                    if len(c_name) == 1:
                        candidate_list = c_name
                        used_ref = True
                    elif len(c_name) > 1:
                        candidate_list = c_name

        # Sicherheitsfilter: --require-serial / --require-lens-id
        if candidate_list and (self.require_serial or self.require_lens):
            candidate_list = self._apply_lens_serial_requirements(candidate_list, jpeg_metadata, ref_metadata)

        if not candidate_list:
            return None, used_ref, False
        if len(candidate_list) > 1:
            if self.verbose:
                print(f"[AMB ] Mehrdeutig für {jpeg_path.name} (Kandidaten: {len(candidate_list)})"
                      f" | Key={creation_key}")
                for c in candidate_list:
                    print("       -> {name} | Serial={sn} | Lens={ln} | ShutterCount={sc} | CreateKey={ck}"
                          .format(name=c['path'].name,
                                  sn=c.get('SerialNumber'), ln=c.get('LensID'),
                                  sc=c.get('ShutterCount'), ck=c.get('CreateKey')))
            return None, used_ref, True

        return candidate_list[0], used_ref, False

    # --------------- main flow ---------------

    def run(self):
        raw_metadata_list = self.load_raw_metadata()
        self.build_raw_indices(raw_metadata_list)

        print("Scanne JPEGs …")
        jpeg_metadata_list = self.exif.scan_tree(self.jpeg_root, JPG_EXTS, JPG_TAGS)

        missing_creation_key = 0
        matched = 0
        copied = 0
        ambiguous = 0
        ref_fallback = 0

        for jpeg_metadata in jpeg_metadata_list:
            jpeg_path = Path(jpeg_metadata["SourceFile"])

            if self.exif.compose_creation(jpeg_metadata) is None:
                missing_creation_key += 1

            best_raw, used_ref, was_ambiguous = self.match_jpeg_to_raw(jpeg_metadata, use_reference=True)

            if was_ambiguous:
                ambiguous += 1
                continue

            if not best_raw:
                if self.verbose:
                    print(f"[MISS] Kein eindeutiges RAW für {jpeg_path.name}")
                continue

            if used_ref:
                ref_fallback += 1

            matched += 1
            ok = self.exif.copy_gps(best_raw["path"], jpeg_path, dry_run=self.dry_run)
            if ok:
                copied += 1
                print(f"[OK] GPS: {best_raw['path'].name} -> {jpeg_path.name}")
            else:
                print(f"[ERR] GPS-Kopie fehlgeschlagen: {best_raw['path']} -> {jpeg_path}", file=sys.stderr)

        print("\n===== Zusammenfassung =====")
        print(f"JPEGs gescannt:           {len(jpeg_metadata_list)}")
        print(f"Ohne Creation-Key:        {missing_creation_key}")
        print(f"Mit Referenz-Fallback:    {ref_fallback}")
        print(f"Matches gefunden:         {matched}")
        print(f"Mehrdeutige Matches:      {ambiguous}")
        print(f"GPS erfolgreich kopiert:  {copied}" + (" (DRY-RUN)" if self.dry_run else ""))

# --------------------- CLI ---------------------

def main():
    parser = argparse.ArgumentParser(
        description="Kopiere GPS von RAW (NEF) ins JPEG. Matching: CreateDate → Fallbacks: ImageNumber/PreservedFileName (aus JPEG, dann optional aus --ref-jpeg-root). Sicherheitsfilter: --require-serial/--require-lens-id."
    )
    parser.add_argument("--jpeg-root", required=True, help="Ordner mit JPEGs (rekursiv).")
    parser.add_argument("--raw-root", required=True, help="Ordner mit RAWs (rekursiv).")
    parser.add_argument("--ref-jpeg-root", default=None, help="Optional: Ordner mit Referenz-JPEGs (öffentliche Version).")
    parser.add_argument("--raw-cache", default="/tmp/raw_exif_cache.json", help="Pfad zur RAW-Cachedatei (JSON).")
    parser.add_argument("--reindex", action="store_true", help="RAW-Verzeichnis neu scannen und Cache überschreiben.")
    parser.add_argument("--dry-run", action="store_true", help="Nur anzeigen, nichts schreiben.")
    parser.add_argument("--require-serial", action="store_true", help="Nur RAWs akzeptieren, deren SerialNumber der JPEG/Referenz entspricht.")
    parser.add_argument("--require-lens-id", action="store_true", help="Nur RAWs akzeptieren, deren LensID/LensModel der JPEG/Referenz entspricht.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Mehr Ausgaben.")
    args = parser.parse_args()

    copier = GPSCopier(args)
    copier.run()

if __name__ == "__main__":
    main()
