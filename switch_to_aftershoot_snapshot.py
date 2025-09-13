#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XMP Snapshot → Top-Level Applier (Aftershoot/Lightroom)

- Scannt Ordner nach .xmp
- Listet Snapshot-Basisnamen (Zeitstempel am letzten " - " abgeschnitten) alphabetisch
- Interaktive Auswahl (Zahl + Enter)
- Für jede XMP:
  * löscht alle top-level crs:* Attribute
  * nimmt den zuletzt passenden Snapshot (ohne Zeitstempel)
  * kopiert dessen crs:* nach oben
- Ausgabe:
  * Standard: Originaldateien werden überschrieben (in-place)
  * Optional: mit --output / -o werden neue Dateien mit gleichem Namen in diesen Ordner geschrieben
- Schreiben:
  * erzwingt gewünschte Namespace-Präfixe: x, rdf, xmp, crs, crss, As
  * erhält ggf. xpacket-Wrapper (mit x:xmptk)
  * GENERISCHE Multiline-Attribute: JEDES Start-Tag mit Attributen -> jedes Attribut in eigener Zeile
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple
from xml.etree import ElementTree as ET

# --- Namespaces & gewünschte Präfixe ---
NS = {
    'x':   "adobe:ns:meta/",
    'rdf': "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    'xmp': "http://ns.adobe.com/xap/1.0/",
    'crs': "http://ns.adobe.com/camera-raw-settings/1.0/",
    'crss':"http://ns.adobe.com/camera-raw-saved-settings/1.0/",
    'As':  "http://ns.aftershoot.com/edits/1.0/",
}

def _register_all_namespaces() -> None:
    """Sorgt dafür, dass ET beim Serialisieren genau diese Präfixe verwendet (statt ns2, ns3, …)."""
    for prefix, uri in NS.items():
        ET.register_namespace(prefix, uri)

class XMPStyleApplier:
    """Scannen, Stil wählen, Snapshot-Parameter in Top-Level kopieren – mit festen Präfixen & generischer Multiline-Ausgabe."""

    # ---------- Snapshot-Helfer ----------

    @staticmethod
    def _base_style(name: str) -> str:
        """Entfernt den Zeitstempel-Teil, indem am LETZTEN ' - ' getrennt wird."""
        if not name:
            return name
        parts = name.rsplit(' - ', 1)
        return parts[0] if len(parts) == 2 else name

    @staticmethod
    def _iter_snapshots(root: ET.Element) -> Iterable[Tuple[ET.Element, str, str]]:
        """Yield (snapshot_element, full_name, base_name)."""
        for desc in root.findall('.//rdf:Description', {'rdf': NS['rdf']}):
            if desc.get(f'{{{NS["crss"]}}}Type') == 'Snapshot':
                full = desc.get(f'{{{NS["crss"]}}}Name', '') or ''
                base = XMPStyleApplier._base_style(full)
                yield desc, full, base

    # ---------- Scannen & Auswahl ----------

    def collect_base_styles(self, xmp_files: Iterable[Path]) -> List[str]:
        names: Set[str] = set()
        for f in xmp_files:
            try:
                root = ET.parse(f).getroot()
            except ET.ParseError:
                continue
            for _desc, _full, base in self._iter_snapshots(root):
                if base:
                    names.add(base)
        return sorted(names, key=str.lower)

    @staticmethod
    def choose_from_list(options: List[str]) -> str:
        if not options:
            raise SystemExit("Keine Snapshot-Namen im Ordner gefunden.")
        print("\nVerfügbare Stile (alphabetisch, Zeitstempel entfernt):")
        for i, name in enumerate(options, 1):
            print(f"  {i:2d}) {name}")
        while True:
            sel = input("\nStil-Nummer eingeben und Enter: ").strip()
            if sel.isdigit():
                idx = int(sel)
                if 1 <= idx <= len(options):
                    return options[idx - 1]
            print("Bitte eine gültige Zahl aus der Liste eingeben.")

    # ---------- Top-Level finden/ändern ----------

    @staticmethod
    def _find_top_description(root: ET.Element) -> Optional[ET.Element]:
        """
        Heuristik: Die erste rdf:Description mit crs:* Attributen ODER xmp:Rating.
        Fallback: erste rdf:Description direkt unter rdf:RDF.
        """
        find = root.findall('.//rdf:Description', {'rdf': NS['rdf']})
        for desc in find:
            if desc.get(f'{{{NS["xmp"]}}}Rating') is not None:
                return desc
            if any(k.startswith(f'{{{NS["crs"]}}}') for k in desc.attrib):
                return desc
        rdf_root = root.find('.//rdf:RDF', {'rdf': NS['rdf']})
        if rdf_root is not None:
            found = rdf_root.find('rdf:Description', {'rdf': NS['rdf']})
            if found is not None:
                return found
        return None

    @staticmethod
    def _clear_crs_attributes(elem: ET.Element) -> None:
        """Entfernt ALLE crs:* Attribute von elem (alles andere bleibt)."""
        to_delete = [k for k in elem.attrib.keys() if k.startswith(f'{{{NS["crs"]}}}')]
        for k in to_delete:
            del elem.attrib[k]

    @staticmethod
    def _find_latest_matching_snapshot(root: ET.Element, chosen_base: str) -> Optional[ET.Element]:
        """Der *letzte* Snapshot mit passendem Basisnamen – i.d.R. der jüngste."""
        last = None
        for desc, _full, base in XMPStyleApplier._iter_snapshots(root):
            if base == chosen_base:
                last = desc
        return last

    @staticmethod
    def _copy_snapshot_params_to_top(snapshot_desc: ET.Element, top_desc: ET.Element) -> None:
        """
        Kopiert ALLE crs:* Attribute aus:
           <crss:Parameters><rdf:Description crs:*="..."/></crss:Parameters>
        in das Top-Level rdf:Description.
        """
        params = snapshot_desc.find('.//crss:Parameters', {'crss': NS['crss']})
        inner = params.find('rdf:Description', {'rdf': NS['rdf']}) if params is not None else None
        if inner is None:
            raise RuntimeError("Snapshot ohne inneren Parameter-Block (rdf:Description).")
        for k, v in inner.attrib.items():
            if k.startswith(f'{{{NS["crs"]}}}'):
                top_desc.set(k, v)

    # ---------- GENERISCHE Multiline-Attributformatierung ----------

    @staticmethod
    def _format_all_start_tags_multiline(xml: str) -> str:
        """
        Generische, vorsichtige Pretty-Formatierung:
        - JEDES Opening-Tag mit Attributen wird so umgebrochen, dass jede name="value" Paarung
          in einer eigenen Zeile steht.
        - Selbstschließende Tags bleiben selbstschließend.
        - End-Tags, Kommentare, CDATA und Processing Instructions werden nicht angetastet.

        Hinweis: Wir suchen name="value" Paare; exotische Attribute (mit Entities in den Anführungszeichen)
        sind in XMP sehr unüblich, für die üblichen Lightroom-/CameraRaw-Felder ist das robust.
        """
        # Matcht ein beliebiges Start-Tag mit Attributen:
        # <TagName  attr1="..." attr2="...">  ODER  <TagName  attr1="..." ... />
        pattern = re.compile(
            r'<([A-Za-z_][\w:.\-]*)\s+([^<>]*?)(/?)>',
            flags=re.DOTALL
        )

        def repl(m: re.Match) -> str:
            tag = m.group(1)               # z.B. rdf:Description
            attrs = (m.group(2) or '').strip()
            selfclose = (m.group(3) == '/')

            # Wenn keine name="value"-Paare -> unverändert (z. B. falls nur whitespace)
            pairs = re.findall(r'([^\s=]+)="([^"]*)"', attrs)
            if not pairs:
                return m.group(0)

            lines = [f'<{tag}']
            for name, val in pairs:
                lines.append(f'  {name}="{val}"')
            if selfclose:
                lines[-1] = lines[-1] + ' />'
            else:
                lines[-1] = lines[-1] + '>'
            return "\n".join(lines)

        return pattern.sub(repl, xml)

    def _serialize_with_prefixes_and_format(
        self,
        root: ET.Element,
        out_path: Path,
        keep_xpacket: bool,
        xmptk_value: Optional[str],
    ) -> None:
        """Serialisiert mit festen Präfixen, optionalem xpacket und generischer Multiline-Attributformatierung."""
        _register_all_namespaces()
        raw = ET.tostring(root, encoding='utf-8', xml_declaration=False).decode('utf-8')
        formatted = self._format_all_start_tags_multiline(raw)

        if keep_xpacket:
            xpacket_begin = '<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>\n'
            # xmptk wieder anfügen, falls im Root fehlt & bekannt
            if xmptk_value and ' x:xmptk=' not in formatted:
                formatted = formatted.replace(
                    '<x:xmpmeta ',
                    f'<x:xmpmeta x:xmptk="{xmptk_value}" ',
                    1
                )
            xpacket_end = '\n<?xpacket end="w"?>\n'
            out_text = xpacket_begin + formatted + xpacket_end
        else:
            out_text = "<?xml version='1.0' encoding='utf-8'?>\n" + formatted

        out_path.write_text(out_text, encoding='utf-8')

    # ---------- Dateiverarbeitung ----------

    def apply_to_file(self, path: Path, chosen_base: str, output_dir: Optional[Path]) -> str:
        # Prüfe, ob Original ein xpacket enthält
        original_text = None
        try:
            original_text = path.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            pass
        had_xpacket = bool(original_text and original_text.lstrip().startswith('<?xpacket'))

        try:
            tree = ET.parse(path)
            root = tree.getroot()
        except ET.ParseError as e:
            return f"{path.name}: XML-Fehler ({e})."

        top = self._find_top_description(root)
        if top is None:
            return f"{path.name}: top-level rdf:Description nicht gefunden."

        snap = self._find_latest_matching_snapshot(root, chosen_base)
        if snap is None:
            return f"{path.name}: kein Snapshot für '{chosen_base}' gefunden."

        self._clear_crs_attributes(top)
        try:
            self._copy_snapshot_params_to_top(snap, top)
        except RuntimeError as e:
            return f"{path.name}: {e}"

        # Zielpfad
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            out_path = output_dir / path.name
        else:
            out_path = path

        # xmptk-Wert aus Root (x:xmpmeta) übernehmen, falls vorhanden
        xmptk_value = root.get(f'{{{NS["x"]}}}xmptk')

        # Schreiben mit festen Präfixen & generischer Multiline-Formatierung
        self._serialize_with_prefixes_and_format(root, out_path, keep_xpacket=had_xpacket, xmptk_value=xmptk_value)

        # kurze Vorschau
        preview = {k: top.get(f'{{{NS["crs"]}}}{k}') for k in
                   ['Temperature', 'Tint', 'Exposure2012', 'Vibrance', 'Saturation']}
        used_name = snap.get(f'{{{NS["crss"]}}}Name', '')
        target = str(out_path) if output_dir else "IN-PLACE"
        return f"{path.name}: angewendet '{used_name}' → {target}. Preview: {preview}"

    def apply_to_folder_interactive(self, folder: Path, output_dir: Optional[Path]) -> None:
        if not folder.is_dir():
            raise SystemExit("Ordner existiert nicht.")

        files = sorted([p for p in folder.iterdir() if p.suffix.lower() == ".xmp"])
        if not files:
            raise SystemExit("Keine .xmp-Dateien gefunden.")

        options = self.collect_base_styles(files)
        chosen = self.choose_from_list(options)

        print(f"\nAusgewählt: {chosen}\nVerarbeite {len(files)} Dateien...\n")
        ok = 0
        for f in files:
            msg = self.apply_to_file(f, chosen, output_dir)
            print(msg)
            if "angewendet" in msg:
                ok += 1
        print(f"\nFertig. {ok}/{len(files)} Dateien verarbeitet.")


def _main() -> None:
    ap = argparse.ArgumentParser(description="Snapshots aus XMP wählen und deren crs:* nach oben kopieren.")
    ap.add_argument("folder", help="Ordner mit .xmp Dateien")
    ap.add_argument("-o", "--output", type=Path, help="Optionaler Ausgabeordner. "
                                                      "Standard: Originaldateien überschreiben.")
    args = ap.parse_args()

    app = XMPStyleApplier()
    app.apply_to_folder_interactive(Path(args.folder), output_dir=args.output)


if __name__ == "__main__":
    _main()
