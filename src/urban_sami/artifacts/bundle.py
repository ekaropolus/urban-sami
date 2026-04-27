from __future__ import annotations

import hashlib
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(output_dir: str | Path, *, files: list[Path], metadata: dict) -> Path:
    out = Path(output_dir) / "artifact_manifest.json"
    payload = {
        "metadata": metadata,
        "files": [
            {
                "name": file.name,
                "path": str(file),
                "sha256": sha256_file(file),
            }
            for file in files
        ],
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out


def write_bundle(output_dir: str | Path, *, files: list[Path], bundle_name: str) -> Path:
    output_dir = Path(output_dir)
    bundle_path = output_dir / bundle_name
    with ZipFile(bundle_path, "w", compression=ZIP_DEFLATED) as zf:
        for file in files:
            zf.write(file, arcname=file.name)
    return bundle_path

