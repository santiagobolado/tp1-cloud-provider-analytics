"""Controles de gobierno automáticos (consigna §6.2 "Gobierno preliminar").

1. Metadatos: toda columna de toda tabla del pipeline está en el catálogo.
2. Catálogo completo: cada dataset del catálogo existe en el pipeline y viceversa.
3. Acceso: el repositorio no contiene credenciales.
(La inmutabilidad de Landing se prueba en test_batch_to_bronze.)
"""

import os
import re
import subprocess

from src.governance.catalog import DATASETS, describe
from src.governance.schemas import expected_columns

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_toda_columna_esta_documentada(spark):
    missing = [f"{ds}.{c}" for ds, cols in expected_columns(spark).items()
               for c in cols if not describe(ds, c)]
    assert missing == [], f"Columnas sin descripción en catalog.py: {missing}"


def test_catalogo_y_pipeline_tienen_los_mismos_datasets(spark):
    assert set(expected_columns(spark)) == set(DATASETS)


def test_cada_dataset_tiene_responsable_linaje_y_retencion():
    for name, d in DATASETS.items():
        assert d["owner"].startswith("R"), name
        assert d["sources"] and d["produced_by"], name
        assert d["retention"], name


def test_linaje_apunta_a_datasets_existentes():
    for name, d in DATASETS.items():
        for s in d["sources"]:
            assert s.startswith("landing/") or s in DATASETS, f"{name} <- {s}"


SECRET_PATTERNS = [r"AstraCS:[A-Za-z0-9]{8,}", r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
                   r"(?i)aws_secret_access_key\s*=\s*\S{16,}"]


def test_el_repositorio_no_contiene_credenciales():
    files = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True,
                           check=True).stdout.split()
    assert not [f for f in files if re.search(r"secure-connect.*\.zip$|\.env$|\.pem$", f)]
    hits = []
    for f in files:
        if f.startswith("data/landing/") or f.endswith((".png", ".pdf", ".svg")):
            continue
        try:
            text = open(os.path.join(ROOT, f), encoding="utf-8", errors="ignore").read()
        except (IsADirectoryError, FileNotFoundError):
            continue
        hits += [f"{f}: {p}" for p in SECRET_PATTERNS if re.search(p, text)]
    assert hits == [], hits
