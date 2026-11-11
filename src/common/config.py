"""Configuración externalizada del pipeline.

Todas las rutas y parámetros se leen de variables de entorno, con valores por
defecto pensados para correr desde la raíz del repo. Ver config/.env.example.
Ninguna credencial vive en el código.
"""

import os
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Settings:
    landing: str
    lake: str
    evidence: str
    ingest_date: str          # YYYY-MM-DD; fija la partición de la corrida

    @property
    def bronze(self) -> str:
        return f"{self.lake}/bronze"

    @property
    def quarantine(self) -> str:
        return f"{self.lake}/quarantine"


def load_settings(**overrides) -> Settings:
    """Lee la configuración del entorno; `overrides` tiene prioridad (tests)."""
    values = {
        "landing": os.environ.get("LANDING_PATH", "data/landing"),
        "lake": os.environ.get("LAKE_PATH", "data"),
        "evidence": os.environ.get("EVIDENCE_PATH", "evidence"),
        # Por defecto, la fecha de hoy. Se puede fijar para reprocesar un día
        # puntual o para demostrar idempotencia de forma determinística.
        "ingest_date": os.environ.get("INGEST_DATE", date.today().isoformat()),
    }
    values.update({k: v for k, v in overrides.items() if v is not None})
    date.fromisoformat(values["ingest_date"])  # falla temprano si viene mal
    return Settings(**values)
