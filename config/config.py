"""
Configuración central del proyecto de infraestructura de datos
"""
import os
from pathlib import Path

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "usm-infra-grupo2")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME", "bucket_grupo2_infra")
LOCATION = os.getenv("GCP_LOCATION", "US")

CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

RAW_DATASET = "raw"
DWH_DATASET = "dwh"
DMS_DATASET = "dms"

DEFAULT_DAYS = 30
DEFAULT_DISTRIBUTORS = 5
DEFAULT_CLIENTS = 10
DEFAULT_BRANCHES = 25

PROJECT_ROOT = Path(__file__).parent.parent
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
DOCS_DIR = PROJECT_ROOT / "docs"
SRC_DIR = PROJECT_ROOT / "src"


def validate_config():
    """Valida que la configuración esté correctamente establecida"""
    if not CREDENTIALS_PATH:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS no está configurada")

    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(
            f"Archivo de credenciales no encontrado: {CREDENTIALS_PATH}")

    return True
